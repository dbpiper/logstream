use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{info, warn};

use std::sync::Arc;

use crate::{
    cw_counts::CwCounter,
    cw_tail::CloudWatchTailer,
    enrich::enrich_event,
    es_counts::EsCounter,
    event_router::EventSender,
    seasonal_stats::{validate_event_integrity, DataIntegrity, SeasonalStats},
    stress::StressTracker,
    types::LogEvent,
};

const FULL_RESYNC_THRESHOLD_PCT: u64 = 5;
const ES_STARTUP_WAIT_SECS: u64 = 30;
const ES_STARTUP_MAX_ATTEMPTS: u32 = 20;
const MAX_PARALLEL_DAYS: usize = 2;

#[derive(Clone)]
pub struct ReconcileContext {
    pub tailer: CloudWatchTailer,
    pub sink_tx: EventSender,
    pub es_counter: EsCounter,
    pub cw_counter: CwCounter,
    pub cw_stress: Arc<StressTracker>,
    pub seasonal_stats: Arc<SeasonalStats>,
}

#[derive(Clone, Copy)]
pub struct ReconcileParams {
    pub period: Duration,
    pub replay_window: Duration,
    pub buffer_cap: usize,
    pub resync_scale: usize,
}

#[derive(Clone, Copy)]
struct SyncParams {
    min_range_ms: i64,
    sample: usize,
    mid_span_ms: i64,
    buffer_cap: usize,
    resync_scale: usize,
}

async fn wait_for_es_ready(es_counter: &EsCounter) {
    let now_ms = current_time_ms();
    let start_ms = now_ms - 60_000;
    for attempt in 1..=ES_STARTUP_MAX_ATTEMPTS {
        match es_counter.count_range(start_ms, now_ms).await {
            Ok(_) => {
                info!("reconcile: ES ready after {} attempts", attempt);
                return;
            }
            Err(err) => {
                warn!(
                    "reconcile: ES not ready (attempt {}/{}): {err:?}",
                    attempt, ES_STARTUP_MAX_ATTEMPTS
                );
                sleep(Duration::from_secs(ES_STARTUP_WAIT_SECS)).await;
            }
        }
    }
    warn!("reconcile: ES not ready after max attempts, proceeding anyway");
}

fn derive_sync_params(range_ms: i64, params: ReconcileParams) -> SyncParams {
    let leaf = (range_ms / 16).max(1);
    let leaf_count = (range_ms / leaf).max(1);
    let sample = ((leaf_count / 4).max(16)) as usize;
    let mid_span = (range_ms / 8).max(1);
    SyncParams {
        min_range_ms: leaf,
        sample,
        mid_span_ms: mid_span,
        buffer_cap: params.buffer_cap,
        resync_scale: params.resync_scale,
    }
}

pub async fn run_reconcile_loop(ctx: ReconcileContext, params: ReconcileParams) {
    wait_for_es_ready(&ctx.es_counter).await;
    let mut ticker = interval(params.period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now_ms = current_time_ms();
        let start_ms = now_ms.saturating_sub(params.replay_window.as_millis() as i64);
        let range_ms = now_ms.saturating_sub(start_ms);
        let sync = derive_sync_params(range_ms, params);
        info!(
            "reconcile: replaying window start={} end={}",
            start_ms, now_ms
        );

        if let Err(err) = almost_sure_sync(&ctx, start_ms, now_ms, sync).await {
            warn!("reconcile almost_sure_sync error: {err:?}");
        }
    }
}

pub async fn run_full_history(ctx: ReconcileContext, params: ReconcileParams, backfill_days: u32) {
    wait_for_es_ready(&ctx.es_counter).await;
    let mut ticker = interval(params.period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        info!(
            "full_history reconcile tick backfill_days={} period_secs={}",
            backfill_days,
            params.period.as_secs()
        );
        if let Err(err) = reconcile_all_days(&ctx, params, backfill_days).await {
            warn!("full history reconcile error: {err:?}");
        }
    }
}

async fn reconcile_all_days(
    ctx: &ReconcileContext,
    params: ReconcileParams,
    backfill_days: u32,
) -> Result<(), anyhow::Error> {
    let priority_days = 7u32.min(backfill_days);
    for offset in 0..priority_days {
        if let Err(err) = process_day(ctx, params, offset).await {
            warn!("priority day offset {} error: {err:?}", offset);
        }
    }

    let semaphore = std::sync::Arc::new(Semaphore::new(MAX_PARALLEL_DAYS));
    let mut futs: FuturesUnordered<tokio::task::JoinHandle<()>> = FuturesUnordered::new();

    let mut offset: u32 = priority_days;
    while backfill_days == 0 || offset < backfill_days {
        let permit = semaphore.clone().acquire_owned().await?;
        let day_offset = offset;
        let ctx_clone = ctx.clone();
        futs.push(tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = process_day(&ctx_clone, params, day_offset).await {
                warn!("day offset {} error: {err:?}", day_offset);
            }
        }));
        offset += 1;
        while futs.len() >= MAX_PARALLEL_DAYS {
            let _ = futs.next().await;
        }
    }
    while let Some(_res) = futs.next().await {}
    Ok(())
}

async fn almost_sure_sync(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    sync: SyncParams,
) -> Result<(), anyhow::Error> {
    let mut stack = vec![(start_ms, end_ms)];

    while let Some((s, e)) = stack.pop() {
        let es_count = ctx.es_counter.count_range(s, e).await?;
        let cw_count = ctx.cw_counter.count_range(s, e).await?;

        if es_count == 0 && cw_count == 0 {
            continue;
        }

        let range = e - s;
        let mid = s + range / 2;
        let diff = es_count.abs_diff(cw_count);
        let max_count = es_count.max(cw_count);

        if max_count > 0 && (diff * 100 / max_count) >= FULL_RESYNC_THRESHOLD_PCT {
            safe_replace_range(ctx, s, e, sync.buffer_cap).await;
            continue;
        }

        if es_count == cw_count {
            let (cw_first, cw_last) = ctx.tailer.sample_ids(s, e, sync.sample).await?;
            let (es_first, es_last) = ctx.es_counter.sample_ids(s, e, sync.sample).await?;
            let mut boundary_match = cw_first == es_first && cw_last == es_last;

            if boundary_match {
                let half = (sync.mid_span_ms / 2).max(1);
                let mid_start = (mid - half).max(s);
                let mid_end = (mid + half).min(e);
                let cw_mid = ctx
                    .tailer
                    .sample_ids_window(mid_start, mid_end, sync.sample)
                    .await?;
                if !cw_mid.is_empty() {
                    let found = ctx
                        .es_counter
                        .count_ids_in_range(mid_start, mid_end, &cw_mid)
                        .await?;
                    boundary_match = boundary_match && found as usize == cw_mid.len();
                }
            }

            if boundary_match {
                continue;
            }
        }

        if range > sync.min_range_ms {
            stack.push((mid, e));
            stack.push((s, mid));
            continue;
        }

        let buf = sync.sample.saturating_mul(sync.resync_scale).max(1);
        safe_replace_range(ctx, s, e, buf).await;
    }

    Ok(())
}

async fn safe_replace_range(ctx: &ReconcileContext, start_ms: i64, end_ms: i64, buffer_cap: usize) {
    info!("reconcile safe_replace start={} end={}", start_ms, end_ms);

    let (raw_tx, mut raw_rx) = tokio::sync::mpsc::channel::<LogEvent>(buffer_cap);

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(raw) = raw_rx.recv().await {
            events.push(raw);
        }
        events
    });

    let fetch_result = ctx.tailer.fetch_range(start_ms, end_ms, &raw_tx).await;
    drop(raw_tx);

    let events = match collector.await {
        Ok(events) => events,
        Err(err) => {
            warn!(
                "reconcile collector failed: {err:?} for {}-{}",
                start_ms, end_ms
            );
            return;
        }
    };

    if let Err(err) = fetch_result {
        warn!(
            "reconcile CW fetch failed: {err:?} for {}-{}",
            start_ms, end_ms
        );
        return;
    }

    let cw_count = events.len() as u64;
    let feasibility = check_feasibility(ctx, start_ms, end_ms, cw_count);

    match feasibility {
        FeasibilityAction::FullReplace => {
            execute_full_replace(ctx, start_ms, end_ms, events, cw_count).await;
        }
        FeasibilityAction::CheckIntegrity => {
            execute_integrity_aware_ingest(ctx, start_ms, end_ms, events).await;
        }
    }
}

enum FeasibilityAction {
    FullReplace,
    CheckIntegrity,
}

fn check_feasibility(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    cw_count: u64,
) -> FeasibilityAction {
    let range_ms = end_ms - start_ms;
    let result = ctx
        .seasonal_stats
        .is_feasible(start_ms, range_ms, cw_count, &ctx.cw_stress);

    match &result {
        crate::seasonal_stats::FeasibilityResult::NoHistory => {
            if cw_count > 0 || !ctx.cw_stress.is_stressed() {
                ctx.seasonal_stats.record_verified(start_ms, cw_count);
            }
            FeasibilityAction::FullReplace
        }
        crate::seasonal_stats::FeasibilityResult::Feasible {
            expected,
            stddev,
            sigma_used,
        } => {
            info!(
                "reconcile feasible: cw={} expected={:.0}±{:.0} ({}σ) for {}-{}",
                cw_count, expected, stddev, sigma_used, start_ms, end_ms
            );
            ctx.seasonal_stats.record_verified(start_ms, cw_count);
            FeasibilityAction::FullReplace
        }
        crate::seasonal_stats::FeasibilityResult::Suspicious {
            expected,
            stddev,
            deviation,
            sigma_used,
        } => {
            warn!(
                "reconcile suspicious: cw={} expected={:.0}±{:.0} dev={:.0} ({}σ) for {}-{}",
                cw_count, expected, stddev, deviation, sigma_used, start_ms, end_ms
            );
            FeasibilityAction::CheckIntegrity
        }
    }
}

async fn execute_full_replace(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    events: Vec<LogEvent>,
    cw_count: u64,
) {
    if let Err(err) = ctx.es_counter.delete_range(start_ms, end_ms).await {
        warn!(
            "reconcile ES delete failed: {err:?} for {}-{}",
            start_ms, end_ms
        );
    }

    for raw in events {
        if let Some(enriched) = enrich_event(raw, None) {
            if ctx.sink_tx.send(enriched).await.is_err() {
                warn!("reconcile sink closed for {}-{}", start_ms, end_ms);
                break;
            }
        }
    }

    info!(
        "reconcile complete (full replace): {} events for {}-{}",
        cw_count, start_ms, end_ms
    );
}

async fn execute_integrity_aware_ingest(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    events: Vec<LogEvent>,
) {
    if events.is_empty() {
        info!(
            "reconcile: suspicious empty response, preserving ES data for {}-{}",
            start_ms, end_ms
        );
        return;
    }

    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let integrity = validate_event_integrity(&timestamps, start_ms, end_ms);

    match integrity {
        DataIntegrity::Valid => {
            info!(
                "reconcile: data integrity valid despite suspicious count, upserting {} events for {}-{}",
                events.len(), start_ms, end_ms
            );
            upsert_events(ctx, events).await;
        }
        DataIntegrity::Partial { coverage } => {
            info!(
                "reconcile: partial data integrity (coverage={:.2}), upserting {} events for {}-{}",
                coverage,
                events.len(),
                start_ms,
                end_ms
            );
            upsert_events(ctx, events).await;
        }
        DataIntegrity::Invalid => {
            warn!(
                "reconcile: data integrity check failed, preserving ES data for {}-{}",
                start_ms, end_ms
            );
        }
    }
}

async fn upsert_events(ctx: &ReconcileContext, events: Vec<LogEvent>) {
    for raw in events {
        if let Some(enriched) = enrich_event(raw, None) {
            if ctx.sink_tx.send(enriched).await.is_err() {
                warn!("reconcile upsert: sink closed");
                break;
            }
        }
    }
}

async fn process_day(
    ctx: &ReconcileContext,
    params: ReconcileParams,
    offset: u32,
) -> Result<(), anyhow::Error> {
    let started = Instant::now();
    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(offset as i64);
    let start = day
        .and_hms_opt(0, 0, 0)
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());
    let end = day
        .and_hms_opt(23, 59, 59)
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());
    let start_ms = start.and_utc().timestamp_millis();
    let end_ms = end.and_utc().timestamp_millis();
    let range_ms = end_ms.saturating_sub(start_ms);
    let sync = derive_sync_params(range_ms, params);

    almost_sure_sync(ctx, start_ms, end_ms, sync).await?;
    info!(
        "day offset {} heal complete elapsed_ms={}",
        offset,
        started.elapsed().as_millis()
    );
    Ok(())
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
