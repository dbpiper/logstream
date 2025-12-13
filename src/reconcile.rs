use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{debug, info, warn};

use std::sync::Arc;

use crate::{
    cw_counts::CwCounter,
    cw_tail::CloudWatchTailer,
    enrich::enrich_event,
    es_counts::EsCounter,
    event_router::EventSender,
    seasonal_stats::{validate_event_integrity, Confidence, DataIntegrity, SeasonalStats},
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
    let feasibility = check_feasibility_async(ctx, start_ms, end_ms, cw_count).await;

    match feasibility {
        FeasibilityAction::FullReplace => {
            execute_full_replace(ctx, start_ms, end_ms, events, cw_count).await;
        }
        FeasibilityAction::CheckIntegrity => {
            execute_integrity_aware_ingest(ctx, start_ms, end_ms, events).await;
        }
        FeasibilityAction::LiveLearn => {
            execute_live_learn_upsert(ctx, start_ms, end_ms, events).await;
        }
    }
}

const LIVE_LEARN_THRESHOLD_PCT: u64 = 10;

enum FeasibilityAction {
    FullReplace,
    CheckIntegrity,
    LiveLearn,
}

async fn check_feasibility_async(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    cw_count: u64,
) -> FeasibilityAction {
    let range_ms = end_ms - start_ms;
    let confidence = ctx.seasonal_stats.confidence();

    if ctx.seasonal_stats.needs_es_fallback() {
        return check_with_es_blend(ctx, start_ms, end_ms, cw_count, range_ms, confidence).await;
    }

    let result = ctx
        .seasonal_stats
        .is_feasible(start_ms, range_ms, cw_count, &ctx.cw_stress);

    match &result {
        crate::seasonal_stats::FeasibilityResult::NoHistory => {
            live_learn_check(ctx, start_ms, end_ms, cw_count).await
        }
        crate::seasonal_stats::FeasibilityResult::Feasible {
            expected,
            stddev,
            sigma_used,
        } => {
            info!(
                "reconcile feasible: cw={} expected={:.0}±{:.0} ({}σ) conf={:?} for {}-{}",
                cw_count, expected, stddev, sigma_used, confidence, start_ms, end_ms
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
                "reconcile suspicious: cw={} expected={:.0}±{:.0} dev={:.0} ({}σ) conf={:?} for {}-{}",
                cw_count, expected, stddev, deviation, sigma_used, confidence, start_ms, end_ms
            );
            FeasibilityAction::CheckIntegrity
        }
    }
}

async fn check_with_es_blend(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    cw_count: u64,
    range_ms: i64,
    confidence: Confidence,
) -> FeasibilityAction {
    let es_count = match ctx.es_counter.count_range(start_ms, end_ms).await {
        Ok(c) => c,
        Err(_) => {
            return live_learn_check(ctx, start_ms, end_ms, cw_count).await;
        }
    };

    ctx.seasonal_stats.record_from_es(start_ms, es_count);

    let range_hours = (range_ms as f64) / 3_600_000.0;
    let mid_timestamp = start_ms + range_ms / 2;

    let (expected, stddev) = match ctx.seasonal_stats.blend_with_es(mid_timestamp, es_count) {
        Some((m, s)) => (m * range_hours, s * range_hours.sqrt()),
        None => {
            let es_f = es_count as f64;
            (es_f, (es_f.sqrt()).max(10.0))
        }
    };

    let stress_level = ctx.cw_stress.stress_level();
    let sigma_multiplier = match stress_level {
        crate::stress::StressLevel::Normal => 4.0,
        crate::stress::StressLevel::Elevated => 2.5,
        crate::stress::StressLevel::Critical => 1.5,
    };

    let cw_f = cw_count as f64;
    let deviation = (cw_f - expected).abs();
    let threshold = (stddev * sigma_multiplier).max(expected * 0.1).max(10.0);

    if deviation <= threshold {
        info!(
            "reconcile es_blend feasible: cw={} es={} expected={:.0}±{:.0} conf={:?} for {}-{}",
            cw_count, es_count, expected, stddev, confidence, start_ms, end_ms
        );
        ctx.seasonal_stats.record_verified(start_ms, cw_count);
        FeasibilityAction::FullReplace
    } else {
        let max_count = es_count.max(cw_count);
        let diff_pct = if max_count > 0 {
            es_count.abs_diff(cw_count) * 100 / max_count
        } else {
            0
        };

        if diff_pct <= LIVE_LEARN_THRESHOLD_PCT {
            info!(
                "reconcile es_blend close: cw={} es={} diff={}% conf={:?}, learning for {}-{}",
                cw_count, es_count, diff_pct, confidence, start_ms, end_ms
            );
            ctx.seasonal_stats.record_verified(start_ms, es_count);
            FeasibilityAction::LiveLearn
        } else if es_count > cw_count && ctx.cw_stress.is_stressed() {
            info!(
                "reconcile es_blend: es={} > cw={} under stress, conf={:?}, learning for {}-{}",
                es_count, cw_count, confidence, start_ms, end_ms
            );
            ctx.seasonal_stats.record_verified(start_ms, es_count);
            FeasibilityAction::LiveLearn
        } else {
            warn!(
                "reconcile es_blend suspicious: cw={} es={} expected={:.0} dev={:.0} conf={:?} for {}-{}",
                cw_count, es_count, expected, deviation, confidence, start_ms, end_ms
            );
            FeasibilityAction::CheckIntegrity
        }
    }
}

async fn live_learn_check(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    cw_count: u64,
) -> FeasibilityAction {
    let es_count = match ctx.es_counter.count_range(start_ms, end_ms).await {
        Ok(c) => c,
        Err(_) => return FeasibilityAction::CheckIntegrity,
    };

    let max_count = es_count.max(cw_count);
    let diff = es_count.abs_diff(cw_count);

    if max_count == 0 {
        ctx.seasonal_stats.record_verified(start_ms, 0);
        return FeasibilityAction::LiveLearn;
    }

    let diff_pct = diff * 100 / max_count;

    if diff_pct <= LIVE_LEARN_THRESHOLD_PCT {
        info!(
            "reconcile live_learn: es={} cw={} diff={}% (≤{}%), trusting ES for {}-{}",
            es_count, cw_count, diff_pct, LIVE_LEARN_THRESHOLD_PCT, start_ms, end_ms
        );
        ctx.seasonal_stats.record_verified(start_ms, es_count);
        FeasibilityAction::LiveLearn
    } else if es_count > cw_count && ctx.cw_stress.is_stressed() {
        info!(
            "reconcile live_learn: es={} > cw={} under stress, trusting ES for {}-{}",
            es_count, cw_count, start_ms, end_ms
        );
        ctx.seasonal_stats.record_verified(start_ms, es_count);
        FeasibilityAction::LiveLearn
    } else {
        info!(
            "reconcile live_learn: es={} vs cw={} diff={}% (>{}%), replacing for {}-{}",
            es_count, cw_count, diff_pct, LIVE_LEARN_THRESHOLD_PCT, start_ms, end_ms
        );
        if cw_count > 0 || !ctx.cw_stress.is_stressed() {
            ctx.seasonal_stats.record_verified(start_ms, cw_count);
        }
        FeasibilityAction::FullReplace
    }
}

async fn execute_full_replace(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    events: Vec<LogEvent>,
    cw_count: u64,
) {
    // CRITICAL: Collect IDs we're about to ingest BEFORE sending
    // We do NOT delete first - that causes data loss if ingestion fails
    let cw_ids: std::collections::HashSet<String> = events.iter().map(|e| e.id.clone()).collect();
    let cw_id_count = cw_ids.len();

    debug!(
        "reconcile safe_replace: starting for {}-{} with {} CW events ({} unique IDs)",
        start_ms,
        end_ms,
        events.len(),
        cw_id_count
    );

    // Upsert all events first (safe - won't lose data)
    let mut ingested = 0usize;
    let mut enrich_failures = 0usize;
    for raw in events {
        if let Some(enriched) = enrich_event(raw, None) {
            if ctx.sink_tx.send(enriched).await.is_err() {
                warn!(
                    "reconcile safe_replace: sink closed after {} ingested for {}-{}",
                    ingested, start_ms, end_ms
                );
                break;
            }
            ingested += 1;
        } else {
            enrich_failures += 1;
        }
    }

    if enrich_failures > 0 {
        warn!(
            "reconcile safe_replace: {} events failed enrichment for {}-{}",
            enrich_failures, start_ms, end_ms
        );
    }

    // Only delete orphaned events (in ES but not in CW) after successful upserts
    if ingested > 0 {
        match ctx.es_counter.get_ids_in_range(start_ms, end_ms).await {
            Ok(es_ids) => {
                let es_id_count = es_ids.len();
                let es_id_set: std::collections::HashSet<String> = es_ids.into_iter().collect();

                // Find orphans (in ES but not in CW)
                let orphan_ids: Vec<String> = es_id_set
                    .iter()
                    .filter(|id| !cw_ids.contains(*id))
                    .cloned()
                    .collect();

                // Find missing (in CW but not in ES) - for debugging
                let missing_from_es: Vec<&String> = cw_ids
                    .iter()
                    .filter(|id| !es_id_set.contains(*id))
                    .collect();

                if !missing_from_es.is_empty() {
                    info!(
                        "reconcile safe_replace: {} events in CW but not in ES for {}-{} (will be added)",
                        missing_from_es.len(),
                        start_ms,
                        end_ms
                    );
                    if missing_from_es.len() <= 20 {
                        for id in &missing_from_es {
                            debug!("reconcile safe_replace: missing event ID: {}", id);
                        }
                    } else {
                        debug!(
                            "reconcile safe_replace: first 20 missing IDs: {:?}",
                            &missing_from_es[..20]
                        );
                    }
                }

                debug!(
                    "reconcile safe_replace: ES has {} IDs, CW has {}, orphans={}, missing={}",
                    es_id_count,
                    cw_id_count,
                    orphan_ids.len(),
                    missing_from_es.len()
                );

                if !orphan_ids.is_empty() {
                    info!(
                        "reconcile safe_replace: deleting {} orphan events (in ES but not CW) for {}-{}",
                        orphan_ids.len(),
                        start_ms,
                        end_ms
                    );
                    if orphan_ids.len() <= 20 {
                        for id in &orphan_ids {
                            debug!("reconcile safe_replace: orphan event ID: {}", id);
                        }
                    }
                    if let Err(err) = ctx.es_counter.delete_ids(&orphan_ids).await {
                        warn!(
                            "reconcile safe_replace: delete orphans failed: {err:?} ({} orphans) for {}-{}",
                            orphan_ids.len(),
                            start_ms,
                            end_ms
                        );
                    }
                }
            }
            Err(err) => {
                warn!(
                    "reconcile safe_replace: failed to get ES IDs for orphan check: {err:?} for {}-{}",
                    start_ms, end_ms
                );
            }
        }
    } else {
        warn!(
            "reconcile safe_replace: zero events ingested (of {} CW events) for {}-{}, skipping orphan check",
            cw_count, start_ms, end_ms
        );
    }

    info!(
        "reconcile safe_replace: complete - ingested={} cw_count={} for {}-{}",
        ingested, cw_count, start_ms, end_ms
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
    let total = events.len();
    let mut ingested = 0usize;
    let mut enrich_failures = 0usize;

    for raw in events {
        if let Some(enriched) = enrich_event(raw, None) {
            if ctx.sink_tx.send(enriched).await.is_err() {
                warn!(
                    "reconcile upsert: sink closed after {}/{} events",
                    ingested, total
                );
                break;
            }
            ingested += 1;
        } else {
            enrich_failures += 1;
        }
    }

    if enrich_failures > 0 {
        warn!(
            "reconcile upsert: {} of {} events failed enrichment",
            enrich_failures, total
        );
    }

    debug!(
        "reconcile upsert: completed {}/{} events (enrich_failures={})",
        ingested, total, enrich_failures
    );
}

async fn execute_live_learn_upsert(
    ctx: &ReconcileContext,
    start_ms: i64,
    end_ms: i64,
    events: Vec<LogEvent>,
) {
    if events.is_empty() {
        return;
    }

    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp_ms).collect();
    let integrity = validate_event_integrity(&timestamps, start_ms, end_ms);

    if !integrity.is_usable() {
        info!(
            "reconcile live_learn: skipping {} events with poor integrity for {}-{}",
            events.len(),
            start_ms,
            end_ms
        );
        return;
    }

    info!(
        "reconcile live_learn: upserting {} events (no delete) for {}-{}",
        events.len(),
        start_ms,
        end_ms
    );

    upsert_events(ctx, events).await;
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
