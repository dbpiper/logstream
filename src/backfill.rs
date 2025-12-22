use std::sync::Arc;
use std::time::Duration;

pub use crate::time_windows::utc_day_range_ms as day_range_ms;
use anyhow::Result;
use tokio::sync::mpsc;

use crate::buffer::BufferCapacities;
use crate::config::Config;
use crate::cw_tail::{CloudWatchTailer, TailConfig};
use crate::enrich::enrich_event;
use crate::event_router::EventSenderFactory;
use crate::naming;
use crate::process::{GroupScheduler, Priority, Resources, WorkerPool};
use crate::prune_state::PruneState;
use crate::state::CheckpointState;
use crate::stress::StressTracker;
use crate::types::LogEvent;

pub const BACKFILL_CONCURRENCY: usize = 2;

#[derive(Clone)]
pub struct BackfillDayContext {
    pub cfg: Arc<Config>,
    pub aws_cfg: Arc<aws_config::SdkConfig>,
    pub sender_factory: Arc<EventSenderFactory>,
    pub buffer_caps: BufferCapacities,
    pub prune_state: Arc<PruneState>,
    pub stable_alias: Arc<str>,
}

pub fn calculate_eps(event_count: usize, elapsed_ms: u128) -> u128 {
    if elapsed_ms > 0 {
        event_count as u128 * 1000 / elapsed_ms
    } else {
        0
    }
}

pub fn validate_backfill_config(
    backfill_days: u32,
    concurrency: usize,
) -> Result<(), &'static str> {
    if concurrency == 0 {
        return Err("concurrency must be at least 1");
    }
    if backfill_days > 36500 {
        return Err("backfill_days exceeds 100 years");
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct BackfillStats {
    pub total_events: usize,
    pub total_cpu_time: Duration,
    pub processes_completed: usize,
}

impl BackfillStats {
    pub fn events_per_second(&self) -> f64 {
        if self.total_cpu_time.as_secs_f64() > 0.0 {
            self.total_events as f64 / self.total_cpu_time.as_secs_f64()
        } else {
            0.0
        }
    }
}

pub async fn run_backfill_days(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sender_factory: EventSenderFactory,
    buffer_caps: BufferCapacities,
    es_stress_tracker: Option<Arc<StressTracker>>,
    cw_stress_tracker: Option<Arc<StressTracker>>,
    prune_state: Arc<PruneState>,
) -> Result<BackfillStats> {
    let cfg = Arc::new(cfg);
    if cfg.backfill_days == 0 {
        return Ok(BackfillStats::default());
    }

    let resources = Resources {
        cw_api_quota: 100,
        es_bulk_capacity: buffer_caps.backfill_raw,
        memory_quota: buffer_caps.backfill_raw * 10,
    };
    let group_scheduler =
        GroupScheduler::new(cfg.log_group.clone(), resources, BACKFILL_CONCURRENCY);

    let work_queue = Arc::new(group_scheduler.create_backfill_queue(cfg.backfill_days));

    tracing::info!(
        "backfill scheduler: {} days for {} (concurrency={}, max_ready={})",
        cfg.backfill_days,
        cfg.log_group,
        BACKFILL_CONCURRENCY,
        BACKFILL_CONCURRENCY * 2
    );

    let initial_pids = work_queue.start().await;
    tracing::info!(
        "backfill: spawned {} initial processes (demand-driven, {} pending)",
        initial_pids.len(),
        work_queue.pending_count().await
    );

    let worker_pool = WorkerPool::new(group_scheduler.scheduler().clone(), BACKFILL_CONCURRENCY);
    tracing::info!(
        "backfill: worker pool with {} workers (optimal={})",
        worker_pool.num_workers(),
        WorkerPool::optimal_worker_count()
    );

    let cfg_arc = cfg;
    let aws_cfg_arc = Arc::new(aws_cfg);
    let sender_factory_arc = Arc::new(sender_factory);

    let pause_check = {
        let es_tracker = es_stress_tracker.clone();
        let cw_tracker = cw_stress_tracker.clone();
        move |priority: u8| -> Option<Duration> {
            if let Some(ref es) = es_tracker {
                if let Some(duration) = es.should_pause_for_priority(priority) {
                    return Some(duration);
                }
            }
            if let Some(ref cw) = cw_tracker {
                if let Some(duration) = cw.should_pause_for_priority(priority) {
                    return Some(duration);
                }
            }
            None
        }
    };

    let worker_handles = worker_pool.spawn_workers_with_pause(
        work_queue.clone(),
        {
            let cfg = cfg_arc.clone();
            let aws_cfg = aws_cfg_arc.clone();
            let sender_factory = sender_factory_arc.clone();
            let prune_state = prune_state.clone();
            let stable_alias = naming::stable_alias(&cfg.index_prefix, cfg.log_group.as_ref());
            move |_pid, info| {
                let cfg = cfg.clone();
                let aws_cfg = aws_cfg.clone();
                let sender_factory = sender_factory.clone();
                let prune_state = prune_state.clone();
                let stable_alias = stable_alias.clone();
                async move {
                    execute_backfill_day(
                        BackfillDayContext {
                            cfg,
                            aws_cfg,
                            sender_factory,
                            buffer_caps,
                            prune_state,
                            stable_alias,
                        },
                        info.day_offset,
                        info.priority,
                    )
                    .await
                }
            }
        },
        pause_check,
    );

    let mut stats = BackfillStats::default();
    for handle in worker_handles {
        if let Ok(worker_stats) = handle.await {
            stats.total_events += worker_stats.total_events;
            stats.total_cpu_time += worker_stats.total_cpu_time;
            stats.processes_completed += worker_stats.processes_completed;
            tracing::debug!(
                "worker-{}: {} processes, {} events, {:.2} eps",
                worker_stats.worker_id,
                worker_stats.processes_completed,
                worker_stats.total_events,
                worker_stats.events_per_second()
            );
        }
    }

    let counts = group_scheduler.process_counts();
    tracing::info!(
        "backfill complete: {} events in {:?} ({:.2} eps), {} processes terminated",
        stats.total_events,
        stats.total_cpu_time,
        stats.events_per_second(),
        counts.terminated
    );

    Ok(stats)
}

pub async fn execute_backfill_day(
    ctx: BackfillDayContext,
    day_offset: u32,
    priority: Priority,
) -> usize {
    let (start_ms, end_ms) = day_range_ms(day_offset);

    let Some((start_ms, end_ms)) = ctx
        .prune_state
        .apply_window(&ctx.stable_alias, start_ms, end_ms)
        .await
    else {
        tracing::info!(
            "backfill day offset {} skipped (pruned) for {}",
            day_offset,
            ctx.stable_alias
        );
        return 0;
    };

    let tailer = match create_tailer_for_day(&ctx.cfg, &ctx.aws_cfg).await {
        Ok(t) => t,
        Err(err) => {
            tracing::warn!(
                "backfill {} day {} tailer init failed: {err:?}",
                ctx.cfg.log_group,
                day_offset
            );
            return 0;
        }
    };

    let sink_tx = ctx.sender_factory.at(priority);
    let (raw_tx, mut raw_rx) = mpsc::channel::<LogEvent>(ctx.buffer_caps.backfill_raw);
    let log_group = ctx.cfg.log_group.clone();
    let log_group_for_consumer = log_group.clone();

    let consumer_handle = tokio::spawn(async move {
        let mut sent = 0usize;
        while let Some(raw) = raw_rx.recv().await {
            if let Some(enriched) = enrich_event(raw, log_group_for_consumer.as_ref()) {
                if sink_tx.send(enriched).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
        tracing::debug!(
            "backfill {} day {} drained {} events",
            log_group_for_consumer,
            day_offset,
            sent
        );
        sent
    });

    let fetch_result = tailer.fetch_range(start_ms, end_ms, &raw_tx).await;
    drop(raw_tx);

    let sent = consumer_handle.await.unwrap_or(0);

    if let Err(ref err) = fetch_result {
        tracing::warn!(
            "backfill {} day {} fetch error: {err:?}",
            log_group,
            day_offset
        );
    }

    sent
}

async fn create_tailer_for_day(
    cfg: &Config,
    aws_cfg: &aws_config::SdkConfig,
) -> Result<CloudWatchTailer> {
    let cw_client = aws_sdk_cloudwatchlogs::Client::new(aws_cfg);
    let checkpoint = CheckpointState::load(&cfg.checkpoint_path)?;
    Ok(CloudWatchTailer::new(
        TailConfig {
            log_group: cfg.log_group.clone(),
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
            backoff_base: Duration::from_millis(cfg.backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.backoff_max_ms),
        },
        cw_client,
        checkpoint,
        cfg.checkpoint_path.clone(),
    ))
}
