//! Backfill logic for historical log ingestion.
//! Processes multiple days concurrently with priority-based scheduling.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{Duration as ChronoDuration, NaiveTime, Utc};
use tokio::sync::mpsc;

use crate::buffer::BufferCapacities;
use crate::config::Config;
use crate::cw_tail::{CloudWatchTailer, TailConfig};
use crate::enrich::enrich_event;
use crate::es_recovery::ClusterStressTracker;
use crate::event_router::EventSenderFactory;
use crate::process::{GroupScheduler, Priority, Resources, WorkerPool};
use crate::state::CheckpointState;
use crate::types::LogEvent;

/// Maximum concurrent day backfills per log group.
pub const BACKFILL_CONCURRENCY: usize = 8;

/// Calculate the time range for a given day offset.
/// Returns (start_ms, end_ms) for the day.
pub fn day_range_ms(day_offset: u32) -> (i64, i64) {
    let day = Utc::now().date_naive() - ChronoDuration::days(day_offset as i64);

    let start = day
        .and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap_or_default())
        .and_utc()
        .timestamp_millis();

    let end = day
        .and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap_or_default())
        .and_utc()
        .timestamp_millis();

    (start, end)
}

/// Calculate throughput in events per second.
pub fn calculate_eps(event_count: usize, elapsed_ms: u128) -> u128 {
    if elapsed_ms > 0 {
        event_count as u128 * 1000 / elapsed_ms
    } else {
        0
    }
}

/// Validate backfill configuration.
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

// ============================================================================
// Backfill Execution
// ============================================================================

/// Statistics from a backfill run.
#[derive(Debug, Clone, Default)]
pub struct BackfillStats {
    /// Total events processed.
    pub total_events: usize,
    /// Total CPU time used.
    pub total_cpu_time: Duration,
    /// Number of processes completed.
    pub processes_completed: usize,
}

impl BackfillStats {
    /// Calculate events per second.
    pub fn events_per_second(&self) -> f64 {
        if self.total_cpu_time.as_secs_f64() > 0.0 {
            self.total_events as f64 / self.total_cpu_time.as_secs_f64()
        } else {
            0.0
        }
    }
}

/// OS-style process scheduler for backfill with demand-driven spawning.
///
/// Like Linux's process management:
/// - Only keeps max_ready processes in the ready queue (like RLIMIT_NPROC)
/// - Spawns new processes as workers complete work (like fork after wait)
/// - Prioritizes recent days over older days (like nice values)
/// - Pauses workers when cluster is under stress
pub async fn run_backfill_days(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sender_factory: EventSenderFactory,
    buffer_caps: BufferCapacities,
    stress_tracker: Option<Arc<ClusterStressTracker>>,
) -> Result<BackfillStats> {
    if cfg.backfill_days == 0 {
        return Ok(BackfillStats::default());
    }

    // Create OS-style process scheduler with group scheduler
    let resources = Resources {
        cw_api_quota: 100,
        es_bulk_capacity: buffer_caps.backfill_raw,
        memory_quota: buffer_caps.backfill_raw * 10,
    };
    let group_scheduler =
        GroupScheduler::new(cfg.log_group.clone(), resources, BACKFILL_CONCURRENCY);

    // Create demand-driven work queue (Linux-style RLIMIT_NPROC)
    // Only spawns max_ready processes at a time, spawning more as workers complete
    let work_queue = Arc::new(group_scheduler.create_backfill_queue(cfg.backfill_days));

    tracing::info!(
        "backfill scheduler: {} days for {} (concurrency={}, max_ready={})",
        cfg.backfill_days,
        cfg.log_group,
        BACKFILL_CONCURRENCY,
        BACKFILL_CONCURRENCY * 2
    );

    // Start the queue - spawns initial batch of processes (like init)
    let initial_pids = work_queue.start().await;
    tracing::info!(
        "backfill: spawned {} initial processes (demand-driven, {} pending)",
        initial_pids.len(),
        work_queue.pending_count().await
    );

    // Create worker pool with OS thread mapping
    let worker_pool = WorkerPool::new(group_scheduler.scheduler().clone(), BACKFILL_CONCURRENCY);
    tracing::info!(
        "backfill: worker pool with {} workers (optimal={})",
        worker_pool.num_workers(),
        WorkerPool::optimal_worker_count()
    );

    // Capture context for worker tasks
    let cfg_arc = Arc::new(cfg.clone());
    let aws_cfg_arc = Arc::new(aws_cfg.clone());
    let sender_factory_arc = Arc::new(sender_factory.clone());

    // Create priority-aware pause check function based on cluster stress
    // Lower priority work (old backfill days) pauses more aggressively
    let pause_check = {
        let tracker = stress_tracker.clone();
        move |priority: u8| -> Option<Duration> {
            let Some(tracker) = &tracker else {
                return None;
            };
            tracker.should_pause_for_priority(priority)
        }
    };

    // Spawn workers with demand-driven task factory and stress-aware pausing
    // Workers complete() on the queue, which spawns the next pending work
    let worker_handles = worker_pool.spawn_workers_with_pause(
        work_queue.clone(),
        {
            let cfg = cfg_arc.clone();
            let aws_cfg = aws_cfg_arc.clone();
            let sender_factory = sender_factory_arc.clone();
            move |_pid, info| {
                let cfg = cfg.clone();
                let aws_cfg = aws_cfg.clone();
                let sender_factory = sender_factory.clone();
                async move {
                    execute_backfill_day(
                        &cfg,
                        &aws_cfg,
                        &sender_factory,
                        buffer_caps,
                        info.day_offset,
                        info.priority,
                    )
                    .await
                }
            }
        },
        pause_check,
    );

    // Wait for all workers to complete and collect stats
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

    // Log final stats
    let counts = group_scheduler.process_counts().await;
    tracing::info!(
        "backfill complete: {} events in {:?} ({:.2} eps), {} processes terminated",
        stats.total_events,
        stats.total_cpu_time,
        stats.events_per_second(),
        counts.terminated
    );

    Ok(stats)
}

/// Execute backfill for a single day.
pub async fn execute_backfill_day(
    cfg: &Config,
    aws_cfg: &aws_config::SdkConfig,
    sender_factory: &EventSenderFactory,
    buffer_caps: BufferCapacities,
    day_offset: u32,
    priority: Priority,
) -> usize {
    let (start_ms, end_ms) = day_range_ms(day_offset);

    // Create tailer for this day
    let tailer = match create_tailer_for_day(cfg, aws_cfg).await {
        Ok(t) => t,
        Err(err) => {
            tracing::warn!(
                "backfill {} day {} tailer init failed: {err:?}",
                cfg.log_group,
                day_offset
            );
            return 0;
        }
    };

    let sink_tx = sender_factory.at(priority);
    let (raw_tx, mut raw_rx) = mpsc::channel::<LogEvent>(buffer_caps.backfill_raw);
    let log_group = cfg.log_group.clone();

    let consumer_handle = tokio::spawn(async move {
        let mut sent = 0usize;
        while let Some(raw) = raw_rx.recv().await {
            if let Some(enriched) = enrich_event(raw, None) {
                if sink_tx.send(enriched).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
        tracing::debug!(
            "backfill {} day {} drained {} events",
            log_group,
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
            cfg.log_group,
            day_offset
        );
    }

    sent
}

/// Create a tailer for backfill day processing.
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
