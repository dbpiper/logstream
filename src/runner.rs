//! Group runner - orchestrates all processing for a log group.
//! Uses OS-style process scheduler to manage daemons and batch tasks.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::buffer::BufferCapacities;
use crate::config::Config;
use crate::cw_counts::CwCounter;
use crate::cw_tail::{CloudWatchTailer, TailConfig};
use crate::enrich::enrich_event;
use crate::es_conflicts::EsConflictResolver;
use crate::es_counts::EsCounter;
use crate::event_router::{EventSender, EventSenderFactory};
use crate::process::{GroupScheduler, Priority, ProcessScheduler, Resources, WorkerPool};
use crate::reconcile;
use crate::state::CheckpointState;
use crate::types::LogEvent;

// ============================================================================
// Types
// ============================================================================

/// Context for running a log group.
pub struct GroupContext {
    pub cfg: Config,
    pub aws_cfg: aws_config::SdkConfig,
    pub sender_factory: EventSenderFactory,
    pub buffer_caps: BufferCapacities,
}

/// Configuration extracted from environment for a group.
pub struct GroupEnvConfig {
    pub es_url: String,
    pub es_user: String,
    pub es_pass: String,
    pub disable_conflict_reindex: bool,
    pub disable_reconcile: bool,
    pub heal_days: u32,
}

impl GroupEnvConfig {
    /// Load configuration from environment.
    pub fn from_env(backfill_days: u32) -> Self {
        let disable_conflict_reindex = std::env::var("DISABLE_CONFLICT_REINDEX")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let disable_reconcile = std::env::var("DISABLE_RECONCILE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let heal_days = if backfill_days > 0 {
            0
        } else {
            std::env::var("RECENT_HEAL_DAYS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(30)
        };

        Self {
            es_url: std::env::var("ES_HOST").unwrap_or_else(|_| "http://localhost:9200".into()),
            es_user: std::env::var("ES_USER").unwrap_or_else(|_| "elastic".into()),
            es_pass: std::env::var("ES_PASS").unwrap_or_else(|_| "changeme".into()),
            disable_conflict_reindex,
            disable_reconcile,
            heal_days,
        }
    }
}

/// Handle collection for cleanup on shutdown.
pub struct ProcessHandles {
    pub tail: Option<tokio::task::JoinHandle<()>>,
    pub reconcile: Option<tokio::task::JoinHandle<()>>,
    pub reconcile_full: Option<tokio::task::JoinHandle<()>>,
    pub conflict: Option<tokio::task::JoinHandle<()>>,
    pub heal: Option<tokio::task::JoinHandle<()>>,
    pub backfill: Option<tokio::task::JoinHandle<()>>,
}

impl ProcessHandles {
    /// Create empty handles.
    pub fn empty() -> Self {
        Self {
            tail: None,
            reconcile: None,
            reconcile_full: None,
            conflict: None,
            heal: None,
            backfill: None,
        }
    }

    /// Abort all handles.
    pub fn abort_all(&self) {
        if let Some(h) = &self.tail {
            h.abort();
        }
        if let Some(h) = &self.reconcile {
            h.abort();
        }
        if let Some(h) = &self.reconcile_full {
            h.abort();
        }
        if let Some(h) = &self.conflict {
            h.abort();
        }
        if let Some(h) = &self.heal {
            h.abort();
        }
        if let Some(h) = &self.backfill {
            h.abort();
        }
    }
}

// ============================================================================
// Tailer Creation
// ============================================================================

/// Create a CloudWatchTailer clone for reconcile operations.
pub async fn create_tailer_for_reconcile(
    cfg: &Config,
    checkpoint_path: PathBuf,
    aws_cfg: &aws_config::SdkConfig,
) -> Result<CloudWatchTailer> {
    let cw_client = aws_sdk_cloudwatchlogs::Client::new(aws_cfg);
    let checkpoint = CheckpointState::load(&checkpoint_path)?;
    Ok(CloudWatchTailer::new(
        TailConfig {
            log_group: cfg.log_group.clone(),
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
            backoff_base: Duration::from_millis(cfg.backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.backoff_max_ms),
        },
        cw_client,
        checkpoint,
        checkpoint_path,
    ))
}

// ============================================================================
// Process Spawning
// ============================================================================

/// Spawn daemon process IDs for a group.
pub struct SpawnedDaemons {
    pub tail_pid: u64,
    pub reconcile_pid: Option<u64>,
    pub full_history_pid: Option<u64>,
    pub conflict_pid: Option<u64>,
}

/// Spawn all daemon processes.
pub async fn spawn_daemon_processes(
    group_scheduler: &GroupScheduler,
    env_cfg: &GroupEnvConfig,
    backfill_days: u32,
) -> SpawnedDaemons {
    let tail_pid = group_scheduler.spawn_realtime_tail().await;
    tracing::debug!("spawned tail process pid={}", tail_pid);

    let reconcile_pid = if !env_cfg.disable_reconcile {
        Some(group_scheduler.spawn_reconcile().await)
    } else {
        None
    };

    let full_history_pid = if !env_cfg.disable_reconcile && backfill_days == 0 {
        Some(group_scheduler.spawn_full_history_reconcile().await)
    } else {
        None
    };

    let conflict_pid = if !env_cfg.disable_conflict_reindex {
        Some(group_scheduler.spawn_conflict_reindex().await)
    } else {
        None
    };

    SpawnedDaemons {
        tail_pid,
        reconcile_pid,
        full_history_pid,
        conflict_pid,
    }
}

// ============================================================================
// Daemon Execution
// ============================================================================

/// Execute the conflict reindex daemon.
pub fn execute_conflict_daemon(
    pid: u64,
    es_conflicts: EsConflictResolver,
    scheduler: Arc<ProcessScheduler>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = es_conflicts.run_conflict_reindex().await {
            tracing::warn!("conflict reindex failed: {err:?}");
        }
        scheduler.terminate(pid, 0, Duration::ZERO).await;
    })
}

/// Execute the real-time tail daemon.
pub fn execute_tail_daemon(
    pid: u64,
    mut tailer: CloudWatchTailer,
    sink: EventSender,
    buffer_cap: usize,
    scheduler: Arc<ProcessScheduler>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (raw_tx, mut raw_rx) = mpsc::channel::<LogEvent>(buffer_cap);

        tokio::spawn(async move {
            let mut events = 0usize;
            while let Some(raw) = raw_rx.recv().await {
                if let Some(enriched) = enrich_event(raw, None) {
                    if sink.send(enriched).await.is_err() {
                        break;
                    }
                    events += 1;
                }
            }
            events
        });

        if let Err(err) = tailer.run(raw_tx).await {
            tracing::error!("tailer failed: {err:?}");
        }
        scheduler.terminate(pid, 0, Duration::ZERO).await;
    })
}

/// Context for executing reconcile daemons.
pub struct ReconcileExecContext<'a> {
    pub cfg: &'a Config,
    pub aws_cfg: &'a aws_config::SdkConfig,
    pub sender_factory: &'a EventSenderFactory,
    pub es_counter: &'a EsCounter,
    pub cw_counter: &'a CwCounter,
    pub buffer_caps: &'a BufferCapacities,
}

/// Execute the reconcile loop daemon.
pub async fn execute_reconcile_daemon(
    pid: u64,
    ctx: ReconcileExecContext<'_>,
    scheduler: Arc<ProcessScheduler>,
) -> Result<tokio::task::JoinHandle<()>> {
    let replay_window = Duration::from_secs(10 * 24 * 60 * 60);
    let reconcile_ctx = reconcile::ReconcileContext {
        tailer: create_tailer_for_reconcile(ctx.cfg, ctx.cfg.checkpoint_path.clone(), ctx.aws_cfg)
            .await?,
        sink_tx: ctx.sender_factory.at(Priority::HIGH),
        es_counter: ctx.es_counter.clone(),
        cw_counter: ctx.cw_counter.clone(),
    };
    let params = reconcile::ReconcileParams {
        period: Duration::from_secs(ctx.cfg.reconcile_interval_secs),
        replay_window,
        buffer_cap: ctx.buffer_caps.reconcile_raw,
        resync_scale: ctx.buffer_caps.resync_per_sample,
    };

    Ok(tokio::spawn(async move {
        reconcile::run_reconcile_loop(reconcile_ctx, params).await;
        scheduler.terminate(pid, 0, Duration::ZERO).await;
    }))
}

/// Execute the full history reconcile daemon.
pub async fn execute_full_history_daemon(
    pid: u64,
    ctx: ReconcileExecContext<'_>,
    backfill_days: u32,
    scheduler: Arc<ProcessScheduler>,
) -> Result<tokio::task::JoinHandle<()>> {
    let reconcile_ctx = reconcile::ReconcileContext {
        tailer: create_tailer_for_reconcile(ctx.cfg, ctx.cfg.checkpoint_path.clone(), ctx.aws_cfg)
            .await?,
        sink_tx: ctx.sender_factory.at(Priority::NORMAL),
        es_counter: ctx.es_counter.clone(),
        cw_counter: ctx.cw_counter.clone(),
    };
    let params = reconcile::ReconcileParams {
        period: Duration::from_secs(ctx.cfg.reconcile_interval_secs),
        replay_window: Duration::from_secs(24 * 60 * 60),
        buffer_cap: ctx.buffer_caps.reconcile_raw,
        resync_scale: ctx.buffer_caps.resync_per_sample,
    };

    Ok(tokio::spawn(async move {
        reconcile::run_full_history(reconcile_ctx, params, backfill_days).await;
        scheduler.terminate(pid, 0, Duration::ZERO).await;
    }))
}

// ============================================================================
// Heal Execution
// ============================================================================

/// Heal concurrency - same as backfill for simplicity.
const HEAL_CONCURRENCY: usize = 4;

/// Execute heal days with demand-driven spawning (Linux-style).
///
/// Like run_backfill_days, this uses a BatchWorkQueue to only spawn
/// max_ready processes at a time, avoiding memory exhaustion.
pub async fn run_heal_days(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sender_factory: EventSenderFactory,
    heal_days: u32,
    buffer_caps: BufferCapacities,
) {
    if heal_days == 0 {
        return;
    }

    // Create group scheduler for heal
    let resources = Resources {
        cw_api_quota: 50,
        es_bulk_capacity: buffer_caps.heal_raw,
        memory_quota: buffer_caps.heal_raw * 5,
    };
    let group_scheduler = GroupScheduler::new(cfg.log_group.clone(), resources, HEAL_CONCURRENCY);

    // Create demand-driven work queue
    let work_queue = Arc::new(group_scheduler.create_heal_queue(heal_days));

    tracing::info!(
        "heal scheduler: {} days for {} (concurrency={}, max_ready={})",
        heal_days,
        cfg.log_group,
        HEAL_CONCURRENCY,
        HEAL_CONCURRENCY * 2
    );

    // Start the queue
    let initial_pids = work_queue.start().await;
    tracing::info!(
        "heal: spawned {} initial processes (demand-driven, {} pending)",
        initial_pids.len(),
        work_queue.pending_count().await
    );

    // Create worker pool
    let worker_pool = WorkerPool::new(group_scheduler.scheduler().clone(), HEAL_CONCURRENCY);

    // Create shared context
    let cfg_arc = Arc::new(cfg.clone());
    let aws_cfg_arc = Arc::new(aws_cfg.clone());
    let sender_factory_arc = Arc::new(sender_factory.clone());

    // Spawn workers with demand-driven factory
    let worker_handles = worker_pool.spawn_workers(work_queue.clone(), {
        let cfg = cfg_arc.clone();
        let aws_cfg = aws_cfg_arc.clone();
        let sender_factory = sender_factory_arc.clone();
        move |_pid, info| {
            let cfg = cfg.clone();
            let aws_cfg = aws_cfg.clone();
            let sender_factory = sender_factory.clone();
            async move {
                execute_heal_day(
                    &cfg,
                    &aws_cfg,
                    &sender_factory,
                    buffer_caps,
                    info.day_offset,
                )
                .await
            }
        }
    });

    // Wait for all workers
    for handle in worker_handles {
        let _ = handle.await;
    }

    let counts = group_scheduler.process_counts().await;
    tracing::info!(
        "heal complete: {} processes terminated for {}",
        counts.terminated,
        cfg.log_group
    );
}

/// Execute heal for a single day.
async fn execute_heal_day(
    cfg: &Config,
    aws_cfg: &aws_config::SdkConfig,
    sender_factory: &EventSenderFactory,
    buffer_caps: BufferCapacities,
    day_offset: u32,
) -> usize {
    let tailer = match create_tailer_for_reconcile(cfg, cfg.checkpoint_path.clone(), aws_cfg).await
    {
        Ok(t) => t,
        Err(err) => {
            tracing::error!("heal day {}: failed to create tailer: {err:?}", day_offset);
            return 0;
        }
    };

    let day = chrono::Utc::now().date_naive() - chrono::Duration::days(day_offset as i64);
    let start = day
        .and_hms_opt(0, 0, 0)
        .unwrap_or_else(|| (chrono::Utc::now() - chrono::Duration::days(365)).naive_utc());
    let end = day
        .and_hms_opt(23, 59, 59)
        .unwrap_or_else(|| chrono::Utc::now().naive_utc());

    let start_ms = start.and_utc().timestamp_millis();
    let end_ms = end.and_utc().timestamp_millis();

    let (raw_tx, mut raw_rx) = mpsc::channel::<LogEvent>(buffer_caps.heal_raw);
    let sink_tx = sender_factory.at(Priority::IDLE);

    let consumer = tokio::spawn(async move {
        let mut sent = 0usize;
        while let Some(raw) = raw_rx.recv().await {
            if let Some(enriched) = enrich_event(raw, None) {
                if sink_tx.send(enriched).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
        sent
    });

    match tailer.fetch_range(start_ms, end_ms, &raw_tx).await {
        Ok(count) => tracing::info!("heal day offset {} replayed {} events", day_offset, count),
        Err(err) => tracing::warn!("heal day offset {} error: {err:?}", day_offset),
    }

    drop(raw_tx);
    consumer.await.unwrap_or(0)
}

// ============================================================================
// Resource Calculation
// ============================================================================

/// Create resources for a group scheduler.
pub fn create_group_resources(buffer_caps: &BufferCapacities) -> Resources {
    Resources {
        cw_api_quota: 100,
        es_bulk_capacity: buffer_caps.backfill_raw,
        memory_quota: buffer_caps.backfill_raw * 10,
    }
}
