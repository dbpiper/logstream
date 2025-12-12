use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use aws_config::{timeout::TimeoutConfig, BehaviorVersion};
use dotenvy::dotenv;
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

use logstream::adaptive;
use logstream::backfill::{run_backfill_days, BACKFILL_CONCURRENCY};
use logstream::buffer::{derive_buffer_capacities, BufferCapacities};
use logstream::checkpoint::checkpoint_path_for;
use logstream::config::Config;
use logstream::cw_counts::CwCounter;
use logstream::cw_tail::{CloudWatchTailer, TailConfig};
use logstream::es_bulk_sink::{EsBulkConfig, EsBulkSink};
use logstream::es_conflicts::EsConflictResolver;
use logstream::es_counts::EsCounter;
use logstream::es_index::{
    apply_backfill_settings, cleanup_problematic_indices, drop_index_if_exists,
};
use logstream::es_recovery;
use logstream::es_schema_heal::SchemaHealer;
use logstream::event_router::{build_event_router, EventRouter, EventSenderFactory};
use logstream::process::GroupScheduler;
use logstream::process::Priority;
use logstream::runner::{
    create_group_resources, execute_conflict_daemon, execute_full_history_daemon,
    execute_reconcile_daemon, execute_tail_daemon, run_heal_days, GroupEnvConfig,
    ReconcileExecContext,
};
use logstream::seasonal_stats::SeasonalStats;
use logstream::state::CheckpointState;
use logstream::stress::{StressConfig, StressTracker};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv();
    init_tracing();

    let cfg_path = std::env::args().nth(1).map(PathBuf::from);
    let cfg = Config::load(cfg_path)?;
    info!("starting logstream with config {:?}", cfg);

    let index_prefix = cfg.index_prefix.clone();
    let buffer_caps = derive_buffer_capacities(&cfg);
    info!(
        "buffer capacities: sink_enriched={} tail_raw={} reconcile_raw={} backfill_raw={} heal_raw={} resync_per_sample={}",
        buffer_caps.sink_enriched,
        buffer_caps.tail_raw,
        buffer_caps.reconcile_raw,
        buffer_caps.backfill_raw,
        buffer_caps.heal_raw,
        buffer_caps.resync_per_sample
    );

    let aws_cfg = create_aws_config(&cfg).await;
    let (event_router, sender_factory) = create_event_router();

    let es_cfg = EsConfig::from_env();
    let sink = create_bulk_sink(&cfg, &es_cfg, index_prefix.clone())?;
    let es_stress_tracker = sink.stress_tracker();
    let cw_stress_tracker =
        std::sync::Arc::new(StressTracker::with_config(StressConfig::CLOUDWATCH));
    let seasonal_stats = std::sync::Arc::new(SeasonalStats::new());
    let adaptive_controller = adaptive::create_controller();
    info!(
        "adaptive controller: initial batch={} in_flight={}",
        adaptive_controller.batch_size(),
        adaptive_controller.max_in_flight()
    );
    sink.start_adaptive(event_router, adaptive_controller.clone());

    run_index_hygiene(&es_cfg, &index_prefix).await;
    run_schema_healing(&es_cfg, &cfg, &index_prefix).await;
    run_recovery_checks(&es_cfg, &cfg, &index_prefix).await;
    let groups = cfg.effective_log_groups();
    let mut handles = Vec::new();

    for group in groups {
        let group_checkpoint = checkpoint_path_for(&cfg.checkpoint_path, &group);
        let group_cfg = cfg.with_log_group(group.clone(), group_checkpoint.clone());
        let aws_cfg = aws_cfg.clone();
        let sender_factory = sender_factory.clone();
        let es_stress = es_stress_tracker.clone();
        let cw_stress = cw_stress_tracker.clone();
        let stats = seasonal_stats.clone();

        let handle = tokio::spawn(async move {
            if let Err(err) = run_group(
                group_cfg,
                aws_cfg,
                sender_factory,
                buffer_caps,
                es_stress,
                cw_stress,
                stats,
            )
            .await
            {
                tracing::error!("group {} failed: {err:?}", group);
            }
        });
        handles.push(handle);
    }

    signal::ctrl_c().await?;
    info!("shutdown signal received");
    for handle in handles {
        handle.abort();
    }

    Ok(())
}

struct EsConfig {
    url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
    backfill_refresh: String,
    backfill_replicas: String,
}

impl EsConfig {
    fn from_env() -> Self {
        Self {
            url: std::env::var("ES_HOST")
                .unwrap_or_else(|_| "http://localhost:9200".into())
                .into(),
            user: std::env::var("ES_USER")
                .unwrap_or_else(|_| "elastic".into())
                .into(),
            pass: std::env::var("ES_PASS")
                .unwrap_or_else(|_| "changeme".into())
                .into(),
            backfill_refresh: std::env::var("ES_BACKFILL_REFRESH_INTERVAL")
                .unwrap_or_else(|_| "-1".into()),
            backfill_replicas: std::env::var("ES_BACKFILL_REPLICAS").unwrap_or_else(|_| "0".into()),
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_max_level(Level::INFO)
        .init();
}

async fn create_aws_config(cfg: &Config) -> aws_config::SdkConfig {
    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(10))
        .operation_timeout(Duration::from_secs(cfg.http_timeout_secs))
        .build();

    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_cloudwatchlogs::config::Region::new(
            cfg.region.to_string(),
        ))
        .timeout_config(timeout_config)
        .load()
        .await
}

fn create_event_router() -> (EventRouter, EventSenderFactory) {
    let (router, sender_factory) = build_event_router();
    info!("event router: multi-level priority (CRITICAL > HIGH > NORMAL > LOW > IDLE)");
    (router, sender_factory)
}

fn create_bulk_sink(cfg: &Config, es_cfg: &EsConfig, index_prefix: Arc<str>) -> Result<EsBulkSink> {
    let max_batch = std::env::var("BULK_MAX_BATCH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(cfg.batch_size.max(20000));

    let sink_cfg = EsBulkConfig {
        url: es_cfg.url.clone(),
        user: es_cfg.user.clone(),
        pass: es_cfg.pass.clone(),
        batch_size: cfg.batch_size,
        max_batch_size: max_batch,
        timeout: cfg.http_timeout(),
        gzip: true,
        index_prefix,
    };
    EsBulkSink::new(sink_cfg)
}

async fn run_index_hygiene(es_cfg: &EsConfig, index_prefix: &str) {
    let default_index = format!("{}-default", index_prefix);
    if let Err(err) =
        drop_index_if_exists(&es_cfg.url, &es_cfg.user, &es_cfg.pass, &default_index).await
    {
        tracing::warn!("failed to drop {} before replay: {err:?}", default_index);
    } else {
        info!("dropped {} to ensure clean reindex", default_index);
    }

    if let Err(err) =
        cleanup_problematic_indices(&es_cfg.url, &es_cfg.user, &es_cfg.pass, index_prefix).await
    {
        tracing::warn!("failed to cleanup problematic indices: {err:?}");
    }

    if let Err(err) = apply_backfill_settings(
        &es_cfg.url,
        &es_cfg.user,
        &es_cfg.pass,
        index_prefix,
        &es_cfg.backfill_refresh,
        &es_cfg.backfill_replicas,
    )
    .await
    {
        tracing::warn!("failed to apply backfill index settings: {err:?}");
    } else {
        info!(
            "applied backfill settings refresh_interval={} replicas={}",
            es_cfg.backfill_refresh, es_cfg.backfill_replicas
        );
    }
}

async fn run_schema_healing(es_cfg: &EsConfig, cfg: &Config, index_prefix: &str) {
    let schema_healer = match SchemaHealer::new(
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        cfg.http_timeout(),
        index_prefix.to_string(),
    ) {
        Ok(h) => h,
        Err(err) => {
            tracing::warn!("schema_heal: failed to create healer: {err:?}");
            return;
        }
    };

    match schema_healer.heal().await {
        Ok(fixed) if fixed > 0 => info!("schema_heal: fixed {} indices with mapping drift", fixed),
        Ok(_) => info!("schema_heal: all indices have correct mappings"),
        Err(err) => tracing::warn!("schema_heal: failed: {err:?}"),
    }
}

async fn run_recovery_checks(es_cfg: &EsConfig, cfg: &Config, index_prefix: &str) {
    match es_recovery::check_on_startup(
        &es_cfg.url,
        &es_cfg.user,
        &es_cfg.pass,
        cfg.http_timeout(),
        index_prefix,
    )
    .await
    {
        Ok(()) => info!("es_recovery: startup checks passed"),
        Err(err) => tracing::warn!("es_recovery: startup check failed: {err:?}"),
    }
}

async fn run_group(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sender_factory: EventSenderFactory,
    buffer_caps: BufferCapacities,
    es_stress_tracker: std::sync::Arc<StressTracker>,
    cw_stress_tracker: std::sync::Arc<StressTracker>,
    seasonal_stats: std::sync::Arc<SeasonalStats>,
) -> Result<()> {
    let resources = create_group_resources(&buffer_caps);
    let group_scheduler =
        GroupScheduler::new(cfg.log_group.clone(), resources, BACKFILL_CONCURRENCY);

    let cw_client = aws_sdk_cloudwatchlogs::Client::new(&aws_cfg);
    let cwi_client = aws_sdk_cloudwatchlogs::Client::new(&aws_cfg);
    let checkpoint = CheckpointState::load(&cfg.checkpoint_path)?;

    let env_cfg = GroupEnvConfig::from_env(cfg.backfill_days);

    let tailer = CloudWatchTailer::with_stress_tracker(
        TailConfig {
            log_group: cfg.log_group.clone(),
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
            backoff_base: Duration::from_millis(cfg.backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.backoff_max_ms),
        },
        cw_client,
        checkpoint,
        cfg.checkpoint_path.clone(),
        cw_stress_tracker.clone(),
    );

    let es_counter = EsCounter::new(
        env_cfg.es_url.clone(),
        env_cfg.es_user.clone(),
        env_cfg.es_pass.clone(),
        cfg.http_timeout(),
        cfg.index_prefix.clone(),
    )?;
    let cw_counter = CwCounter::new(cwi_client, cfg.log_group.clone());
    let es_conflicts = EsConflictResolver::new(
        env_cfg.es_url.clone(),
        env_cfg.es_user.clone(),
        env_cfg.es_pass.clone(),
        cfg.http_timeout(),
        cfg.index_prefix.clone(),
    )?;

    info!(
        log_group = %cfg.log_group,
        disable_reconcile = %env_cfg.disable_reconcile,
        disable_conflict_reindex = %env_cfg.disable_conflict_reindex,
        backfill_days = cfg.backfill_days,
        heal_days = env_cfg.heal_days,
        "run_group: process scheduler initialized"
    );

    let tail_pid = group_scheduler.spawn_realtime_tail().await;
    let reconcile_pid = if !env_cfg.disable_reconcile {
        Some(group_scheduler.spawn_reconcile().await)
    } else {
        None
    };
    let full_history_pid = if !env_cfg.disable_reconcile {
        Some(group_scheduler.spawn_full_history_reconcile().await)
    } else {
        None
    };
    let conflict_pid = if !env_cfg.disable_conflict_reindex {
        Some(group_scheduler.spawn_conflict_reindex().await)
    } else {
        None
    };

    let counts = group_scheduler.process_counts().await;
    info!(
        log_group = %cfg.log_group,
        daemons = 1 + reconcile_pid.is_some() as usize + full_history_pid.is_some() as usize + conflict_pid.is_some() as usize,
        backfill_days = cfg.backfill_days,
        heal_days = env_cfg.heal_days,
        ready = counts.ready,
        "run_group: daemons spawned, batch work will be demand-driven"
    );

    let conflict_handle = conflict_pid.map(|pid| {
        execute_conflict_daemon(
            pid,
            es_conflicts.clone(),
            group_scheduler.scheduler().clone(),
        )
    });

    let tail_sink = sender_factory.at(Priority::CRITICAL);
    let mut tail_handle = execute_tail_daemon(
        tail_pid,
        tailer,
        tail_sink,
        buffer_caps.tail_raw,
        group_scheduler.scheduler().clone(),
    );

    let reconcile_handle = if let Some(pid) = reconcile_pid {
        let ctx = ReconcileExecContext {
            cfg: &cfg,
            aws_cfg: &aws_cfg,
            sender_factory: &sender_factory,
            es_counter: &es_counter,
            cw_counter: &cw_counter,
            buffer_caps: &buffer_caps,
            cw_stress: cw_stress_tracker.clone(),
            seasonal_stats: seasonal_stats.clone(),
        };
        Some(execute_reconcile_daemon(pid, ctx, group_scheduler.scheduler().clone()).await?)
    } else {
        None
    };

    let reconcile_full_handle = if let Some(pid) = full_history_pid {
        let ctx = ReconcileExecContext {
            cfg: &cfg,
            aws_cfg: &aws_cfg,
            sender_factory: &sender_factory,
            es_counter: &es_counter,
            cw_counter: &cw_counter,
            buffer_caps: &buffer_caps,
            cw_stress: cw_stress_tracker.clone(),
            seasonal_stats: seasonal_stats.clone(),
        };
        Some(
            execute_full_history_daemon(
                pid,
                ctx,
                cfg.backfill_days,
                group_scheduler.scheduler().clone(),
            )
            .await?,
        )
    } else {
        None
    };

    let heal_handle = if env_cfg.heal_days > 0 {
        let cfg_for_heal = cfg.clone();
        let aws_cfg_for_heal = aws_cfg.clone();
        let sender_factory_for_heal = sender_factory.clone();
        let es_stress_for_heal = es_stress_tracker.clone();
        let cw_stress_for_heal = cw_stress_tracker.clone();
        Some(tokio::spawn(async move {
            run_heal_days(
                cfg_for_heal,
                aws_cfg_for_heal,
                sender_factory_for_heal,
                env_cfg.heal_days,
                buffer_caps,
                Some(es_stress_for_heal),
                Some(cw_stress_for_heal),
            )
            .await
        }))
    } else {
        None
    };

    let log_group_for_backfill = cfg.log_group.clone();
    let log_group_for_exit = cfg.log_group.clone();
    let backfill_handle = if cfg.backfill_days > 0 {
        let cfg_for_backfill = cfg.clone();
        let aws_cfg_for_backfill = aws_cfg.clone();
        let es_stress_for_backfill = es_stress_tracker.clone();
        let cw_stress_for_backfill = cw_stress_tracker.clone();
        Some(tokio::spawn(async move {
            match run_backfill_days(
                cfg_for_backfill,
                aws_cfg_for_backfill,
                sender_factory.clone(),
                buffer_caps,
                Some(es_stress_for_backfill),
                Some(cw_stress_for_backfill),
            )
            .await
            {
                Ok(stats) => tracing::info!(
                    "backfill completed for {}: {} events, {:.2} eps",
                    log_group_for_backfill,
                    stats.total_events,
                    stats.events_per_second()
                ),
                Err(err) => {
                    tracing::error!("backfill FAILED for {}: {err:?}", log_group_for_backfill)
                }
            }
        }))
    } else {
        None
    };

    tokio::select! {
        _ = &mut tail_handle => {
            info!("tailer exited for group {}", log_group_for_exit);
        }
    }

    group_scheduler.shutdown_daemons().await;
    let final_counts = group_scheduler.process_counts().await;
    info!(
        log_group = %log_group_for_exit,
        terminated = final_counts.terminated,
        "run_group: shutting down"
    );

    if let Some(h) = reconcile_handle {
        h.abort();
    }
    if let Some(h) = reconcile_full_handle {
        h.abort();
    }
    if let Some(h) = heal_handle {
        h.abort();
    }
    if let Some(h) = backfill_handle {
        h.abort();
    }
    if let Some(h) = conflict_handle {
        h.abort();
    }
    tail_handle.abort();

    Ok(())
}
