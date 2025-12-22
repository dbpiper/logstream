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
use logstream::enrich::sanitize_log_group_name;
use logstream::es_bootstrap::start_es_bootstrap;
use logstream::es_bulk_sink::{EsBulkConfig, EsBulkSink};
use logstream::es_conflicts::EsConflictResolver;
use logstream::es_counts::EsCounter;
use logstream::es_disk_guard::{start_es_disk_guard, EsDiskGuardConfig};
use logstream::es_http::EsHttp;
use logstream::es_index::{cleanup_problematic_indices, drop_index_if_exists};
use logstream::es_recovery;
use logstream::es_refresh_tuner::{start_refresh_tuner, RefreshTunerConfig};
use logstream::es_schema_heal::SchemaHealer;
use logstream::event_router::{build_event_router, EventRouter, EventSenderFactory};
use logstream::naming;
use logstream::process::GroupScheduler;
use logstream::process::Priority;
use logstream::prune_state::PruneState;
use logstream::runner::HealRunContext;
use logstream::runner::{
    create_group_resources, execute_conflict_daemon, execute_full_history_daemon,
    execute_reconcile_daemon, execute_tail_daemon, run_heal_days, GroupEnvConfig,
    ReconcileExecContext,
};
use logstream::seasonal_stats::SeasonalStats;
use logstream::state::CheckpointState;
use logstream::stress::{StressConfig, StressTracker};
use logstream::time_windows;

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
    // Avoid crash loops when ES is still starting up: wait until it answers health checks.
    loop {
        match wait_for_es_ready(&es_cfg).await {
            Ok(()) => break,
            Err(err) => {
                tracing::warn!("ES not ready yet: {err:?}; retrying in 1s");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // Ensure templates/ILM/data streams/aliases exist before any ingest or migration.
    logstream::es_bootstrap::bootstrap_now(
        &cfg,
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
    )
    .await?;

    let es_bootstrap = start_es_bootstrap(
        &cfg,
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
    );
    let sink = create_bulk_sink(
        &cfg,
        &es_cfg,
        index_prefix.clone(),
        es_bootstrap.as_ref().map(|h| h.notifier()),
    )?;
    let es_stress_tracker = sink.stress_tracker();
    let cw_stress_tracker =
        std::sync::Arc::new(StressTracker::with_config(StressConfig::CLOUDWATCH));
    let seasonal_stats_path = cfg.checkpoint_path.with_file_name("seasonal_stats.json");
    let seasonal_stats = std::sync::Arc::new(SeasonalStats::load_or_new(&seasonal_stats_path));

    let stats_for_save = seasonal_stats.clone();
    let save_path = seasonal_stats_path.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
        loop {
            interval.tick().await;
            if let Err(e) = stats_for_save.save(&save_path) {
                tracing::warn!("failed to save seasonal_stats: {e}");
            }
        }
    });
    let adaptive_controller = adaptive::create_controller();
    info!(
        "adaptive controller: initial batch={} in_flight={}",
        adaptive_controller.batch_size(),
        adaptive_controller.max_in_flight()
    );
    sink.start_adaptive(event_router, adaptive_controller.clone());
    sink.start_heap_monitor(adaptive_controller.clone());

    let prune_state_path = cfg.checkpoint_path.with_file_name("prune_watermarks.json");
    let prune_state = std::sync::Arc::new(PruneState::load_or_new(prune_state_path)?);

    start_refresh_tuner(
        cfg.clone(),
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        cfg.http_timeout(),
        RefreshTunerConfig {
            hot_refresh_interval: es_cfg.refresh_hot.clone(),
            cold_refresh_interval: es_cfg.refresh_cold.clone(),
            cold_age_days: es_cfg.refresh_cold_age_days,
            interval_secs: es_cfg.refresh_tune_interval_secs,
        },
    );
    start_es_disk_guard(
        cfg.clone(),
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        cfg.http_timeout(),
        EsDiskGuardConfig {
            enabled: es_cfg.disk_guard_enabled,
            interval_secs: es_cfg.disk_guard_interval_secs,
            free_buffer_gb: es_cfg.disk_guard_free_buffer_gb,
            min_keep_days: es_cfg.disk_guard_min_keep_days,
        },
        prune_state.clone(),
    );
    run_index_hygiene(&es_cfg, &index_prefix).await;
    run_schema_healing(&es_cfg, &cfg).await;
    run_recovery_checks(&es_cfg, &cfg).await;
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
        let prune_state_for_group = prune_state.clone();

        let handle = tokio::spawn(async move {
            if let Err(err) = run_group(RunGroupContext {
                cfg: group_cfg,
                aws_cfg,
                sender_factory,
                buffer_caps,
                es_stress_tracker: es_stress,
                cw_stress_tracker: cw_stress,
                seasonal_stats: stats,
                prune_state: prune_state_for_group,
            })
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
    refresh_hot: Arc<str>,
    refresh_cold: Arc<str>,
    refresh_cold_age_days: u64,
    refresh_tune_interval_secs: u64,
    disk_guard_enabled: bool,
    disk_guard_interval_secs: u64,
    disk_guard_free_buffer_gb: u64,
    disk_guard_min_keep_days: u64,
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
            refresh_hot: std::env::var("ES_HOT_REFRESH_INTERVAL")
                .unwrap_or_else(|_| "1s".into())
                .into(),
            refresh_cold: std::env::var("ES_COLD_REFRESH_INTERVAL")
                .unwrap_or_else(|_| "30s".into())
                .into(),
            refresh_cold_age_days: std::env::var("ES_COLD_AGE_DAYS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(7),
            refresh_tune_interval_secs: std::env::var("ES_REFRESH_TUNE_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(3600),
            disk_guard_enabled: std::env::var("ES_DISK_GUARD_ENABLED")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            disk_guard_interval_secs: std::env::var("ES_DISK_GUARD_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60),
            disk_guard_free_buffer_gb: std::env::var("ES_DISK_GUARD_FREE_BUFFER_GB")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(2),
            disk_guard_min_keep_days: std::env::var("ES_DISK_GUARD_MIN_KEEP_DAYS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(7),
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

fn create_bulk_sink(
    cfg: &Config,
    es_cfg: &EsConfig,
    index_prefix: Arc<str>,
    bootstrap_notify: Option<std::sync::Arc<tokio::sync::Notify>>,
) -> Result<EsBulkSink> {
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
        bootstrap_notify,
    };
    EsBulkSink::new(sink_cfg)
}

async fn run_index_hygiene(es_cfg: &EsConfig, index_prefix: &str) {
    const MAX_RETRIES: u32 = 10;
    const RETRY_DELAY_MS: u64 = 2000;

    for attempt in 1..=MAX_RETRIES {
        match wait_for_es_ready(es_cfg).await {
            Ok(_) => {
                info!("ES ready, running index hygiene");
                break;
            }
            Err(err) if attempt < MAX_RETRIES => {
                tracing::warn!(
                    "ES not ready (attempt {}/{}): {err:?}, retrying in {}ms",
                    attempt,
                    MAX_RETRIES,
                    RETRY_DELAY_MS
                );
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
            Err(err) => {
                tracing::warn!(
                    "ES not ready after {} attempts: {err:?}, skipping hygiene",
                    MAX_RETRIES
                );
                return;
            }
        }
    }

    let default_index = format!("{}-default", index_prefix);
    let http = match EsHttp::new(
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        Duration::from_secs(30),
        true,
    ) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!("failed to create es http client for hygiene: {err:?}");
            return;
        }
    };
    if let Err(err) = drop_index_if_exists(&http, &default_index).await {
        tracing::warn!("failed to drop {} before replay: {err:?}", default_index);
    } else {
        info!("dropped {} to ensure clean reindex", default_index);
    }

    if let Err(err) = cleanup_problematic_indices(&http, index_prefix).await {
        tracing::warn!("failed to cleanup problematic indices: {err:?}");
    }
}

async fn wait_for_es_ready(es_cfg: &EsConfig) -> Result<()> {
    let http = EsHttp::new(
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        Duration::from_secs(5),
        true,
    )?;
    let resp = http
        .request(reqwest::Method::GET, "_cluster/health")
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("ES health check failed: {}", resp.status());
    }
    Ok(())
}

async fn run_schema_healing(es_cfg: &EsConfig, cfg: &Config) {
    let schema_healer = match SchemaHealer::new(
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        cfg.http_timeout(),
    ) {
        Ok(h) => h,
        Err(err) => {
            tracing::warn!("schema_heal: failed to create healer: {err:?}");
            return;
        }
    };

    let (start_ms, end_ms) = time_windows::utc_prev_day_bounds_ms();

    for group in cfg.effective_log_groups() {
        let stable = naming::stable_alias(&cfg.index_prefix, &group);
        match schema_healer.heal_window(&stable, start_ms, end_ms).await {
            Ok(true) => info!(
                "schema_heal: repaired window alias={} start={} end={}",
                stable, start_ms, end_ms
            ),
            Ok(false) => {}
            Err(err) => tracing::warn!("schema_heal: failed alias={} err={err:?}", stable),
        }
    }
}

async fn run_recovery_checks(es_cfg: &EsConfig, cfg: &Config) {
    let http = match EsHttp::new(
        es_cfg.url.clone(),
        es_cfg.user.clone(),
        es_cfg.pass.clone(),
        cfg.http_timeout(),
        true,
    ) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!("es_recovery: failed to create http client: {err:?}");
            return;
        }
    };
    match es_recovery::check_on_startup(&http, cfg.http_timeout()).await {
        Ok(()) => info!("es_recovery: startup checks passed"),
        Err(err) => tracing::warn!("es_recovery: startup check failed: {err:?}"),
    }
}

struct RunGroupContext {
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sender_factory: EventSenderFactory,
    buffer_caps: BufferCapacities,
    es_stress_tracker: std::sync::Arc<StressTracker>,
    cw_stress_tracker: std::sync::Arc<StressTracker>,
    seasonal_stats: std::sync::Arc<SeasonalStats>,
    prune_state: std::sync::Arc<PruneState>,
}

async fn run_group(ctx: RunGroupContext) -> Result<()> {
    let sender_factory = ctx.sender_factory.clone();
    let buffer_caps = ctx.buffer_caps;
    let resources = create_group_resources(&ctx.buffer_caps);
    let group_scheduler =
        GroupScheduler::new(ctx.cfg.log_group.clone(), resources, BACKFILL_CONCURRENCY);

    let cw_client = aws_sdk_cloudwatchlogs::Client::new(&ctx.aws_cfg);
    let cwi_client = aws_sdk_cloudwatchlogs::Client::new(&ctx.aws_cfg);
    let checkpoint = CheckpointState::load(&ctx.cfg.checkpoint_path)?;

    let env_cfg = GroupEnvConfig::from_env(ctx.cfg.backfill_days);

    let tailer = CloudWatchTailer::with_stress_tracker(
        TailConfig {
            log_group: ctx.cfg.log_group.clone(),
            poll_interval: Duration::from_secs(ctx.cfg.poll_interval_secs),
            backoff_base: Duration::from_millis(ctx.cfg.backoff_base_ms),
            backoff_max: Duration::from_millis(ctx.cfg.backoff_max_ms),
        },
        cw_client,
        checkpoint,
        ctx.cfg.checkpoint_path.clone(),
        ctx.cw_stress_tracker.clone(),
    );

    let group_slug = sanitize_log_group_name(&ctx.cfg.log_group);
    let es_target = format!("{}-{}", ctx.cfg.index_prefix, group_slug);
    let es_counter = EsCounter::new(
        env_cfg.es_url.clone(),
        env_cfg.es_user.clone(),
        env_cfg.es_pass.clone(),
        ctx.cfg.http_timeout(),
        es_target,
    )?;
    let cw_counter = CwCounter::new(cwi_client, ctx.cfg.log_group.clone());
    let es_conflicts = EsConflictResolver::new(
        env_cfg.es_url.clone(),
        env_cfg.es_user.clone(),
        env_cfg.es_pass.clone(),
        ctx.cfg.http_timeout(),
        ctx.cfg.index_prefix.clone(),
    )?;

    info!(
        log_group = %ctx.cfg.log_group,
        disable_reconcile = %env_cfg.disable_reconcile,
        disable_conflict_reindex = %env_cfg.disable_conflict_reindex,
        backfill_days = ctx.cfg.backfill_days,
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

    let counts = group_scheduler.process_counts();
    info!(
        log_group = %ctx.cfg.log_group,
        daemons = 1 + reconcile_pid.is_some() as usize + full_history_pid.is_some() as usize + conflict_pid.is_some() as usize,
        backfill_days = ctx.cfg.backfill_days,
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
        ctx.buffer_caps.tail_raw,
        ctx.cfg.log_group.clone(),
        group_scheduler.scheduler().clone(),
    );

    let reconcile_handle = if let Some(pid) = reconcile_pid {
        let ctx = ReconcileExecContext {
            cfg: &ctx.cfg,
            aws_cfg: &ctx.aws_cfg,
            sender_factory: &sender_factory,
            es_counter: &es_counter,
            cw_counter: &cw_counter,
            buffer_caps: &ctx.buffer_caps,
            cw_stress: ctx.cw_stress_tracker.clone(),
            seasonal_stats: ctx.seasonal_stats.clone(),
            es_url: &env_cfg.es_url,
            es_user: &env_cfg.es_user,
            es_pass: &env_cfg.es_pass,
            index_prefix: &ctx.cfg.index_prefix,
            prune_state: ctx.prune_state.clone(),
        };
        Some(execute_reconcile_daemon(pid, ctx, group_scheduler.scheduler().clone()).await?)
    } else {
        None
    };

    let reconcile_full_handle = if let Some(pid) = full_history_pid {
        let backfill_days = ctx.cfg.backfill_days;
        let exec_ctx = ReconcileExecContext {
            cfg: &ctx.cfg,
            aws_cfg: &ctx.aws_cfg,
            sender_factory: &sender_factory,
            es_counter: &es_counter,
            cw_counter: &cw_counter,
            buffer_caps: &ctx.buffer_caps,
            cw_stress: ctx.cw_stress_tracker.clone(),
            seasonal_stats: ctx.seasonal_stats.clone(),
            es_url: &env_cfg.es_url,
            es_user: &env_cfg.es_user,
            es_pass: &env_cfg.es_pass,
            index_prefix: &ctx.cfg.index_prefix,
            prune_state: ctx.prune_state.clone(),
        };
        Some(
            execute_full_history_daemon(
                pid,
                exec_ctx,
                backfill_days,
                group_scheduler.scheduler().clone(),
            )
            .await?,
        )
    } else {
        None
    };

    let heal_handle = if env_cfg.heal_days > 0 {
        let cfg_for_heal = ctx.cfg.clone();
        let aws_cfg_for_heal = ctx.aws_cfg.clone();
        let sender_factory_for_heal = sender_factory.clone();
        let es_stress_for_heal = ctx.es_stress_tracker.clone();
        let cw_stress_for_heal = ctx.cw_stress_tracker.clone();
        let prune_state_for_heal = ctx.prune_state.clone();
        let stable_alias_for_heal = std::sync::Arc::<str>::from(format!(
            "{}-{}",
            cfg_for_heal.index_prefix,
            sanitize_log_group_name(&cfg_for_heal.log_group)
        ));
        Some(tokio::spawn(async move {
            run_heal_days(
                HealRunContext {
                    cfg: cfg_for_heal,
                    aws_cfg: aws_cfg_for_heal,
                    sender_factory: sender_factory_for_heal,
                    buffer_caps,
                    prune_state: prune_state_for_heal,
                    stable_alias: stable_alias_for_heal,
                },
                env_cfg.heal_days,
                Some(es_stress_for_heal),
                Some(cw_stress_for_heal),
            )
            .await
        }))
    } else {
        None
    };

    let log_group_for_backfill = ctx.cfg.log_group.clone();
    let log_group_for_exit = ctx.cfg.log_group.clone();
    let backfill_handle = if ctx.cfg.backfill_days > 0 {
        let cfg_for_backfill = ctx.cfg.clone();
        let aws_cfg_for_backfill = ctx.aws_cfg.clone();
        let es_stress_for_backfill = ctx.es_stress_tracker.clone();
        let cw_stress_for_backfill = ctx.cw_stress_tracker.clone();
        let prune_state_for_backfill = ctx.prune_state.clone();
        Some(tokio::spawn(async move {
            match run_backfill_days(
                cfg_for_backfill,
                aws_cfg_for_backfill,
                sender_factory.clone(),
                buffer_caps,
                Some(es_stress_for_backfill),
                Some(cw_stress_for_backfill),
                prune_state_for_backfill,
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
    let final_counts = group_scheduler.process_counts();
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
