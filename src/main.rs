mod config;
mod cw_counts;
mod cw_tail;
mod enrich;
mod es_bulk_sink;
mod es_conflicts;
mod es_counts;
mod es_schema_heal;
mod reconcile;
mod state;
mod types;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use aws_config::{timeout::TimeoutConfig, BehaviorVersion};
use chrono::{Duration as ChronoDuration, Utc};
use config::Config;
use cw_counts::CwCounter;
use cw_tail::{CloudWatchTailer, TailConfig};
use dotenvy::dotenv;
use enrich::enrich_event;
use es_bulk_sink::{EsBulkConfig, EsBulkSink};
use es_conflicts::EsConflictResolver;
use es_counts::EsCounter;
use reqwest::Client;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

const SINK_ENRICHED_MULTIPLIER: usize = 4;
const RAW_TAIL_INFLIGHT_MULTIPLIER: usize = 5;
const RAW_TAIL_FLOOR: usize = 1024;
const RECONCILE_INFLIGHT_MULTIPLIER: usize = 4;
const RECONCILE_FLOOR: usize = 2048;
const HEAL_INFLIGHT_MULTIPLIER: usize = 4;
const HEAL_FLOOR: usize = 2048;
const BACKFILL_INFLIGHT_MULTIPLIER: usize = 10;
const BACKFILL_FLOOR: usize = 4096;
const RESYNC_PER_SAMPLE_MULTIPLIER: usize = 8;
const RESYNC_PER_SAMPLE_FLOOR: usize = 16;

#[derive(Clone, Copy)]
struct BufferCapacities {
    sink_enriched: usize,
    tail_raw: usize,
    reconcile_raw: usize,
    backfill_raw: usize,
    heal_raw: usize,
    resync_per_sample: usize,
}

struct GroupContext {
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sink_tx: mpsc::Sender<types::EnrichedEvent>,
    buffer_caps: BufferCapacities,
}

fn derive_buffer_capacities(cfg: &Config) -> BufferCapacities {
    let base_batch = cfg.batch_size.max(1);
    let inflight = base_batch.saturating_mul(cfg.max_in_flight.max(1));
    let sink_enriched = base_batch.saturating_mul(SINK_ENRICHED_MULTIPLIER);
    let tail_raw = scaled_capacity(
        inflight,
        RAW_TAIL_INFLIGHT_MULTIPLIER,
        RAW_TAIL_FLOOR,
        sink_enriched,
    );
    let reconcile_raw = scaled_capacity(
        inflight,
        RECONCILE_INFLIGHT_MULTIPLIER,
        RECONCILE_FLOOR,
        sink_enriched,
    );
    let heal_raw = scaled_capacity(
        inflight,
        HEAL_INFLIGHT_MULTIPLIER,
        HEAL_FLOOR,
        sink_enriched,
    );
    let backfill_raw = scaled_capacity(
        inflight,
        BACKFILL_INFLIGHT_MULTIPLIER,
        BACKFILL_FLOOR,
        reconcile_raw,
    );
    let resync_per_sample = cfg
        .max_in_flight
        .max(1)
        .saturating_mul(RESYNC_PER_SAMPLE_MULTIPLIER)
        .max(RESYNC_PER_SAMPLE_FLOOR);

    BufferCapacities {
        sink_enriched,
        tail_raw,
        reconcile_raw,
        backfill_raw,
        heal_raw,
        resync_per_sample,
    }
}

fn scaled_capacity(inflight: usize, multiplier: usize, floor: usize, baseline: usize) -> usize {
    inflight.saturating_mul(multiplier).max(floor).max(baseline)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment from .env if present (for log group, region, etc.)
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

    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(10))
        .operation_timeout(Duration::from_secs(cfg.http_timeout_secs))
        .build();

    let aws_cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_sdk_cloudwatchlogs::config::Region::new(
            cfg.region.clone(),
        ))
        .timeout_config(timeout_config)
        .load()
        .await;
    let (tx_sink, rx_sink) = mpsc::channel::<types::EnrichedEvent>(buffer_caps.sink_enriched);
    let es_url = std::env::var("ES_HOST").unwrap_or_else(|_| "http://localhost:9200".into());
    let es_user = std::env::var("ES_USER").unwrap_or_else(|_| "elastic".into());
    let es_pass = std::env::var("ES_PASS").unwrap_or_else(|_| "changeme".into());

    let es_backfill_refresh =
        std::env::var("ES_BACKFILL_REFRESH_INTERVAL").unwrap_or_else(|_| "-1".into());
    let es_backfill_replicas = std::env::var("ES_BACKFILL_REPLICAS").unwrap_or_else(|_| "0".into());

    let max_batch = std::env::var("BULK_MAX_BATCH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(cfg.batch_size.max(20000));
    let sink_cfg = EsBulkConfig {
        url: es_url.clone(),
        user: es_user.clone(),
        pass: es_pass.clone(),
        batch_size: cfg.batch_size,
        max_batch_size: max_batch,
        max_in_flight: cfg.max_in_flight,
        timeout: cfg.http_timeout(),
        gzip: true,
        index_prefix: index_prefix.clone(),
    };
    let sink = EsBulkSink::new(sink_cfg)?;
    sink.start(rx_sink);

    // Always ensure index hygiene and backfill-friendly settings
    let default_index = format!("{}-default", index_prefix);
    if let Err(err) = drop_index_if_exists(&es_url, &es_user, &es_pass, &default_index).await {
        tracing::warn!("failed to drop {} before replay: {err:?}", default_index);
    } else {
        info!("dropped {} to ensure clean reindex", default_index);
    }
    if let Err(err) = cleanup_problematic_indices(&es_url, &es_user, &es_pass, &index_prefix).await
    {
        tracing::warn!("failed to cleanup problematic indices: {err:?}");
    }
    if let Err(err) = apply_backfill_settings(
        &es_url,
        &es_user,
        &es_pass,
        &index_prefix,
        &es_backfill_refresh,
        &es_backfill_replicas,
    )
    .await
    {
        tracing::warn!("failed to apply backfill index settings: {err:?}");
    } else {
        info!(
            "applied backfill settings refresh_interval={} replicas={}",
            es_backfill_refresh, es_backfill_replicas
        );
    }

    // Run schema healing to fix any indices with drifted mappings (O(log n) detection)
    let schema_healer = es_schema_heal::SchemaHealer::new(
        es_url.clone(),
        es_user.clone(),
        es_pass.clone(),
        cfg.http_timeout(),
        index_prefix.clone(),
    )?;
    match schema_healer.heal().await {
        Ok(fixed) if fixed > 0 => info!("schema_heal: fixed {} indices with mapping drift", fixed),
        Ok(_) => info!("schema_heal: all indices have correct mappings"),
        Err(err) => tracing::warn!("schema_heal: failed: {err:?}"),
    }

    let groups = cfg.effective_log_groups();
    let mut handles = Vec::new();

    for group in groups {
        let group_checkpoint = checkpoint_path_for(&cfg.checkpoint_path, &group);
        let group_cfg = cfg.with_log_group(group.clone(), group_checkpoint.clone());
        let ctx = GroupContext {
            cfg: group_cfg,
            aws_cfg: aws_cfg.clone(),
            sink_tx: tx_sink.clone(),
            buffer_caps,
        };

        let handle = tokio::spawn(async move {
            if let Err(err) = run_group(ctx).await {
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

async fn drop_index_if_exists(base_url: &str, user: &str, pass: &str, index: &str) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let url = format!("{}/{}", base_url.trim_end_matches('/'), index);
    let resp = client
        .delete(&url)
        .basic_auth(user, Some(pass))
        .send()
        .await?;
    if resp.status().as_u16() == 404 {
        return Ok(());
    }
    if !resp.status().is_success() {
        anyhow::bail!("drop index {} status {}", index, resp.status());
    }
    Ok(())
}

/// Delete temp and broken indices.
async fn cleanup_problematic_indices(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    // Get all indices with the target prefix
    let url = format!(
        "{}/_cat/indices/{}-*?format=json",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        return Ok(()); // Skip if we can't list indices
    }

    let indices: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();

    for idx in indices {
        let name = idx.get("index").and_then(|v| v.as_str()).unwrap_or("");
        let docs = idx
            .get("docs.count")
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        let health = idx.get("health").and_then(|v| v.as_str()).unwrap_or("");

        // Delete temp indices or empty indices with red health
        let should_delete =
            name.contains("-temp") || (docs == "0" && health == "red") || name.ends_with("-temp");

        if should_delete {
            info!(
                "cleaning up problematic index: {} (docs={}, health={})",
                name, docs, health
            );
            let _ = drop_index_if_exists(base_url, user, pass, name).await;
        }
    }

    Ok(())
}

/// Configure ES indices for bulk ingest.
async fn apply_backfill_settings(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
    refresh_interval: &str,
    replicas: &str,
) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let url = format!(
        "{}/{}-*/_settings",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let body = serde_json::json!({
        "index": {
            "refresh_interval": refresh_interval,
            "number_of_replicas": replicas
        }
    });
    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("apply backfill settings status {}", resp.status());
    }
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_max_level(Level::INFO)
        .init();
}

async fn run_group(ctx: GroupContext) -> Result<()> {
    let cfg = ctx.cfg;
    let aws_cfg = ctx.aws_cfg;
    let sink_tx = ctx.sink_tx;
    let buffer_caps = ctx.buffer_caps;

    let cw_client = aws_sdk_cloudwatchlogs::Client::new(&aws_cfg);
    let cwi_client = aws_sdk_cloudwatchlogs::Client::new(&aws_cfg);
    let checkpoint = state::CheckpointState::load(&cfg.checkpoint_path)?;
    let es_url = std::env::var("ES_HOST").unwrap_or_else(|_| "http://localhost:9200".into());
    let es_user = std::env::var("ES_USER").unwrap_or_else(|_| "elastic".into());
    let es_pass = std::env::var("ES_PASS").unwrap_or_else(|_| "changeme".into());

    let mut tailer = CloudWatchTailer::new(
        TailConfig {
            log_group: cfg.log_group.clone(),
            poll_interval: Duration::from_secs(cfg.poll_interval_secs),
            backoff_base: Duration::from_millis(cfg.backoff_base_ms),
            backoff_max: Duration::from_millis(cfg.backoff_max_ms),
        },
        cw_client,
        checkpoint,
        cfg.checkpoint_path.clone(),
    );

    let es_counter = EsCounter::new(
        es_url.clone(),
        es_user.clone(),
        es_pass.clone(),
        cfg.http_timeout(),
        cfg.index_prefix.clone(),
    )?;
    let cw_counter = CwCounter::new(cwi_client, cfg.log_group.clone());
    let es_conflicts = EsConflictResolver::new(
        es_url,
        es_user,
        es_pass,
        cfg.http_timeout(),
        cfg.index_prefix.clone(),
    )?;

    let disable_conflict_reindex = std::env::var("DISABLE_CONFLICT_REINDEX")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    info!(
        log_group = %cfg.log_group,
        disable_reconcile = %std::env::var("DISABLE_RECONCILE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false),
        backfill_days = cfg.backfill_days,
        heal_days_hint = %std::env::var("RECENT_HEAL_DAYS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(30),
        poll_interval_secs = cfg.poll_interval_secs,
        reconcile_interval_secs = cfg.reconcile_interval_secs,
        sink_enriched = buffer_caps.sink_enriched,
        tail_raw = buffer_caps.tail_raw,
        reconcile_raw = buffer_caps.reconcile_raw,
        backfill_raw = buffer_caps.backfill_raw,
        heal_raw = buffer_caps.heal_raw,
        resync_per_sample = buffer_caps.resync_per_sample,
        "run_group settings"
    );
    if !disable_conflict_reindex {
        let conflicts_clone = es_conflicts.clone();
        tokio::spawn(async move {
            if let Err(err) = conflicts_clone.run_conflict_reindex().await {
                tracing::warn!("conflict reindex failed: {err:?}");
            }
        });
    }

    let tail_sink = sink_tx.clone();
    let mut tail_handle = tokio::spawn(async move {
        let (raw_tx, mut raw_rx) = mpsc::channel::<types::LogEvent>(buffer_caps.tail_raw);
        tokio::spawn(async move {
            while let Some(raw) = raw_rx.recv().await {
                if let Some(enriched) = enrich_event(raw, None) {
                    if tail_sink.send(enriched).await.is_err() {
                        break;
                    }
                }
            }
        });

        if let Err(err) = tailer.run(raw_tx).await {
            tracing::error!("tailer failed: {err:?}");
        }
    });

    let disable_reconcile = std::env::var("DISABLE_RECONCILE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let reconcile_handle = if disable_reconcile {
        None
    } else {
        let replay_window = Duration::from_secs(10 * 24 * 60 * 60);
        let ctx = reconcile::ReconcileContext {
            tailer: tailer_clone_for_reconcile(
                cfg.clone(),
                cfg.checkpoint_path.clone(),
                aws_cfg.clone(),
            )
            .await?,
            sink_tx: sink_tx.clone(),
            es_counter: es_counter.clone(),
            cw_counter: cw_counter.clone(),
        };
        let params = reconcile::ReconcileParams {
            period: Duration::from_secs(cfg.reconcile_interval_secs),
            replay_window,
            buffer_cap: buffer_caps.reconcile_raw,
            resync_scale: buffer_caps.resync_per_sample,
        };
        Some(tokio::spawn(reconcile::run_reconcile_loop(ctx, params)))
    };

    let reconcile_full_handle = if disable_reconcile || cfg.backfill_days > 0 {
        None
    } else {
        let ctx = reconcile::ReconcileContext {
            tailer: tailer_clone_for_reconcile(
                cfg.clone(),
                cfg.checkpoint_path.clone(),
                aws_cfg.clone(),
            )
            .await?,
            sink_tx: sink_tx.clone(),
            es_counter: es_counter.clone(),
            cw_counter: cw_counter.clone(),
        };
        let params = reconcile::ReconcileParams {
            period: Duration::from_secs(cfg.reconcile_interval_secs),
            replay_window: Duration::from_secs(24 * 60 * 60),
            buffer_cap: buffer_caps.reconcile_raw,
            resync_scale: buffer_caps.resync_per_sample,
        };
        Some(tokio::spawn(reconcile::run_full_history(
            ctx,
            params,
            cfg.backfill_days,
        )))
    };

    let heal_days = if cfg.backfill_days > 0 {
        0
    } else {
        std::env::var("RECENT_HEAL_DAYS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(30)
    };
    let heal_handle = tokio::spawn(run_recent_heal(
        cfg.clone(),
        aws_cfg.clone(),
        sink_tx.clone(),
        heal_days,
        buffer_caps,
    ));

    let backfill_handle = tokio::spawn(run_backfill_days(
        cfg.clone(),
        aws_cfg.clone(),
        sink_tx.clone(),
        buffer_caps,
    ));

    tokio::select! {
        _ = &mut tail_handle => {
            info!("tailer exited for group {}", cfg.log_group);
        }
    }

    if let Some(h) = reconcile_handle {
        h.abort();
    }
    if let Some(h) = reconcile_full_handle {
        h.abort();
    }
    heal_handle.abort();
    backfill_handle.abort();
    tail_handle.abort();

    Ok(())
}

async fn tailer_clone_for_reconcile(
    cfg: Config,
    checkpoint_path: PathBuf,
    aws_cfg: aws_config::SdkConfig,
) -> Result<CloudWatchTailer> {
    let cw_client = aws_sdk_cloudwatchlogs::Client::new(&aws_cfg);
    let checkpoint = state::CheckpointState::load(&checkpoint_path)?;
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

async fn run_backfill_days(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sink_tx: mpsc::Sender<types::EnrichedEvent>,
    buffer_caps: BufferCapacities,
) -> Result<()> {
    if cfg.backfill_days == 0 {
        return Ok(());
    }

    let tailer =
        tailer_clone_for_reconcile(cfg.clone(), cfg.checkpoint_path.clone(), aws_cfg.clone())
            .await?;

    tracing::info!(
        "backfill starting: days 0-{} for log group {} (batch parallel)",
        cfg.backfill_days - 1,
        cfg.log_group
    );

    // Process days sequentially to avoid CW connect timeouts
    let batch_size = 1u32;
    let mut offset = 0u32;

    while offset < cfg.backfill_days {
        let batch_end = (offset + batch_size).min(cfg.backfill_days);
        let mut batch_handles = Vec::new();

        for day_offset in offset..batch_end {
            let tailer = tailer.clone();
            let sink_tx = sink_tx.clone();
            let log_group = cfg.log_group.clone();

            batch_handles.push(tokio::spawn(async move {
                let day = Utc::now().date_naive() - ChronoDuration::days(day_offset as i64);
                let start = day
                    .and_hms_opt(0, 0, 0)
                    .unwrap_or_else(|| (Utc::now() - ChronoDuration::days(365)).naive_utc());
                let end = day
                    .and_hms_opt(23, 59, 59)
                    .unwrap_or_else(|| Utc::now().naive_utc());

                let start_ms = start.and_utc().timestamp_millis();
                let end_ms = end.and_utc().timestamp_millis();

                let (raw_tx, mut raw_rx) =
                    mpsc::channel::<types::LogEvent>(buffer_caps.backfill_raw);
                let sink_clone = sink_tx.clone();
                let log_group_clone = log_group.clone();
                let consumer_handle = tokio::spawn(async move {
                    let mut sent = 0usize;
                    while let Some(raw) = raw_rx.recv().await {
                        if let Some(enriched) = crate::enrich::enrich_event(raw, None) {
                            if sink_clone.send(enriched).await.is_err() {
                                break;
                            }
                            sent += 1;
                        }
                    }
                    tracing::info!(
                        "backfill {} day {} drained {} events",
                        log_group_clone,
                        day,
                        sent
                    );
                    sent
                });

                let started = std::time::Instant::now();
                let fetch_result = tailer.fetch_range(start_ms, end_ms, &raw_tx).await;
                drop(raw_tx);

                if let Ok(count) = fetch_result {
                    let elapsed_ms = started.elapsed().as_millis();
                    let eps = if elapsed_ms > 0 {
                        count as u128 * 1000 / elapsed_ms
                    } else {
                        0
                    };
                    tracing::info!(
                        "backfill {} day {} fetched {} events in {}ms ({} eps)",
                        log_group,
                        day,
                        count,
                        elapsed_ms,
                        eps
                    );
                }

                let _ = consumer_handle.await;
            }));
        }

        // Wait for this batch to complete before starting next batch
        for h in batch_handles {
            let _ = h.await;
        }

        offset = batch_end;
    }

    tracing::info!("backfill complete for log group {}", cfg.log_group);
    Ok(())
}

async fn run_recent_heal(
    cfg: Config,
    aws_cfg: aws_config::SdkConfig,
    sink_tx: mpsc::Sender<types::EnrichedEvent>,
    days: u32,
    buffer_caps: BufferCapacities,
) -> Result<()> {
    if days == 0 {
        return Ok(());
    }
    let tailer =
        tailer_clone_for_reconcile(cfg.clone(), cfg.checkpoint_path.clone(), aws_cfg.clone())
            .await?;

    for day_offset in 0..days {
        let day = chrono::Utc::now().date_naive() - chrono::Duration::days(day_offset as i64);
        let start = day
            .and_hms_opt(0, 0, 0)
            .unwrap_or_else(|| (chrono::Utc::now() - chrono::Duration::days(365)).naive_utc());
        let end = day
            .and_hms_opt(23, 59, 59)
            .unwrap_or_else(|| chrono::Utc::now().naive_utc());

        let start_ms = start.and_utc().timestamp_millis();
        let end_ms = end.and_utc().timestamp_millis();

        let (raw_tx, mut raw_rx) = mpsc::channel::<types::LogEvent>(buffer_caps.heal_raw);
        let sink_clone = sink_tx.clone();
        tokio::spawn(async move {
            while let Some(raw) = raw_rx.recv().await {
                if let Some(enriched) = crate::enrich::enrich_event(raw, None) {
                    if sink_clone.send(enriched).await.is_err() {
                        break;
                    }
                }
            }
        });

        match tailer.fetch_range(start_ms, end_ms, &raw_tx).await {
            Ok(count) => tracing::info!(
                "recent heal day offset {} replayed {} events",
                day_offset,
                count
            ),
            Err(err) => tracing::warn!("recent heal day offset {} error: {err:?}", day_offset),
        }
    }

    Ok(())
}

fn checkpoint_path_for(base: &Path, group: &str) -> PathBuf {
    let slug = group
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    if base.extension().is_some() {
        base.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("checkpoints-{slug}.json"))
    } else {
        base.join(format!("checkpoints-{slug}.json"))
    }
}
