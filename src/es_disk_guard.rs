use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::{info, warn};

use chrono::{NaiveDate, NaiveTime};

use crate::config::Config;
use crate::es_http::EsHttp;
use crate::es_window::EsWindowClient;
use crate::naming;
use crate::prune_state::PruneState;

#[derive(Debug, Clone)]
pub struct EsDiskGuardConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub free_buffer_gb: u64,
    pub min_keep_days: u64,
}

pub fn start_es_disk_guard(
    cfg: Config,
    es_url: Arc<str>,
    es_user: Arc<str>,
    es_pass: Arc<str>,
    timeout: Duration,
    guard_cfg: EsDiskGuardConfig,
    prune_state: Arc<PruneState>,
) {
    if !guard_cfg.enabled {
        return;
    }

    tokio::spawn(async move {
        let http = EsHttp::new(
            es_url.clone(),
            es_user.clone(),
            es_pass.clone(),
            timeout,
            true,
        );
        let Ok(http) = http else {
            warn!("disk_guard: failed to create clients");
            return;
        };
        let window = EsWindowClient::from_http(http.clone());

        let mut interval = tokio::time::interval(Duration::from_secs(guard_cfg.interval_secs));
        loop {
            interval.tick().await;
            let ctx = EsDiskGuardCtx {
                http: &http,
                window: &window,
                cfg: &cfg,
                prune_state: &prune_state,
            };
            if let Err(err) = guard_once(ctx, &guard_cfg).await {
                warn!("disk_guard: guard failed: {err:?}");
            }
        }
    });
}

struct EsDiskGuardCtx<'a> {
    http: &'a EsHttp,
    window: &'a EsWindowClient,
    cfg: &'a Config,
    prune_state: &'a PruneState,
}

async fn guard_once(ctx: EsDiskGuardCtx<'_>, guard_cfg: &EsDiskGuardConfig) -> Result<()> {
    let fs = fetch_fs_stats(ctx.http).await?;
    let buffer_bytes = guard_cfg.free_buffer_gb.saturating_mul(1024 * 1024 * 1024);
    let Some((available_min_bytes, needed_max_bytes, high_watermark_max_bytes)) =
        compute_disk_pressure(&fs, buffer_bytes)
    else {
        return Ok(());
    };

    if available_min_bytes >= needed_max_bytes {
        return Ok(());
    }

    info!(
        "disk_guard: low disk space available_min={} needed_max={} (high_watermark_free_max={} buffer_gb={})",
        available_min_bytes, needed_max_bytes, high_watermark_max_bytes, guard_cfg.free_buffer_gb
    );

    let now_ms = chrono::Utc::now().timestamp_millis();
    let keep_cutoff_ms = now_ms.saturating_sub((guard_cfg.min_keep_days as i64) * 86_400_000);

    let mut candidates = build_delete_candidates(ctx.window, ctx.cfg, keep_cutoff_ms).await?;
    if candidates.is_empty() {
        warn!("disk_guard: no eligible backing indices to delete");
        return Ok(());
    }

    // Delete oldest first until disk is back under the watermark requirement.
    for cand in candidates.drain(..) {
        let fs_now = fetch_fs_stats(ctx.http).await?;
        let Some((avail_now, needed_now, _high_now)) = compute_disk_pressure(&fs_now, buffer_bytes)
        else {
            break;
        };
        if avail_now >= needed_now {
            break;
        }

        info!(
            "disk_guard: deleting backing index={} sort_key_ms={} (avail_now={} needed_now={})",
            cand.index, cand.sort_key_ms, avail_now, needed_now
        );
        let _ = delete_index_if_exists(ctx.http, &cand.index).await?;
        let _ = ctx
            .prune_state
            .update_monotonic(cand.stable_alias.as_ref(), cand.watermark_ms, now_ms)
            .await?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct DeleteCandidate {
    index: String,
    sort_key_ms: i64,
    watermark_ms: i64,
    stable_alias: Arc<str>,
}

async fn build_delete_candidates(
    window: &EsWindowClient,
    cfg: &Config,
    keep_cutoff_ms: i64,
) -> Result<Vec<DeleteCandidate>> {
    let mut out = Vec::new();

    for group in cfg.effective_log_groups() {
        let stable = naming::stable_alias(&cfg.index_prefix, &group);
        let [v1, v2] = naming::versioned_streams(&stable);

        for stream in [&v1, &v2] {
            let mut indices = match window.data_stream_backing_indices(stream.as_ref()).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            if indices.len() <= 1 {
                continue;
            }
            let Some(write_index) = indices.pop() else {
                continue;
            };
            let max_by_index = window
                .backing_index_max_timestamp_ms(stream.as_ref())
                .await
                .unwrap_or_default();

            for idx in indices {
                let max_ts_ms = max_by_index.get(&idx).copied().filter(|v| *v > 0);
                let date_start_ms = parse_backing_index_date_start_ms(&idx);

                // Only delete if we can prove this backing index is older than the keep cutoff:
                // - preferred: max(@timestamp) for the index
                // - fallback: date encoded in backing index name (.ds-...-YYYY.MM.DD-000001)
                let eligible = match max_ts_ms {
                    Some(ts) => ts < keep_cutoff_ms,
                    None => date_start_ms.is_some_and(|d| d < keep_cutoff_ms),
                };
                if !eligible {
                    continue;
                }

                let sort_key_ms = max_ts_ms.unwrap_or_else(|| date_start_ms.unwrap_or(i64::MAX));
                let watermark_ms = max_ts_ms.unwrap_or_else(|| {
                    date_start_ms
                        .unwrap_or(keep_cutoff_ms)
                        .saturating_add(86_400_000)
                        .saturating_sub(1)
                });
                out.push(DeleteCandidate {
                    index: idx,
                    sort_key_ms,
                    watermark_ms,
                    stable_alias: stable.clone(),
                });
            }

            let _ = write_index;
        }
    }

    out.sort_by_key(|c| c.sort_key_ms);
    Ok(out)
}

fn compute_disk_pressure(fs: &NodesFsStats, buffer_bytes: u64) -> Option<(u64, u64, u64)> {
    let mut available_min = None::<u64>;
    let mut needed_max = None::<u64>;
    let mut high_max = None::<u64>;

    for data_path in fs.nodes.values().flat_map(|n| n.fs.data.iter()) {
        let avail = data_path.available_in_bytes;
        let high = data_path.high_watermark_free_space_in_bytes;
        let needed = high.saturating_add(buffer_bytes);

        available_min = Some(available_min.map_or(avail, |m| m.min(avail)));
        needed_max = Some(needed_max.map_or(needed, |m| m.max(needed)));
        high_max = Some(high_max.map_or(high, |m| m.max(high)));
    }

    match (available_min, needed_max, high_max) {
        (Some(a), Some(n), Some(h)) => Some((a, n, h)),
        _ => None,
    }
}

fn parse_backing_index_date_start_ms(index: &str) -> Option<i64> {
    // Backing indices look like:
    //   .ds-<stream>-YYYY.MM.DD-000001
    // We'll parse the YYYY.MM.DD piece from the right.
    let mut parts = index.rsplitn(3, '-');
    let _seq = parts.next()?;
    let date_str = parts.next()?;
    let _prefix = parts.next()?;

    let date = NaiveDate::parse_from_str(date_str, "%Y.%m.%d").ok()?;
    let start = date.and_time(NaiveTime::from_hms_opt(0, 0, 0)?).and_utc();
    Some(start.timestamp_millis())
}

async fn delete_index_if_exists(http: &EsHttp, index: &str) -> Result<bool> {
    let resp = http
        .request(reqwest::Method::DELETE, index)
        .send()
        .await
        .context("delete index")?;

    if resp.status().as_u16() == 404 {
        return Ok(false);
    }
    if !resp.status().is_success() {
        anyhow::bail!("delete index {} status {}", index, resp.status());
    }
    Ok(true)
}

async fn fetch_fs_stats(http: &EsHttp) -> Result<NodesFsStats> {
    http.get_json("_nodes/stats/fs", "nodes fs stats request")
        .await
        .context("nodes fs stats parse")
}

#[derive(Debug, Deserialize)]
struct NodesFsStats {
    nodes: std::collections::HashMap<String, NodeFsEntry>,
}

#[derive(Debug, Deserialize)]
struct NodeFsEntry {
    fs: NodeFs,
}

#[derive(Debug, Deserialize)]
struct NodeFs {
    data: Vec<NodeFsDataPath>,
}

#[derive(Debug, Deserialize)]
struct NodeFsDataPath {
    available_in_bytes: u64,
    high_watermark_free_space_in_bytes: u64,
}
