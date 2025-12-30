use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::Config;
use crate::es_disk_guard_logic::compute_delete_candidates_with_store_bytes;
use crate::es_disk_guard_logic::DeleteCandidate;
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
            let now_ms = chrono::Utc::now().timestamp_millis();
            if let Err(err) =
                run_disk_guard_once(&http, &window, &cfg, &prune_state, &guard_cfg, now_ms).await
            {
                warn!("disk_guard: guard failed: {err:?}");
            }
        }
    });
}

pub async fn run_disk_guard_once(
    http: &EsHttp,
    window: &EsWindowClient,
    cfg: &Config,
    prune_state: &PruneState,
    guard_cfg: &EsDiskGuardConfig,
    now_ms: i64,
) -> Result<()> {
    let fs = fetch_fs_stats(http).await?;
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

    let keep_cutoff_ms = now_ms.saturating_sub((guard_cfg.min_keep_days as i64) * 86_400_000);

    let mut candidates = build_delete_candidates(window, http, cfg, keep_cutoff_ms).await?;
    if candidates.is_empty() {
        warn!("disk_guard: no eligible backing indices to delete");
        return Ok(());
    }

    // Delete oldest first until disk is back under the watermark requirement.
    for cand in candidates.drain(..) {
        let fs_now = fetch_fs_stats(http).await?;
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
        let _ = delete_index_if_exists(http, &cand.index).await?;
        let _ = prune_state
            .update_monotonic(cand.stable_alias.as_ref(), cand.watermark_ms, now_ms)
            .await?;
    }

    Ok(())
}

async fn build_delete_candidates(
    window: &EsWindowClient,
    http: &EsHttp,
    cfg: &Config,
    keep_cutoff_ms: i64,
) -> Result<Vec<DeleteCandidate>> {
    const MAX_INDEX_BOUNDS_LOOKUPS_PER_STREAM: usize = 256;
    let mut out = Vec::new();

    for group in cfg.effective_log_groups() {
        let stable = naming::stable_alias(&cfg.index_prefix, &group);
        let [v1, v2] = naming::versioned_streams(&stable);

        for stream in [&v1, &v2] {
            let indices = match window.data_stream_backing_indices(stream.as_ref()).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            if indices.len() <= 1 {
                continue;
            }

            let read_indices = indices
                .iter()
                .take(indices.len().saturating_sub(1))
                .take(MAX_INDEX_BOUNDS_LOOKUPS_PER_STREAM)
                .cloned()
                .collect::<Vec<String>>();

            let max_ts_map = fetch_max_ts_map(window, &read_indices).await;
            let store_bytes_map = fetch_store_bytes_map(http, &read_indices).await;

            let normal_candidates = compute_delete_candidates_with_store_bytes(
                stable.clone(),
                &indices,
                keep_cutoff_ms,
                |index| max_ts_map.get(index).copied(),
                |index| store_bytes_map.get(index).copied(),
                false,
            );
            if !normal_candidates.is_empty() {
                out.extend(normal_candidates);
                continue;
            }

            let emergency_candidates = compute_delete_candidates_with_store_bytes(
                stable.clone(),
                &indices,
                keep_cutoff_ms,
                |index| max_ts_map.get(index).copied(),
                |index| store_bytes_map.get(index).copied(),
                true,
            );
            out.extend(emergency_candidates);
        }
    }

    out.sort_by(compare_candidates);
    Ok(out)
}

async fn fetch_max_ts_map(
    window: &EsWindowClient,
    indices: &[String],
) -> std::collections::BTreeMap<String, i64> {
    use std::collections::BTreeMap;

    let mut out = BTreeMap::<String, i64>::new();
    for index in indices {
        let max_ts = window
            .index_time_bounds_ms(index)
            .await
            .ok()
            .flatten()
            .map(|b| b.max_ms)
            .filter(|v| *v > 0);
        if let Some(ts) = max_ts {
            out.insert(index.clone(), ts);
        }
    }
    out
}

fn compare_candidates(a: &DeleteCandidate, b: &DeleteCandidate) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let by_age = a.sort_key_ms.cmp(&b.sort_key_ms);
    if by_age != Ordering::Equal {
        return by_age;
    }
    match (a.store_bytes, b.store_bytes) {
        (Some(sa), Some(sb)) => sb.cmp(&sa),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

async fn fetch_store_bytes_map(
    http: &EsHttp,
    indices: &[String],
) -> std::collections::BTreeMap<String, u64> {
    use std::collections::BTreeMap;

    #[derive(serde::Deserialize)]
    struct CatIndexRow {
        #[serde(default)]
        index: String,
        #[serde(rename = "pri.store.size", default)]
        pri_store_size: Option<String>,
    }

    let Some(path) = build_cat_indices_path(indices) else {
        return BTreeMap::new();
    };
    let rows = http
        .get_json::<Vec<CatIndexRow>>(
            &format!("{path}?format=json&bytes=b&h=index,pri.store.size"),
            "cat indices sizes",
        )
        .await;
    let Ok(rows) = rows else {
        return BTreeMap::new();
    };
    rows.into_iter()
        .filter_map(|row| {
            let bytes = row.pri_store_size?.parse::<u64>().ok()?;
            (!row.index.is_empty()).then_some((row.index, bytes))
        })
        .collect()
}

fn build_cat_indices_path(indices: &[String]) -> Option<String> {
    let joined = indices
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(",");
    (!joined.is_empty()).then_some(format!("_cat/indices/{joined}"))
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
