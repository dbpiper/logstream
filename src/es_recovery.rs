//! ES cluster health monitoring and recovery.
//! Checks cluster state directly via ES APIs and fixes any issues.
//! No error message parsing - just health checks and recovery actions.

use std::time::Duration;

use anyhow::Result;
use chrono::NaiveDate;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

// Thresholds for health checks
const SHARD_LIMIT_THRESHOLD: usize = 900;
const DISK_WATERMARK_PERCENT: f64 = 85.0;
const HEAP_PRESSURE_PERCENT: f64 = 85.0;
const PENDING_TASKS_THRESHOLD: usize = 100;

#[derive(Debug, Deserialize)]
struct IndexInfo {
    index: String,
}

#[derive(Debug, Deserialize)]
struct ClusterHealth {
    status: String,
    active_shards: usize,
    unassigned_primary_shards: usize,
    number_of_pending_tasks: usize,
}

#[derive(Debug, Deserialize)]
struct NodeStats {
    nodes: std::collections::HashMap<String, NodeInfo>,
}

#[derive(Debug, Deserialize)]
struct NodeInfo {
    fs: Option<FsInfo>,
    jvm: Option<JvmInfo>,
    thread_pool: Option<ThreadPoolInfo>,
    breakers: Option<std::collections::HashMap<String, BreakerInfo>>,
}

#[derive(Debug, Deserialize)]
struct FsInfo {
    total: FsTotal,
}

#[derive(Debug, Deserialize)]
struct FsTotal {
    total_in_bytes: u64,
    available_in_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct JvmInfo {
    mem: JvmMem,
}

#[derive(Debug, Deserialize)]
struct JvmMem {
    heap_used_percent: u64,
}

#[derive(Debug, Deserialize)]
struct ThreadPoolInfo {
    write: Option<ThreadPoolStats>,
    bulk: Option<ThreadPoolStats>,
}

#[derive(Debug, Deserialize)]
struct ThreadPoolStats {
    queue: u64,
}

#[derive(Debug, Deserialize)]
struct BreakerInfo {
    tripped: u64,
}

/// Check cluster health and attempt recovery for any issues found.
/// Returns true if any recovery action was taken.
pub async fn check_and_recover(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> bool {
    let mut recovered = false;

    // Check 1: Cluster health (status, shards, pending tasks)
    if let Ok(health) = get_cluster_health(client, base_url, user, pass).await {
        // RED status - cluster is in trouble
        if health.status == "red" {
            warn!("recovery: cluster status RED, waiting 5s");
            tokio::time::sleep(Duration::from_secs(5)).await;
            recovered = true;
        }

        // Too many active shards - approaching limit
        if health.active_shards >= SHARD_LIMIT_THRESHOLD {
            warn!(
                "recovery: {} shards >= threshold {}, cleaning",
                health.active_shards, SHARD_LIMIT_THRESHOLD
            );
            if let Ok(d) =
                delete_oldest_indices(client, base_url, user, pass, index_prefix, 20).await
            {
                if d > 0 {
                    recovered = true;
                }
            }
        }

        // Unassigned PRIMARY shards - data loss risk (ignore replica shards in single-node)
        if health.unassigned_primary_shards > 0 {
            warn!(
                "recovery: {} unassigned PRIMARY shards, triggering reroute",
                health.unassigned_primary_shards
            );
            let _ = trigger_reroute(client, base_url, user, pass).await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            recovered = true;
        }

        // Too many pending tasks - cluster overloaded
        if health.number_of_pending_tasks > PENDING_TASKS_THRESHOLD {
            warn!(
                "recovery: {} pending tasks, waiting 3s",
                health.number_of_pending_tasks
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
            recovered = true;
        }
    }

    // Check 2: Node-level stats (disk, heap, thread pools, breakers)
    if let Ok(stats) = get_node_stats(client, base_url, user, pass).await {
        for (_node_id, node) in stats.nodes.iter() {
            // Disk space
            if let Some(fs) = &node.fs {
                let used_pct = 100.0
                    * (1.0 - fs.total.available_in_bytes as f64 / fs.total.total_in_bytes as f64);
                if used_pct > DISK_WATERMARK_PERCENT {
                    warn!(
                        "recovery: disk usage {:.1}% > {:.1}%, cleaning",
                        used_pct, DISK_WATERMARK_PERCENT
                    );
                    if let Ok(d) =
                        delete_oldest_indices(client, base_url, user, pass, index_prefix, 30).await
                    {
                        if d > 0 {
                            recovered = true;
                        }
                    }
                    break; // Only need to clean once
                }
            }

            // JVM heap pressure
            if let Some(jvm) = &node.jvm {
                if jvm.mem.heap_used_percent > HEAP_PRESSURE_PERCENT as u64 {
                    warn!(
                        "recovery: JVM heap {}% > {}%, waiting 3s",
                        jvm.mem.heap_used_percent, HEAP_PRESSURE_PERCENT
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    recovered = true;
                    break;
                }
            }

            // Thread pool rejections/queue buildup
            if let Some(tp) = &node.thread_pool {
                let write_queue = tp.write.as_ref().map(|w| w.queue).unwrap_or(0)
                    + tp.bulk.as_ref().map(|b| b.queue).unwrap_or(0);
                if write_queue > 100 {
                    warn!("recovery: write queue {} items, waiting 2s", write_queue);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    recovered = true;
                    break;
                }
            }

            // Circuit breaker trips
            if let Some(breakers) = &node.breakers {
                let total_trips: u64 = breakers.values().map(|b| b.tripped).sum();
                if total_trips > 0 {
                    warn!(
                        "recovery: circuit breakers tripped {} times, waiting 3s",
                        total_trips
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    recovered = true;
                    break;
                }
            }
        }
    }

    // Check 3: Clear any read-only blocks (happens after disk full)
    if let Ok(true) = clear_readonly_blocks(client, base_url, user, pass).await {
        recovered = true;
    }

    recovered
}

/// Clear read-only blocks from all indices.
async fn clear_readonly_blocks(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
) -> Result<bool> {
    let url = format!("{}/_all/_settings", base_url.trim_end_matches('/'));
    let body = r#"{"index.blocks.read_only_allow_delete": null}"#;

    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;

    if resp.status().is_success() {
        let resp_text = resp.text().await.unwrap_or_default();
        if resp_text.contains("\"acknowledged\":true") {
            info!("recovery: cleared read-only blocks");
            return Ok(true);
        }
    }

    Ok(false)
}

/// Trigger cluster reroute to allocate unassigned shards.
async fn trigger_reroute(client: &Client, base_url: &str, user: &str, pass: &str) -> Result<()> {
    let url = format!(
        "{}/_cluster/reroute?retry_failed=true",
        base_url.trim_end_matches('/')
    );

    let resp = client
        .post(&url)
        .basic_auth(user, Some(pass))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await?;

    if resp.status().is_success() {
        info!("recovery: triggered cluster reroute");
    }

    Ok(())
}

/// Delete N oldest indices to free resources.
async fn delete_oldest_indices(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
    count: usize,
) -> Result<usize> {
    let indices = list_dated_indices(client, base_url, user, pass, index_prefix).await?;

    if indices.is_empty() {
        return Ok(0);
    }

    let mut deleted = 0;
    for (date, index_name) in indices.iter().take(count) {
        match delete_index(client, base_url, user, pass, index_name).await {
            Ok(()) => {
                warn!("recovery: deleted {} ({})", index_name, date);
                deleted += 1;
            }
            Err(err) => {
                warn!("recovery: failed to delete {}: {err:?}", index_name);
            }
        }
    }

    Ok(deleted)
}

async fn list_dated_indices(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<Vec<(NaiveDate, String)>> {
    let url = format!(
        "{}/_cat/indices/{}*?format=json&h=index",
        base_url.trim_end_matches('/'),
        index_prefix
    );

    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("failed to list indices: {}", resp.status());
    }

    let indices: Vec<IndexInfo> = resp.json().await?;
    let mut dated: Vec<(NaiveDate, String)> = indices
        .into_iter()
        .filter_map(|info| {
            let date = parse_index_date(&info.index, index_prefix)?;
            Some((date, info.index))
        })
        .collect();

    dated.sort_by_key(|(date, _)| *date);
    Ok(dated)
}

async fn get_cluster_health(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
) -> Result<ClusterHealth> {
    let url = format!("{}/_cluster/health", base_url.trim_end_matches('/'));
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("failed to get cluster health: {}", resp.status());
    }

    Ok(resp.json().await?)
}

async fn get_node_stats(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
) -> Result<NodeStats> {
    let url = format!(
        "{}/_nodes/stats/fs,jvm,thread_pool,breaker",
        base_url.trim_end_matches('/')
    );
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("failed to get node stats: {}", resp.status());
    }

    Ok(resp.json().await?)
}

async fn delete_index(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index: &str,
) -> Result<()> {
    let url = format!("{}/{}", base_url.trim_end_matches('/'), index);
    let resp = client
        .delete(&url)
        .basic_auth(user, Some(pass))
        .send()
        .await?;

    if resp.status().is_success() || resp.status().as_u16() == 404 {
        Ok(())
    } else {
        anyhow::bail!("delete failed with status {}", resp.status())
    }
}

fn parse_index_date(index_name: &str, prefix: &str) -> Option<NaiveDate> {
    let suffix = index_name.strip_prefix(prefix)?.strip_prefix('-')?;
    NaiveDate::parse_from_str(suffix, "%Y.%m.%d").ok()
}

/// Proactive startup check.
pub async fn check_on_startup(
    base_url: &str,
    user: &str,
    pass: &str,
    timeout: Duration,
    index_prefix: &str,
) -> Result<()> {
    let client = Client::builder().timeout(timeout).build()?;
    let _ = check_and_recover(&client, base_url, user, pass, index_prefix).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn test_parse_index_date_valid() {
        let date = parse_index_date("logs-2025.12.11", "logs").unwrap();
        assert_eq!(date.year(), 2025);
        assert_eq!(date.month(), 12);
        assert_eq!(date.day(), 11);
    }

    #[test]
    fn test_parse_index_date_different_prefix() {
        let date = parse_index_date("myapp-2024.01.15", "myapp").unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_parse_index_date_wrong_prefix() {
        let result = parse_index_date("logs-2025.12.11", "other");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_date_no_dash() {
        let result = parse_index_date("logs2025.12.11", "logs");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_date_invalid_date() {
        let result = parse_index_date("logs-2025.13.45", "logs");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_date_wrong_format() {
        let result = parse_index_date("logs-2025-12-11", "logs");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_date_empty() {
        let result = parse_index_date("", "logs");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_index_date_just_prefix() {
        let result = parse_index_date("logs-", "logs");
        assert!(result.is_none());
    }

    #[test]
    fn test_thresholds_are_reasonable() {
        // Test threshold values at runtime
        let shard_limit = SHARD_LIMIT_THRESHOLD;
        let disk_pct = DISK_WATERMARK_PERCENT;
        let heap_pct = HEAP_PRESSURE_PERCENT;
        let pending = PENDING_TASKS_THRESHOLD;

        assert!(shard_limit < 1000 && shard_limit > 500);
        assert!(disk_pct > 50.0 && disk_pct < 100.0);
        assert!(heap_pct > 50.0 && heap_pct < 100.0);
        assert!(pending > 10);
    }
}
