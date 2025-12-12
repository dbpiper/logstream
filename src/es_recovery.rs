use std::time::Duration;

use anyhow::Result;
use chrono::NaiveDate;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

use crate::stress::{StressConfig, StressTracker};

pub use crate::stress::StressLevel;

pub const SHARD_LIMIT_THRESHOLD: usize = 900;
pub const DISK_WATERMARK_PERCENT: f64 = 85.0;
pub const HEAP_PRESSURE_PERCENT: f64 = 85.0;
pub const PENDING_TASKS_THRESHOLD: usize = 100;

#[derive(Debug, Deserialize)]
struct ClusterHealth {
    status: String,
    number_of_pending_tasks: usize,
}

#[derive(Debug, Deserialize)]
struct NodeStats {
    nodes: std::collections::HashMap<String, NodeInfo>,
}

#[derive(Debug, Deserialize)]
struct NodeInfo {
    jvm: Option<JvmInfo>,
    thread_pool: Option<ThreadPoolInfo>,
    breakers: Option<std::collections::HashMap<String, BreakerInfo>>,
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

pub async fn check_and_recover_tracked(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    _index_prefix: &str,
    tracker: &StressTracker,
) -> bool {
    let mut stress_detected = false;

    if let Ok(stats) = get_node_stats(client, base_url, user, pass).await {
        for (_node_id, node) in stats.nodes.iter() {
            if let Some(breakers) = &node.breakers {
                let total_trips: u64 = breakers.values().map(|b| b.tripped).sum();
                if tracker.check_value_increased(total_trips) {
                    tracker.record_failure();
                    stress_detected = true;
                    let backoff = tracker.backoff_duration();
                    warn!(
                        "recovery: NEW circuit breaker trips detected, backoff {:?} (streak={})",
                        backoff,
                        tracker.failure_streak()
                    );
                    tokio::time::sleep(backoff).await;
                }
            }

            if let Some(jvm) = &node.jvm {
                if jvm.mem.heap_used_percent > HEAP_PRESSURE_PERCENT as u64 {
                    tracker.record_failure();
                    stress_detected = true;
                    let backoff = tracker.backoff_duration();
                    warn!(
                        "recovery: JVM heap {}% > {}%, backoff {:?}",
                        jvm.mem.heap_used_percent, HEAP_PRESSURE_PERCENT, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    break;
                }
            }

            if let Some(tp) = &node.thread_pool {
                let write_queue = tp.write.as_ref().map(|w| w.queue).unwrap_or(0)
                    + tp.bulk.as_ref().map(|b| b.queue).unwrap_or(0);
                if write_queue > 200 {
                    tracker.record_failure();
                    stress_detected = true;
                    let backoff = tracker.backoff_duration();
                    warn!(
                        "recovery: write queue {} items, backoff {:?}",
                        write_queue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    break;
                }
            }
        }
    }

    if let Ok(health) = get_cluster_health(client, base_url, user, pass).await {
        if health.status == "red" {
            tracker.record_failure();
            stress_detected = true;
            let backoff = tracker.backoff_duration();
            warn!("recovery: cluster RED, backoff {:?}", backoff);
            tokio::time::sleep(backoff).await;
        }

        if health.number_of_pending_tasks > PENDING_TASKS_THRESHOLD {
            tracker.record_failure();
            stress_detected = true;
            let backoff = tracker.backoff_duration();
            warn!(
                "recovery: {} pending tasks, backoff {:?}",
                health.number_of_pending_tasks, backoff
            );
            tokio::time::sleep(backoff).await;
        }
    }

    let _ = clear_readonly_blocks(client, base_url, user, pass).await;

    if !stress_detected {
        tracker.record_success();
    } else if tracker.stress_level() == StressLevel::Critical {
        warn!(
            "recovery: CRITICAL stress - low priority work paused (streak={})",
            tracker.failure_streak()
        );
    }

    stress_detected
}

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

pub fn parse_index_date(index_name: &str, prefix: &str) -> Option<NaiveDate> {
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
    let tracker = StressTracker::with_config(StressConfig::ES);
    let _ = check_and_recover_tracked(&client, base_url, user, pass, index_prefix, &tracker).await;
    Ok(())
}
