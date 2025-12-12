//! ES cluster health monitoring and recovery.
//! Checks cluster state directly via ES APIs and fixes any issues.
//! No error message parsing - just health checks and recovery actions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use chrono::NaiveDate;
use reqwest::Client;
use serde::Deserialize;
use tracing::{info, warn};

// Thresholds for health checks (public for testing)
pub const SHARD_LIMIT_THRESHOLD: usize = 900;
pub const DISK_WATERMARK_PERCENT: f64 = 85.0;
pub const HEAP_PRESSURE_PERCENT: f64 = 85.0;
pub const PENDING_TASKS_THRESHOLD: usize = 100;

// Backoff configuration
const MIN_BACKOFF_SECS: u64 = 3;
const MAX_BACKOFF_SECS: u64 = 60;
const BACKOFF_MULTIPLIER: u64 = 2;

/// Cluster stress level for adaptive behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressLevel {
    /// Cluster is healthy - full speed
    Normal,
    /// Cluster is under moderate stress - reduce load
    Elevated,
    /// Cluster is under severe stress - minimal load, pause backfill
    Critical,
}

/// Tracks ES cluster stress for adaptive backoff.
/// Uses exponential backoff when stress is detected.
pub struct ClusterStressTracker {
    /// Previous circuit breaker trip count (to detect new trips)
    last_trip_count: AtomicU64,
    /// Current backoff in seconds (exponential)
    current_backoff_secs: AtomicU64,
    /// Consecutive stress detections
    stress_streak: AtomicU64,
    /// Current stress level
    stress_level: std::sync::atomic::AtomicU8,
}

impl Default for ClusterStressTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterStressTracker {
    pub fn new() -> Self {
        Self {
            last_trip_count: AtomicU64::new(0),
            current_backoff_secs: AtomicU64::new(MIN_BACKOFF_SECS),
            stress_streak: AtomicU64::new(0),
            stress_level: std::sync::atomic::AtomicU8::new(0),
        }
    }

    /// Check if new circuit breaker trips occurred.
    pub fn record_trips(&self, total_trips: u64) -> bool {
        let last = self.last_trip_count.swap(total_trips, Ordering::SeqCst);
        total_trips > last
    }

    /// Record stress detection and increase backoff exponentially.
    pub fn record_stress(&self) {
        let streak = self.stress_streak.fetch_add(1, Ordering::SeqCst) + 1;

        // Exponential backoff
        let current = self.current_backoff_secs.load(Ordering::SeqCst);
        let new_backoff = (current * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_SECS);
        self.current_backoff_secs
            .store(new_backoff, Ordering::SeqCst);

        // Update stress level based on streak
        let level = if streak >= 10 {
            StressLevel::Critical
        } else if streak >= 3 {
            StressLevel::Elevated
        } else {
            StressLevel::Normal
        };
        self.stress_level.store(level as u8, Ordering::SeqCst);
    }

    /// Record successful operation - reduce backoff.
    pub fn record_success(&self) {
        // Decay backoff on success
        let current = self.current_backoff_secs.load(Ordering::SeqCst);
        let new_backoff = (current / 2).max(MIN_BACKOFF_SECS);
        self.current_backoff_secs
            .store(new_backoff, Ordering::SeqCst);

        // Reduce streak
        let streak = self.stress_streak.load(Ordering::SeqCst);
        if streak > 0 {
            self.stress_streak.store(streak - 1, Ordering::SeqCst);
        }

        // Update stress level based on new streak
        let new_streak = if streak > 0 { streak - 1 } else { 0 };
        let level = if new_streak >= 10 {
            StressLevel::Critical
        } else if new_streak >= 3 {
            StressLevel::Elevated
        } else {
            StressLevel::Normal
        };
        self.stress_level.store(level as u8, Ordering::SeqCst);
    }

    /// Get current backoff duration.
    pub fn backoff_duration(&self) -> Duration {
        Duration::from_secs(self.current_backoff_secs.load(Ordering::SeqCst))
    }

    /// Check if work at the given priority should pause.
    ///
    /// Priority-aware pausing (like Linux nice values):
    /// - CRITICAL (255): Never pauses - real-time logs must flow
    /// - HIGH (192): Only pauses under Critical stress
    /// - NORMAL (128): Pauses under Elevated or Critical stress
    /// - LOW (64): Pauses under any stress (streak >= 1)
    /// - IDLE (0): Pauses most aggressively (any stress)
    ///
    /// Returns Some(duration) if work should pause, None otherwise.
    pub fn should_pause_for_priority(&self, priority: u8) -> Option<Duration> {
        let streak = self.stress_streak.load(Ordering::SeqCst);
        let stress = self.stress_level();

        // CRITICAL priority (255) - never pause real-time logs
        if priority >= 250 {
            return None;
        }

        // HIGH priority (192+) - only pause under critical stress
        if priority >= 180 {
            if stress == StressLevel::Critical {
                return Some(self.backoff_duration());
            }
            return None;
        }

        // NORMAL priority (128+) - pause under elevated or critical
        if priority >= 100 {
            if stress == StressLevel::Elevated || stress == StressLevel::Critical {
                return Some(self.backoff_duration());
            }
            return None;
        }

        // LOW priority (64+) - pause if any stress detected
        if priority >= 50 {
            if streak >= 1 {
                return Some(self.backoff_duration());
            }
            return None;
        }

        // IDLE priority (0-49) - pause most aggressively
        // Any stress at all causes a pause
        if streak >= 1 {
            // Longer pause for IDLE - multiply backoff by 2
            let backoff = self.backoff_duration();
            return Some(backoff * 2);
        }

        None
    }

    /// Get current stress level.
    pub fn stress_level(&self) -> StressLevel {
        match self.stress_level.load(Ordering::SeqCst) {
            2 => StressLevel::Critical,
            1 => StressLevel::Elevated,
            _ => StressLevel::Normal,
        }
    }

    /// Get current stress streak count.
    pub fn stress_streak(&self) -> u64 {
        self.stress_streak.load(Ordering::SeqCst)
    }
}

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

/// Check cluster health with stress tracking and exponential backoff.
/// This is the preferred function - uses intelligent backoff based on
/// actual stress indicators rather than cumulative counters.
pub async fn check_and_recover_tracked(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    _index_prefix: &str,
    tracker: &ClusterStressTracker,
) -> bool {
    let mut stress_detected = false;

    // Check node stats for stress indicators
    if let Ok(stats) = get_node_stats(client, base_url, user, pass).await {
        for (_node_id, node) in stats.nodes.iter() {
            // Check circuit breaker trips - detect NEW trips only
            if let Some(breakers) = &node.breakers {
                let total_trips: u64 = breakers.values().map(|b| b.tripped).sum();
                if tracker.record_trips(total_trips) {
                    // New trips occurred - this is real stress
                    tracker.record_stress();
                    stress_detected = true;
                    let backoff = tracker.backoff_duration();
                    warn!(
                        "recovery: NEW circuit breaker trips detected, backoff {:?} (streak={})",
                        backoff,
                        tracker.stress_streak()
                    );
                    tokio::time::sleep(backoff).await;
                }
            }

            // Check JVM heap - memory pressure
            if let Some(jvm) = &node.jvm {
                if jvm.mem.heap_used_percent > HEAP_PRESSURE_PERCENT as u64 {
                    tracker.record_stress();
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

            // Check write queue buildup
            if let Some(tp) = &node.thread_pool {
                let write_queue = tp.write.as_ref().map(|w| w.queue).unwrap_or(0)
                    + tp.bulk.as_ref().map(|b| b.queue).unwrap_or(0);
                if write_queue > 200 {
                    tracker.record_stress();
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

    // Check cluster health for RED status
    if let Ok(health) = get_cluster_health(client, base_url, user, pass).await {
        if health.status == "red" {
            tracker.record_stress();
            stress_detected = true;
            let backoff = tracker.backoff_duration();
            warn!("recovery: cluster RED, backoff {:?}", backoff);
            tokio::time::sleep(backoff).await;
        }

        // Pending tasks indicate overload
        if health.number_of_pending_tasks > PENDING_TASKS_THRESHOLD {
            tracker.record_stress();
            stress_detected = true;
            let backoff = tracker.backoff_duration();
            warn!(
                "recovery: {} pending tasks, backoff {:?}",
                health.number_of_pending_tasks, backoff
            );
            tokio::time::sleep(backoff).await;
        }
    }

    // Clear read-only blocks
    let _ = clear_readonly_blocks(client, base_url, user, pass).await;

    // If no stress detected, record success to decay backoff
    if !stress_detected {
        tracker.record_success();
    } else if tracker.stress_level() == StressLevel::Critical {
        warn!(
            "recovery: CRITICAL stress - low priority work paused (streak={})",
            tracker.stress_streak()
        );
    }

    stress_detected
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
    let tracker = ClusterStressTracker::new();
    let _ = check_and_recover_tracked(&client, base_url, user, pass, index_prefix, &tracker).await;
    Ok(())
}
