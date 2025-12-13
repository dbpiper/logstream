use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::adaptive::AdaptiveController;
use crate::enrich::flatten_all_objects;
use crate::stress::{StressConfig, StressLevel, StressTracker};
use crate::types::EnrichedEvent;

#[derive(Clone, Debug)]
pub struct EsBulkConfig {
    pub url: Arc<str>,
    pub user: Arc<str>,
    pub pass: Arc<str>,
    pub batch_size: usize,
    pub max_batch_size: usize,
    pub timeout: Duration,
    pub gzip: bool,
    pub index_prefix: Arc<str>,
}

pub struct EsBulkSink {
    cfg: Arc<EsBulkConfig>,
    client: Client,
    stress_tracker: Arc<StressTracker>,
}

impl EsBulkSink {
    pub fn new(cfg: EsBulkConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(cfg.timeout)
            .pool_max_idle_per_host(64)
            .gzip(cfg.gzip)
            .build()?;
        Ok(Self {
            cfg: Arc::new(cfg),
            client,
            stress_tracker: Arc::new(StressTracker::with_config(StressConfig::ES)),
        })
    }

    pub fn stress_tracker(&self) -> Arc<StressTracker> {
        self.stress_tracker.clone()
    }

    /// Spawn a background task that monitors ES heap and CPU usage.
    pub fn start_heap_monitor(&self, adaptive: Arc<AdaptiveController>) {
        let client = self.client.clone();
        let url = self.cfg.url.clone();
        let user = self.cfg.user.clone();
        let pass = self.cfg.pass.clone();

        tokio::spawn(async move {
            let check_interval = Duration::from_secs(10);

            loop {
                tokio::time::sleep(check_interval).await;

                match check_es_resources(&client, &url, &user, &pass).await {
                    Ok(stats) => {
                        adaptive
                            .set_es_pressure(stats.heap_percent, stats.cpu_percent)
                            .await;
                    }
                    Err(err) => {
                        // If we can't check resources, assume pressure to be safe
                        warn!("es resource monitor: failed to check: {err:?}");
                        adaptive.set_es_pressure(0.90, 0.90).await;
                    }
                }
            }
        });
    }

    pub fn start_adaptive(
        &self,
        mut event_router: crate::event_router::EventRouter,
        adaptive: Arc<AdaptiveController>,
    ) {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        let stress_tracker = self.stress_tracker.clone();

        tokio::spawn(async move {
            let mut buf: Vec<EnrichedEvent> = Vec::with_capacity(cfg.max_batch_size);

            loop {
                // Get current adaptive parameters
                let target_batch = adaptive.batch_size();
                let max_in_flight = adaptive.max_in_flight();
                let delay = adaptive.delay();

                // Apply delay if needed (backpressure)
                if !delay.is_zero() {
                    sleep(delay).await;
                }

                // Additional backoff if cluster is stressed
                let stress_level = stress_tracker.stress_level();
                if stress_level == StressLevel::Critical {
                    // Critical stress - long pause to let ES recover
                    sleep(Duration::from_secs(10)).await;
                } else if stress_level == StressLevel::Elevated {
                    // Elevated stress - shorter pause
                    sleep(Duration::from_secs(2)).await;
                }

                // Receive events up to batch size
                let Some(ev) = event_router.recv().await else {
                    break;
                };
                buf.push(ev);

                // Drain more if available (up to batch size)
                // But drain less if cluster is stressed
                let effective_batch = if stress_level == StressLevel::Critical {
                    target_batch / 4 // 25% batch size when critical
                } else if stress_level == StressLevel::Elevated {
                    target_batch / 2 // 50% batch size when elevated
                } else {
                    target_batch
                };

                while buf.len() < effective_batch {
                    match tokio::time::timeout(Duration::from_millis(10), event_router.recv()).await
                    {
                        Ok(Some(ev)) => buf.push(ev),
                        _ => break,
                    }
                }

                if buf.len() >= effective_batch || buf.len() >= cfg.min_batch_for_send() {
                    let batch = std::mem::take(&mut buf);
                    let started = std::time::Instant::now();

                    let res = send_bulk_adaptive_tracked(
                        &client,
                        &cfg,
                        &batch,
                        adaptive.clone(),
                        max_in_flight,
                        stress_tracker.clone(),
                    )
                    .await;

                    let elapsed = started.elapsed();
                    let latency_ms = elapsed.as_millis() as u64;

                    match res {
                        Ok(_) => {
                            adaptive.record_latency(latency_ms, true).await;
                            info!(
                                "adaptive bulk: batch={} latency={}ms (target_batch={} in_flight={} stress={:?})",
                                batch.len(),
                                latency_ms,
                                target_batch,
                                max_in_flight,
                                stress_level
                            );
                        }
                        Err(err) => {
                            adaptive.record_latency(latency_ms, false).await;
                            warn!("adaptive bulk failed: {err:?}");
                        }
                    }
                }
            }

            // Flush remaining
            if !buf.is_empty() {
                let _ = send_bulk_tracked(
                    &client,
                    &cfg.url,
                    &cfg.user,
                    &cfg.pass,
                    &cfg.index_prefix,
                    &buf,
                    &stress_tracker,
                )
                .await;
            }
        });
    }
}

impl EsBulkConfig {
    fn min_batch_for_send(&self) -> usize {
        self.batch_size.max(100)
    }
}

/// Send bulk with adaptive concurrency control and stress tracking.
async fn send_bulk_adaptive_tracked(
    client: &Client,
    cfg: &EsBulkConfig,
    batch: &[EnrichedEvent],
    adaptive: Arc<AdaptiveController>,
    max_in_flight: usize,
    stress_tracker: Arc<StressTracker>,
) -> Result<()> {
    // Reduce concurrency if cluster is stressed
    let effective_in_flight = match stress_tracker.stress_level() {
        StressLevel::Critical => 1, // Single request when critical
        StressLevel::Elevated => (max_in_flight / 2).max(1),
        StressLevel::Normal => max_in_flight,
    };

    let sem = Arc::new(Semaphore::new(effective_in_flight));
    let chunk_size = (batch.len() / effective_in_flight).max(100);

    let mut handles: FuturesUnordered<tokio::task::JoinHandle<Result<()>>> =
        FuturesUnordered::new();

    for chunk in batch.chunks(chunk_size) {
        let permit = sem.clone().acquire_owned().await?;
        let c = client.clone();
        let url = cfg.url.clone();
        let user = cfg.user.clone();
        let pass = cfg.pass.clone();
        let index_prefix = cfg.index_prefix.clone();
        let chunk_vec: Vec<EnrichedEvent> = chunk.to_vec();
        let adaptive_clone = adaptive.clone();
        let stress_clone = stress_tracker.clone();

        handles.push(tokio::spawn(async move {
            let _p = permit;
            let started = std::time::Instant::now();
            let res = send_bulk_tracked(
                &c,
                &url,
                &user,
                &pass,
                &index_prefix,
                &chunk_vec,
                &stress_clone,
            )
            .await;
            let latency = started.elapsed().as_millis() as u64;

            // Record chunk latency for fine-grained adaptation
            adaptive_clone.record_latency(latency, res.is_ok()).await;

            res
        }));
    }

    // Wait for all chunks
    let mut any_error = None;
    while let Some(result) = handles.next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => any_error = Some(e),
            Err(e) => any_error = Some(anyhow::anyhow!("task failed: {e}")),
        }
    }

    match any_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[derive(Deserialize)]
struct BulkResponse {
    errors: bool,
    items: Vec<BulkItemResponse>,
}

#[derive(Deserialize)]
struct BulkItemResponse {
    index: Option<BulkItemResult>,
}

#[derive(Deserialize)]
struct BulkItemResult {
    #[serde(default)]
    status: u16,
    error: Option<BulkItemError>,
}

/// Error from an individual bulk item.
#[derive(Deserialize, Clone, Debug)]
#[cfg_attr(any(test, feature = "testing"), derive(PartialEq, Eq))]
pub struct BulkItemError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub reason: String,
}

/// Classification of ES bulk item failures.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FailureKind {
    /// Mapping-related error (e.g., type conflict, dynamic mapping).
    Mapping,
    /// Document already exists with different version - safe to skip.
    VersionConflict,
    /// Transient error that can be retried (circuit breaker, timeout, etc).
    Retryable,
    /// Document has too many fields for the index field limit.
    FieldLimitExceeded,
    /// Cluster has hit shard limit - must wait for capacity.
    ShardLimitExceeded,
    /// Unknown or unrecoverable error.
    Other,
}

/// Classify a bulk item error into a failure kind.
pub fn classify_error(error: &BulkItemError) -> FailureKind {
    // Shard limit - cluster is at max capacity, must wait
    if error.error_type == "validation_exception"
        && error.reason.contains("maximum normal shards open")
    {
        return FailureKind::ShardLimitExceeded;
    }
    if error.error_type == "document_parsing_exception"
        && error.reason.contains("Limit of total fields")
    {
        return FailureKind::FieldLimitExceeded;
    }
    let is_mapping = matches!(
        error.error_type.as_str(),
        "mapper_parsing_exception"
            | "illegal_argument_exception"
            | "strict_dynamic_mapping_exception"
    ) || error.reason.contains("mapper")
        || error.reason.contains("dynamic")
        || error.reason.contains("type");
    if is_mapping {
        return FailureKind::Mapping;
    }
    if error.error_type == "version_conflict_engine_exception" {
        return FailureKind::VersionConflict;
    }
    let is_retryable = matches!(
        error.error_type.as_str(),
        "circuit_breaker_exception"
            | "timeout_exception"
            | "es_rejected_execution_exception"
            | "cluster_block_exception"
    );
    if is_retryable {
        return FailureKind::Retryable;
    }
    FailureKind::Other
}

async fn send_bulk_tracked(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
    batch: &[EnrichedEvent],
    stress_tracker: &StressTracker,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let url = format!("{}/_bulk", base_url.trim_end_matches('/'));
    let mut current_batch: Vec<EnrichedEvent> = batch.to_vec();

    for attempt in 1..=10u64 {
        let body = build_bulk_body(&current_batch, index_prefix)?;

        let send_result = client
            .post(&url)
            .basic_auth(user, Some(pass))
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await;

        match send_result {
            Ok(resp) if resp.status().is_success() => {
                let resp_body = resp.text().await.unwrap_or_default();

                if !resp_body.contains("\"errors\":true") {
                    info!("es bulk sent batch={} status=200 OK", current_batch.len());
                    return Ok(());
                }

                let failed_items = parse_failed_items(&resp_body, &current_batch);

                if failed_items.is_empty() {
                    info!("es bulk sent batch={} status=200 OK", current_batch.len());
                    return Ok(());
                }

                let mut mapping_indices: HashSet<usize> = HashSet::new();
                let mut retryable_indices: HashSet<usize> = HashSet::new();
                let mut field_limit_indices: HashSet<usize> = HashSet::new();
                let mut shard_limit_hit = false;
                let mut version_conflict_count = 0usize;
                let mut other_count = 0usize;
                let mut other_failures: Vec<(usize, String, String)> = Vec::new();

                for item in &failed_items {
                    match item.kind {
                        FailureKind::Mapping => {
                            mapping_indices.insert(item.index);
                        }
                        FailureKind::VersionConflict => {
                            version_conflict_count += 1;
                        }
                        FailureKind::Retryable => {
                            retryable_indices.insert(item.index);
                            warn!(
                                "es bulk retryable failure: type={} reason={}",
                                item.error_type,
                                &item.reason[..item.reason.len().min(200)]
                            );
                        }
                        FailureKind::FieldLimitExceeded => {
                            field_limit_indices.insert(item.index);
                        }
                        FailureKind::ShardLimitExceeded => {
                            shard_limit_hit = true;
                        }
                        FailureKind::Other => {
                            other_count += 1;
                            other_failures.push((
                                item.index,
                                item.error_type.clone(),
                                item.reason.clone(),
                            ));
                            warn!(
                                "es bulk unknown failure: type={} reason={}",
                                item.error_type,
                                &item.reason[..item.reason.len().min(200)]
                            );
                        }
                    }
                }

                // Handle shard limit - cluster-wide issue, wait and retry ALL docs
                if shard_limit_hit {
                    stress_tracker.record_failure();
                    let backoff = stress_tracker.backoff_duration();
                    warn!(
                        "es bulk: shard limit exceeded, recording stress and waiting {:?} (attempt {}/10)",
                        backoff, attempt
                    );
                    sleep(backoff).await;
                    continue; // Retry entire batch
                }

                // Record success to decay stress if no shard issues
                stress_tracker.record_success();

                if version_conflict_count > 0 {
                    info!(
                        "es bulk: {} version conflicts (already indexed, skipping)",
                        version_conflict_count
                    );
                }

                if !field_limit_indices.is_empty() {
                    warn!(
                        "es bulk: {} docs exceeded field limit, stringifying parsed fields (attempt {})",
                        field_limit_indices.len(),
                        attempt
                    );
                }

                if !other_failures.is_empty() {
                    warn!(
                        "es bulk: {} docs with unrecoverable errors, ingesting as raw with error info",
                        other_failures.len()
                    );
                    let fallback_docs: Vec<EnrichedEvent> = other_failures
                        .iter()
                        .filter_map(|(idx, err_type, reason)| {
                            current_batch
                                .get(*idx)
                                .map(|ev| create_fallback_event(ev, err_type, reason))
                        })
                        .collect();

                    if !fallback_docs.is_empty() {
                        let fallback_body = build_bulk_body(&fallback_docs, index_prefix).ok();
                        if let Some(body) = fallback_body {
                            let resp = client
                                .post(&url)
                                .basic_auth(user, Some(pass))
                                .header("Content-Type", "application/x-ndjson")
                                .body(body)
                                .send()
                                .await;
                            match resp {
                                Ok(r) if r.status().is_success() => {
                                    info!(
                                        "es bulk: ingested {} fallback docs with raw message",
                                        fallback_docs.len()
                                    );
                                }
                                Ok(r) => {
                                    warn!(
                                        "es bulk: fallback ingestion failed status={}",
                                        r.status()
                                    );
                                }
                                Err(e) => {
                                    warn!("es bulk: fallback ingestion failed: {}", e);
                                }
                            }
                        }
                    }
                }

                let needs_retry = !mapping_indices.is_empty()
                    || !retryable_indices.is_empty()
                    || !field_limit_indices.is_empty();
                if !needs_retry {
                    info!(
                        "es bulk sent batch={} (version_conflicts={} other={})",
                        current_batch.len(),
                        version_conflict_count,
                        other_count
                    );
                    return Ok(());
                }

                if !mapping_indices.is_empty() {
                    warn!(
                        "es bulk mapping failures: {} docs, re-normalizing (attempt {})",
                        mapping_indices.len(),
                        attempt
                    );
                }

                if !retryable_indices.is_empty() {
                    warn!(
                        "es bulk retryable failures: {} docs, will retry (attempt {})",
                        retryable_indices.len(),
                        attempt
                    );
                }

                let retry_indices: HashSet<usize> = mapping_indices
                    .union(&retryable_indices)
                    .copied()
                    .chain(field_limit_indices.iter().copied())
                    .collect();
                current_batch = current_batch
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, mut ev)| {
                        if retry_indices.contains(&i) {
                            if mapping_indices.contains(&i) {
                                if let Some(ref mut parsed) = ev.parsed {
                                    flatten_all_objects(parsed);
                                }
                            }
                            if field_limit_indices.contains(&i) {
                                if let Some(ref mut parsed) = ev.parsed {
                                    reduce_fields_to_limit(parsed);
                                }
                                if !ev.message.is_string() {
                                    ev.message = serde_json::Value::String(ev.message.to_string());
                                }
                            }
                            Some(ev)
                        } else {
                            None
                        }
                    })
                    .collect();

                if current_batch.is_empty() {
                    return Ok(());
                }

                continue;
            }

            Ok(resp) => {
                let status = resp.status();
                let resp_body = resp.text().await.unwrap_or_default();
                warn!(
                    "es bulk status={} attempt={} body_sample={}",
                    status,
                    attempt,
                    &resp_body[..resp_body.len().min(500)]
                );
            }

            Err(err) => {
                warn!("es bulk connection error attempt={}: {err}", attempt);
            }
        }

        let backoff = Duration::from_millis(500 * attempt.min(10));
        sleep(backoff).await;
    }

    if !current_batch.is_empty() {
        warn!(
            "es bulk: {} docs failed after retries, stringifying all fields",
            current_batch.len()
        );
        for ev in &mut current_batch {
            ev.parsed = ev
                .parsed
                .take()
                .map(|v| serde_json::Value::String(v.to_string()));
            if !ev.message.is_string() {
                ev.message = serde_json::Value::String(ev.message.to_string());
            }
        }

        let body = build_bulk_body(&current_batch, index_prefix)?;
        match client
            .post(&url)
            .basic_auth(user, Some(pass))
            .header("Content-Type", "application/x-ndjson")
            .body(body)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!(
                    "es bulk: final fallback ingested {} docs",
                    current_batch.len()
                );
            }
            Ok(resp) => {
                warn!("es bulk: final fallback failed status={}", resp.status());
            }
            Err(e) => {
                warn!("es bulk: final fallback failed: {}", e);
            }
        }
    }

    Ok(())
}

const FIELD_LIMIT_TARGET: usize = 900;

/// Count ES fields correctly: ES counts unique field PATHS, not every nested object.
/// Arrays don't multiply field count - identical keys across array elements = 1 field.
/// e.g. `{"items": [{"x": 1}, {"x": 2}]}` = 1 field (items.x), not 2.
fn count_fields(value: &serde_json::Value) -> usize {
    let mut paths = std::collections::HashSet::new();
    collect_field_paths(value, String::new(), &mut paths);
    paths.len()
}

fn collect_field_paths(
    value: &serde_json::Value,
    prefix: String,
    paths: &mut std::collections::HashSet<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                paths.insert(path.clone());
                collect_field_paths(child, path, paths);
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr {
                collect_field_paths(child, prefix.clone(), paths);
            }
        }
        _ => {}
    }
}

fn max_depth(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Object(map) => 1 + map.values().map(max_depth).max().unwrap_or(0),
        serde_json::Value::Array(arr) => 1 + arr.iter().map(max_depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Count fields if we were to flatten at target_depth.
/// Uses unique path counting like ES does.
fn count_fields_at_depth(value: &serde_json::Value, target_depth: usize, _current: usize) -> usize {
    let mut paths = std::collections::HashSet::new();
    collect_paths_at_depth(value, String::new(), 0, target_depth, &mut paths);
    paths.len()
}

fn collect_paths_at_depth(
    value: &serde_json::Value,
    prefix: String,
    current_depth: usize,
    target_depth: usize,
    paths: &mut std::collections::HashSet<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                paths.insert(path.clone());
                if current_depth + 1 < target_depth {
                    collect_paths_at_depth(child, path, current_depth + 1, target_depth, paths);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for child in arr {
                collect_paths_at_depth(child, prefix.clone(), current_depth, target_depth, paths);
            }
        }
        _ => {}
    }
}

fn flatten_at_depth(value: &mut serde_json::Value, target_depth: usize, current_depth: usize) {
    match value {
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                if current_depth + 1 >= target_depth {
                    match v {
                        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                            *v = serde_json::Value::String(v.to_string());
                        }
                        _ => {}
                    }
                } else {
                    flatten_at_depth(v, target_depth, current_depth + 1);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                if current_depth + 1 >= target_depth {
                    match v {
                        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                            *v = serde_json::Value::String(v.to_string());
                        }
                        _ => {}
                    }
                } else {
                    flatten_at_depth(v, target_depth, current_depth + 1);
                }
            }
        }
        _ => {}
    }
}

fn binary_search_optimal_depth(value: &serde_json::Value) -> usize {
    let depth = max_depth(value);
    if depth <= 1 {
        return 0;
    }

    let mut lo = 1usize;
    let mut hi = depth;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let fields = count_fields_at_depth(value, mid, 0);
        if fields <= FIELD_LIMIT_TARGET {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    lo
}

/// Reduce a JSON value to fit within the ES field limit using binary search.
/// Preserves maximum structure by finding the optimal depth to flatten at.
pub fn reduce_fields_to_limit(value: &mut serde_json::Value) {
    if count_fields(value) <= FIELD_LIMIT_TARGET {
        return;
    }

    let optimal_depth = binary_search_optimal_depth(value);

    if optimal_depth == 0 {
        *value = serde_json::Value::String(value.to_string());
        return;
    }

    flatten_at_depth(value, optimal_depth, 0);

    if count_fields(value) > FIELD_LIMIT_TARGET {
        stringify_largest_children(value);
    }
}

fn stringify_largest_children(value: &mut serde_json::Value) {
    loop {
        let current_count = count_fields(value);
        if current_count <= FIELD_LIMIT_TARGET {
            return;
        }

        let path = find_largest_subtree(value);
        if path.is_empty() {
            *value = serde_json::Value::String(value.to_string());
            return;
        }

        stringify_at_path(value, &path);
    }
}

fn find_largest_subtree(value: &serde_json::Value) -> Vec<String> {
    fn recurse(
        value: &serde_json::Value,
        path: &mut Vec<String>,
        best_path: &mut Vec<String>,
        best_count: &mut usize,
    ) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, v) in map {
                    let child_count = count_fields(v);
                    if child_count > *best_count
                        && matches!(
                            v,
                            serde_json::Value::Object(_) | serde_json::Value::Array(_)
                        )
                    {
                        *best_count = child_count;
                        path.push(key.clone());
                        *best_path = path.clone();
                        path.pop();
                    }
                    path.push(key.clone());
                    recurse(v, path, best_path, best_count);
                    path.pop();
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let child_count = count_fields(v);
                    let key = format!("[{}]", i);
                    if child_count > *best_count
                        && matches!(
                            v,
                            serde_json::Value::Object(_) | serde_json::Value::Array(_)
                        )
                    {
                        *best_count = child_count;
                        path.push(key.clone());
                        *best_path = path.clone();
                        path.pop();
                    }
                    path.push(key);
                    recurse(v, path, best_path, best_count);
                    path.pop();
                }
            }
            _ => {}
        }
    }

    let mut path = Vec::new();
    let mut best_path = Vec::new();
    let mut best_count = 0usize;
    recurse(value, &mut path, &mut best_path, &mut best_count);
    best_path
}

fn stringify_at_path(value: &mut serde_json::Value, path: &[String]) {
    if path.is_empty() {
        return;
    }

    let mut current = value;
    for (i, key) in path.iter().enumerate() {
        let is_last = i == path.len() - 1;

        if key.starts_with('[') && key.ends_with(']') {
            let idx: usize = key[1..key.len() - 1].parse().unwrap_or(0);
            if let serde_json::Value::Array(arr) = current {
                if is_last {
                    if let Some(v) = arr.get_mut(idx) {
                        *v = serde_json::Value::String(v.to_string());
                    }
                    return;
                }
                current = arr.get_mut(idx).unwrap();
            } else {
                return;
            }
        } else if let serde_json::Value::Object(map) = current {
            if is_last {
                if let Some(v) = map.get_mut(key) {
                    *v = serde_json::Value::String(v.to_string());
                }
                return;
            }
            current = map.get_mut(key).unwrap();
        } else {
            return;
        }
    }
}

fn build_bulk_body(batch: &[EnrichedEvent], index_prefix: &str) -> Result<String> {
    let mut body = String::with_capacity(batch.len() * 256);
    for ev in batch {
        let id = &ev.event.id;
        let idx = resolve_index(ev, index_prefix);
        body.push_str("{\"index\":{\"_index\":\"");
        body.push_str(&idx);
        body.push_str("\",\"_id\":\"");
        body.push_str(id);
        body.push_str("\"}}\n");
        body.push_str(&serde_json::to_string(&ev)?);
        body.push('\n');
    }
    Ok(body)
}

/// Create a fallback event with raw message and error info when ES ingestion fails.
pub fn create_fallback_event(
    original: &EnrichedEvent,
    error_type: &str,
    error_reason: &str,
) -> EnrichedEvent {
    let mut fallback_parsed = serde_json::json!({
        "_ingestion_error": {
            "type": error_type,
            "reason": error_reason.chars().take(500).collect::<String>(),
            "original_message_preview": original.message.as_str()
                .map(|s| s.chars().take(1000).collect::<String>())
                .unwrap_or_else(|| original.message.to_string().chars().take(1000).collect()),
        }
    });

    if let Some(ref parsed) = original.parsed {
        if let Some(event) = parsed.get("event") {
            fallback_parsed["event"] = event.clone();
        }
        if let Some(level) = parsed.get("level") {
            fallback_parsed["level"] = level.clone();
        }
        if let Some(service) = parsed.get("service_name") {
            fallback_parsed["service_name"] = service.clone();
        }
    }

    EnrichedEvent {
        timestamp: original.timestamp.clone(),
        event: original.event.clone(),
        message: original.message.clone(),
        parsed: Some(fallback_parsed),
        target_index: original.target_index.clone(),
        tags: {
            let mut tags = original.tags.clone();
            tags.push("ingestion_error".to_string());
            tags
        },
    }
}

struct FailedItem {
    index: usize,
    kind: FailureKind,
    error_type: String,
    reason: String,
}

fn parse_failed_items(resp_body: &str, batch: &[EnrichedEvent]) -> Vec<FailedItem> {
    let parsed: Result<BulkResponse, _> = serde_json::from_str(resp_body);
    let Ok(bulk_resp) = parsed else {
        return Vec::new();
    };

    if !bulk_resp.errors {
        return Vec::new();
    }

    bulk_resp
        .items
        .into_iter()
        .enumerate()
        .filter_map(|(i, item)| {
            let result = item.index?;
            if result.status >= 400 {
                if i < batch.len() {
                    let (kind, error_type, reason) = match result.error {
                        Some(ref err) => (
                            classify_error(err),
                            err.error_type.clone(),
                            err.reason.clone(),
                        ),
                        None => (
                            FailureKind::Other,
                            "unknown".to_string(),
                            "no error details".to_string(),
                        ),
                    };
                    Some(FailedItem {
                        index: i,
                        kind,
                        error_type,
                        reason,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

pub fn resolve_index(ev: &EnrichedEvent, index_prefix: &str) -> String {
    if let Some(idx) = ev.target_index.as_ref() {
        return idx.clone();
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(&ev.timestamp) {
        let date = dt.with_timezone(&Utc).format("%Y.%m.%d").to_string();
        return format!("{}-{}", index_prefix, date);
    }
    format!("{}-default", index_prefix)
}

/// ES resource usage stats
struct EsResourceStats {
    heap_percent: f64,
    cpu_percent: f64,
}

/// Check ES heap and CPU usage by querying /_nodes/stats.
/// Returns heap and CPU usage as fractions (0.0 to 1.0).
async fn check_es_resources(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
) -> Result<EsResourceStats> {
    let url = format!("{}/_nodes/stats/jvm,os", base_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .basic_auth(user, Some(pass))
        .timeout(Duration::from_secs(5))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("ES stats request failed: {}", resp.status());
    }

    let body: serde_json::Value = resp.json().await?;

    let mut max_heap_percent = 0.0f64;
    let mut max_cpu_percent = 0.0f64;

    if let Some(nodes) = body.get("nodes").and_then(|n| n.as_object()) {
        for (_node_id, node_stats) in nodes {
            // Check JVM heap
            if let Some(jvm) = node_stats.get("jvm") {
                if let Some(mem) = jvm.get("mem") {
                    let used = mem
                        .get("heap_used_in_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let max = mem
                        .get("heap_max_in_bytes")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1);

                    if max > 0 {
                        let percent = used as f64 / max as f64;
                        max_heap_percent = max_heap_percent.max(percent);
                    }
                }
            }

            // Check OS CPU
            if let Some(os) = node_stats.get("os") {
                if let Some(cpu) = os.get("cpu") {
                    // ES returns cpu.percent as an integer 0-100
                    let cpu_pct = cpu.get("percent").and_then(|v| v.as_u64()).unwrap_or(0);
                    let percent = cpu_pct as f64 / 100.0;
                    max_cpu_percent = max_cpu_percent.max(percent);
                }
            }
        }
    }

    Ok(EsResourceStats {
        heap_percent: max_heap_percent,
        cpu_percent: max_cpu_percent,
    })
}
