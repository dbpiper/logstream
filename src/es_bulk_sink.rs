use std::{sync::Arc, time::Duration};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::adaptive::AdaptiveController;
use crate::es_recovery::{self, ClusterStressTracker};
use crate::types::EnrichedEvent;

#[derive(Clone, Debug)]
pub struct EsBulkConfig {
    pub url: String,
    pub user: String,
    pub pass: String,
    pub batch_size: usize, // min batch
    pub max_batch_size: usize,
    pub timeout: Duration,
    pub gzip: bool,
    pub index_prefix: String,
}

pub struct EsBulkSink {
    cfg: EsBulkConfig,
    client: Client,
    stress_tracker: Arc<ClusterStressTracker>,
}

impl EsBulkSink {
    pub fn new(cfg: EsBulkConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(cfg.timeout)
            .pool_max_idle_per_host(64)
            .gzip(cfg.gzip)
            .build()?;
        Ok(Self {
            cfg,
            client,
            stress_tracker: Arc::new(ClusterStressTracker::new()),
        })
    }

    /// Get the stress tracker for external monitoring.
    pub fn stress_tracker(&self) -> Arc<ClusterStressTracker> {
        self.stress_tracker.clone()
    }

    /// Start with event router and adaptive rate control.
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
                if stress_level == es_recovery::StressLevel::Critical {
                    // Critical stress - long pause to let ES recover
                    sleep(Duration::from_secs(10)).await;
                } else if stress_level == es_recovery::StressLevel::Elevated {
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
                let effective_batch = if stress_level == es_recovery::StressLevel::Critical {
                    target_batch / 4 // 25% batch size when critical
                } else if stress_level == es_recovery::StressLevel::Elevated {
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
                        &stress_tracker,
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
    stress_tracker: &ClusterStressTracker,
) -> Result<()> {
    // Reduce concurrency if cluster is stressed
    let effective_in_flight = match stress_tracker.stress_level() {
        es_recovery::StressLevel::Critical => 1, // Single request when critical
        es_recovery::StressLevel::Elevated => (max_in_flight / 2).max(1),
        es_recovery::StressLevel::Normal => max_in_flight,
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

        handles.push(tokio::spawn(async move {
            let _p = permit;
            let started = std::time::Instant::now();
            let res = send_bulk_tracked(&c, &url, &user, &pass, &index_prefix, &chunk_vec).await;
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

/// Send bulk with tracked recovery (uses new circuit breaker detection).
async fn send_bulk_tracked(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
    batch: &[EnrichedEvent],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut body = String::with_capacity(batch.len() * 256);
    for ev in batch {
        let id = ev.event.id.clone();
        let idx = resolve_index(ev, index_prefix);
        body.push_str("{\"index\":{\"_index\":\"");
        body.push_str(&idx);
        body.push_str("\",\"_id\":\"");
        body.push_str(&id);
        body.push_str("\"}}\n");
        body.push_str(&serde_json::to_string(&ev)?);
        body.push('\n');
    }

    let url = format!("{}/_bulk", base_url.trim_end_matches('/'));

    for attempt in 1..=20u64 {
        let send_result = client
            .post(&url)
            .basic_auth(user, Some(pass))
            .header("Content-Type", "application/x-ndjson")
            .body(body.clone())
            .send()
            .await;

        match send_result {
            Ok(resp) if resp.status().is_success() => {
                let resp_body = resp.text().await.unwrap_or_default();

                // Only warn if there are actual failures (status >= 400)
                // ES sets "errors":true for version conflicts (updates) which are normal
                if resp_body.contains("\"errors\":true") {
                    let has_real_errors = resp_body.contains("\"status\":4")
                        || resp_body.contains("\"status\":5")
                        || resp_body.contains("\"failed\":1");

                    if has_real_errors {
                        warn!(
                            "es bulk has item failures: {}",
                            &resp_body[..resp_body.len().min(500)]
                        );
                    } else {
                        // Version conflicts/updates are normal during backfill - just debug log
                        tracing::debug!(
                            "es bulk has updates (not errors): {} items",
                            batch.len()
                        );
                    }
                }

                info!("es bulk sent batch={} status=200 OK", batch.len());
                return Ok(());
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

        if attempt >= 20 {
            anyhow::bail!("es bulk failed after 20 retries");
        }

        // Exponential backoff
        let backoff = Duration::from_millis(500 * attempt.min(10));
        sleep(backoff).await;
    }

    Ok(())
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
