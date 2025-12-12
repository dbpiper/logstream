use std::{sync::Arc, time::Duration};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::adaptive::AdaptiveController;
use crate::es_recovery;
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
}

impl EsBulkSink {
    pub fn new(cfg: EsBulkConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(cfg.timeout)
            .pool_max_idle_per_host(64)
            .gzip(cfg.gzip)
            .build()?;
        Ok(Self { cfg, client })
    }

    /// Start with scheduler and adaptive rate control.
    pub fn start_adaptive(
        &self,
        mut scheduler: crate::scheduler::Scheduler,
        adaptive: Arc<AdaptiveController>,
    ) {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
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

                // Receive events up to batch size
                let Some(ev) = scheduler.recv().await else {
                    break;
                };
                buf.push(ev);

                // Drain more if available (up to batch size)
                while buf.len() < target_batch {
                    match tokio::time::timeout(Duration::from_millis(10), scheduler.recv()).await {
                        Ok(Some(ev)) => buf.push(ev),
                        _ => break,
                    }
                }

                if buf.len() >= target_batch || buf.len() >= cfg.min_batch_for_send() {
                    let batch = std::mem::take(&mut buf);
                    let started = std::time::Instant::now();

                    let res =
                        send_bulk_adaptive(&client, &cfg, &batch, adaptive.clone(), max_in_flight)
                            .await;

                    let elapsed = started.elapsed();
                    let latency_ms = elapsed.as_millis() as u64;

                    match res {
                        Ok(_) => {
                            adaptive.record_latency(latency_ms, true).await;
                            info!(
                                "adaptive bulk: batch={} latency={}ms (target_batch={} in_flight={})",
                                batch.len(),
                                latency_ms,
                                target_batch,
                                max_in_flight
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
                let _ = send_bulk(
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

/// Send bulk with adaptive concurrency control.
async fn send_bulk_adaptive(
    client: &Client,
    cfg: &EsBulkConfig,
    batch: &[EnrichedEvent],
    adaptive: Arc<AdaptiveController>,
    max_in_flight: usize,
) -> Result<()> {
    let sem = Arc::new(Semaphore::new(max_in_flight));
    let chunk_size = (batch.len() / max_in_flight).max(100);

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
            let res = send_bulk(&c, &url, &user, &pass, &index_prefix, &chunk_vec).await;
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

async fn send_bulk(
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
    let mut attempt = 0u64;
    let mut recovery_attempts = 0u64;

    loop {
        attempt += 1;
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

                if resp_body.contains("\"errors\":true") {
                    // Some items failed - check cluster health and fix any issues
                    if recovery_attempts < 3 {
                        let recovered = es_recovery::check_and_recover(
                            client,
                            base_url,
                            user,
                            pass,
                            index_prefix,
                        )
                        .await;

                        if recovered {
                            recovery_attempts += 1;
                            info!(
                                "es bulk: cluster issue fixed, retrying (attempt {})",
                                recovery_attempts
                            );
                            continue;
                        }
                    }

                    // Log remaining errors but continue (may be version conflicts, etc)
                    warn!(
                        "es bulk has item errors, sample: {}",
                        &resp_body[..resp_body.len().min(500)]
                    );
                }

                info!("es bulk sent batch={} status=200 OK", batch.len());
                return Ok(());
            }

            Ok(resp) => {
                let status = resp.status();
                let resp_body = resp.text().await.unwrap_or_default();

                // Non-2xx response - check cluster health and fix any issues
                if recovery_attempts < 3 {
                    let recovered =
                        es_recovery::check_and_recover(client, base_url, user, pass, index_prefix)
                            .await;

                    if recovered {
                        recovery_attempts += 1;
                        info!(
                            "es bulk status={}: cluster issue fixed, retrying (attempt {})",
                            status, recovery_attempts
                        );
                        continue;
                    }
                }

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

        let backoff = Duration::from_millis(500 * attempt.min(10));
        sleep(backoff).await;
    }
}

fn resolve_index(ev: &EnrichedEvent, index_prefix: &str) -> String {
    if let Some(idx) = ev.target_index.as_ref() {
        return idx.clone();
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(&ev.timestamp) {
        let date = dt.with_timezone(&Utc).format("%Y.%m.%d").to_string();
        return format!("{}-{}", index_prefix, date);
    }
    format!("{}-default", index_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EventMeta;

    fn sample_event(timestamp: &str, target_index: Option<String>) -> EnrichedEvent {
        EnrichedEvent {
            timestamp: timestamp.to_string(),
            event: EventMeta {
                id: "test-id".to_string(),
            },
            message: serde_json::Value::String("test".to_string()),
            parsed: None,
            target_index,
            tags: vec![],
        }
    }

    #[test]
    fn test_resolve_index_with_target() {
        let ev = sample_event("2025-12-11T12:00:00Z", Some("custom-index".to_string()));
        let idx = resolve_index(&ev, "logs");
        assert_eq!(idx, "custom-index");
    }

    #[test]
    fn test_resolve_index_from_timestamp() {
        let ev = sample_event("2025-12-11T12:00:00+00:00", None);
        let idx = resolve_index(&ev, "logs");
        assert_eq!(idx, "logs-2025.12.11");
    }

    #[test]
    fn test_resolve_index_different_prefix() {
        let ev = sample_event("2024-01-15T08:30:00Z", None);
        let idx = resolve_index(&ev, "myapp");
        assert_eq!(idx, "myapp-2024.01.15");
    }

    #[test]
    fn test_resolve_index_invalid_timestamp() {
        let ev = sample_event("not-a-timestamp", None);
        let idx = resolve_index(&ev, "logs");
        assert_eq!(idx, "logs-default");
    }

    #[test]
    fn test_resolve_index_empty_timestamp() {
        let ev = sample_event("", None);
        let idx = resolve_index(&ev, "logs");
        assert_eq!(idx, "logs-default");
    }

    #[test]
    fn test_es_bulk_config_clone() {
        let cfg = EsBulkConfig {
            url: "http://localhost:9200".to_string(),
            user: "elastic".to_string(),
            pass: "password".to_string(),
            batch_size: 100,
            max_batch_size: 1000,
            timeout: Duration::from_secs(30),
            gzip: true,
            index_prefix: "logs".to_string(),
        };

        let cloned = cfg.clone();
        assert_eq!(cloned.url, cfg.url);
        assert_eq!(cloned.batch_size, cfg.batch_size);
        assert_eq!(cloned.gzip, cfg.gzip);
    }

    #[test]
    fn test_es_bulk_sink_new() {
        let cfg = EsBulkConfig {
            url: "http://localhost:9200".to_string(),
            user: "elastic".to_string(),
            pass: "password".to_string(),
            batch_size: 100,
            max_batch_size: 1000,
            timeout: Duration::from_secs(30),
            gzip: true,
            index_prefix: "logs".to_string(),
        };

        let sink = EsBulkSink::new(cfg);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_resolve_index_with_timezone_offset() {
        let ev = sample_event("2025-06-15T10:30:00-05:00", None);
        let idx = resolve_index(&ev, "logs");
        // Should be UTC: 15:30 UTC on 2025-06-15
        assert_eq!(idx, "logs-2025.06.15");
    }

    #[test]
    fn test_resolve_index_target_takes_precedence() {
        // Even with a valid timestamp, target_index should take precedence
        let ev = sample_event("2025-12-11T12:00:00Z", Some("override-index".to_string()));
        let idx = resolve_index(&ev, "logs");
        assert_eq!(idx, "override-index");
    }
}
