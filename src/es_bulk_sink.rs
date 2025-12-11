use std::{sync::Arc, time::Duration};

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::types::EnrichedEvent;

#[derive(Clone, Debug)]
pub struct EsBulkConfig {
    pub url: String,
    pub user: String,
    pub pass: String,
    pub batch_size: usize, // min batch
    pub max_batch_size: usize,
    pub max_in_flight: usize,
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

    pub fn start(&self, mut rx: mpsc::Receiver<EnrichedEvent>) {
        let cfg = self.cfg.clone();
        let client = self.client.clone();
        tokio::spawn(async move {
            let sem = Arc::new(Semaphore::new(cfg.max_in_flight));
            let mut in_flight: FuturesUnordered<tokio::task::JoinHandle<()>> =
                FuturesUnordered::new();
            let mut buf: Vec<EnrichedEvent> = Vec::with_capacity(cfg.max_batch_size);
            let target_batch = cfg.max_batch_size.max(cfg.batch_size);

            while let Some(ev) = rx.recv().await {
                buf.push(ev);
                if buf.len() >= target_batch {
                    let batch = std::mem::take(&mut buf);
                    let permit = sem.clone().acquire_owned().await.unwrap();
                    let c = client.clone();
                    let url = cfg.url.clone();
                    let user = cfg.user.clone();
                    let pass = cfg.pass.clone();
                    let index_prefix = cfg.index_prefix.clone();
                    in_flight.push(tokio::spawn(async move {
                        let _p = permit;
                        let started = std::time::Instant::now();
                        let res = send_bulk(&c, &url, &user, &pass, &index_prefix, &batch).await;
                        let elapsed = started.elapsed();
                        match res {
                            Ok(_) => {
                                info!(
                                    "es bulk sent batch={} status=200 latency_ms={}",
                                    batch.len(),
                                    elapsed.as_millis()
                                );
                            }
                            Err(err) => {
                                warn!("es bulk send failed: {err:?}");
                            }
                        }
                    }));
                }
                while in_flight.len() >= cfg.max_in_flight {
                    let _ = in_flight.next().await;
                }
            }
            if !buf.is_empty() {
                let batch = std::mem::take(&mut buf);
                if let Err(err) = send_bulk(
                    &client,
                    &cfg.url,
                    &cfg.user,
                    &cfg.pass,
                    &cfg.index_prefix,
                    batch.as_slice(),
                )
                .await
                {
                    warn!("es bulk send failed: {err:?}");
                }
            }
            while let Some(_done) = in_flight.next().await {}
        });
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
        // Deterministic id for idempotency
        let id = ev.event.id.clone();
        let idx = resolve_index(ev, index_prefix);
        body.push_str("{\"index\":{\"_index\":\"");
        body.push_str(&idx);
        body.push_str("\",\"_id\":\"");
        body.push_str(&id);
        body.push_str("\"}}\n");
        // Serialize event
        body.push_str(&serde_json::to_string(&ev)?);
        body.push('\n');
    }

    let url = format!("{}/_bulk", base_url.trim_end_matches('/'));
    let mut attempt = 0u64;
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
                // Check for item-level errors in bulk response
                if let Ok(body) = resp.text().await {
                    if body.contains("\"errors\":true") {
                        warn!(
                            "es bulk has item errors, sample: {}",
                            &body[..body.len().min(500)]
                        );
                    }
                }
                info!("es bulk sent batch={} status=200 OK", batch.len());
                return Ok(());
            }
            Ok(resp) => {
                warn!(
                    "es bulk status={} attempt={} body_sample={:?}",
                    resp.status(),
                    attempt,
                    resp.text().await.ok()
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
