use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_cloudwatchlogs::Client as CwClient;
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};

use crate::state::{CheckpointState, StreamCursor};
use crate::types::LogEvent;

#[derive(Clone, Debug)]
pub struct TailConfig {
    pub log_group: String,
    pub poll_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

#[derive(Clone)]
pub struct CloudWatchTailer {
    cfg: TailConfig,
    client: CwClient,
    checkpoint: CheckpointState,
    checkpoint_path: std::path::PathBuf,
}

impl CloudWatchTailer {
    pub fn new(
        cfg: TailConfig,
        client: CwClient,
        checkpoint: CheckpointState,
        checkpoint_path: std::path::PathBuf,
    ) -> Self {
        Self {
            cfg,
            client,
            checkpoint,
            checkpoint_path,
        }
    }

    pub async fn run(&mut self, tx: mpsc::Sender<LogEvent>) -> Result<()> {
        let mut backoff = self.cfg.backoff_base;
        loop {
            let started = Instant::now();
            match self.poll_once(&tx).await {
                Ok(_) => {
                    backoff = self.cfg.backoff_base;
                }
                Err(err) => {
                    warn!("poll error: {err:?}");
                    backoff = (backoff * 2).min(self.cfg.backoff_max);
                }
            }

            let elapsed = started.elapsed();
            if elapsed < self.cfg.poll_interval {
                sleep(self.cfg.poll_interval - elapsed).await;
            } else {
                sleep(backoff).await;
            }
        }
    }

    async fn poll_once(&mut self, tx: &mpsc::Sender<LogEvent>) -> Result<()> {
        // For simplicity, use a single global cursor keyed by log group
        let key = self.cfg.log_group.clone();
        let cursor = self.checkpoint.cursor_for(&key);
        let start_time = cursor
            .next_start_time_ms
            .unwrap_or_else(|| current_time_ms() - 5 * 60 * 1000);

        let mut next_token = cursor.next_token.clone();
        let mut latest_ts = start_time;

        loop {
            let mut req = self
                .client
                .filter_log_events()
                .log_group_name(&self.cfg.log_group)
                .start_time(start_time)
                .limit(10_000);

            if let Some(token) = &next_token {
                req = req.next_token(token);
            }

            let resp = send_with_backoff(req).await.context("filter_log_events")?;

            if let Some(events) = resp.events {
                for e in events {
                    if let (Some(id), Some(ts), Some(msg)) = (e.event_id, e.timestamp, e.message) {
                        latest_ts = latest_ts.max(ts);
                        let _ = tx
                            .send(LogEvent {
                                id,
                                timestamp_ms: ts,
                                message: msg,
                            })
                            .await;
                    }
                }
            }

            next_token = resp.next_token;

            if next_token.is_none() {
                break;
            }
        }

        // Advance past latest to avoid duplicates
        let new_cursor = StreamCursor {
            next_token: None,
            next_start_time_ms: Some(latest_ts + 1),
        };
        self.checkpoint.update_cursor(&key, new_cursor);
        self.checkpoint.save(&self.checkpoint_path)?;
        info!("checkpoint advanced to {}", latest_ts);
        Ok(())
    }

    /// Fetch events in range [start_ms, end_ms] without modifying checkpoints.
    pub async fn fetch_range(
        &self,
        start_ms: i64,
        end_ms: i64,
        tx: &mpsc::Sender<LogEvent>,
    ) -> Result<usize> {
        // Split into 16 chunks
        let range_ms = end_ms.saturating_sub(start_ms);
        let chunk_ms = (range_ms / 16).max(1);
        let mut chunks = Vec::new();
        let mut chunk_start = start_ms;
        while chunk_start < end_ms {
            let chunk_end = (chunk_start + chunk_ms).min(end_ms);
            chunks.push((chunk_start, chunk_end));
            chunk_start = chunk_end;
        }

        // Fetch chunks in parallel
        let permits = chunks.len().max(1);
        let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(permits));
        let mut handles = Vec::new();

        for (cs, ce) in chunks {
            let client = self.client.clone();
            let log_group = self.cfg.log_group.clone();
            let tx_clone = tx.clone();
            let sem = semaphore.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let mut next_token: Option<String> = None;
                let mut sent: usize = 0;
                loop {
                    let mut req = client
                        .filter_log_events()
                        .log_group_name(&log_group)
                        .start_time(cs)
                        .end_time(ce)
                        .limit(10_000);
                    if let Some(t) = &next_token {
                        req = req.next_token(t);
                    }
                    // Inline retry logic for spawned task
                    let resp = {
                        let mut attempt = 0u32;
                        loop {
                            attempt += 1;
                            match req.clone().send().await {
                                Ok(r) => break Ok(r),
                                Err(err) => {
                                    let msg = format!("{err:?}");
                                    let is_retryable = msg.contains("ThrottlingException")
                                        || msg.contains("ServiceUnavailable")
                                        || msg.contains("dispatch failure")
                                        || msg.contains("SendRequest");
                                    if is_retryable && attempt < 20 {
                                        let backoff = std::time::Duration::from_millis(
                                            500 * (attempt as u64).min(10),
                                        );
                                        tokio::time::sleep(backoff).await;
                                        continue;
                                    }
                                    break Err(anyhow::anyhow!("CW fetch failed: {err:?}"));
                                }
                            }
                        }
                    };
                    match resp {
                        Ok(r) => {
                            if let Some(events) = r.events {
                                for e in events {
                                    if let (Some(id), Some(ts), Some(msg)) =
                                        (e.event_id, e.timestamp, e.message)
                                    {
                                        let _ = tx_clone
                                            .send(LogEvent {
                                                id,
                                                timestamp_ms: ts,
                                                message: msg,
                                            })
                                            .await;
                                        sent += 1;
                                    }
                                }
                            }
                            next_token = r.next_token;
                            if next_token.is_none() {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!("chunk {}-{} error: {e:?}", cs, ce);
                            break;
                        }
                    }
                }
                sent
            }));
        }

        let mut total = 0usize;
        for h in handles {
            match h.await {
                Ok(n) => total += n,
                Err(e) => tracing::warn!("chunk task panicked: {e:?}"),
            }
        }
        Ok(total)
    }

    /// Sample IDs from start and end of range.
    pub async fn sample_ids(
        &self,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let mut first = Vec::new();
        let mut next_token: Option<String> = None;
        // First chunk
        loop {
            let mut req = self
                .client
                .filter_log_events()
                .log_group_name(&self.cfg.log_group)
                .start_time(start_ms)
                .end_time(end_ms)
                .limit(10_000);
            if let Some(t) = &next_token {
                req = req.next_token(t);
            }
            let resp = send_with_backoff(req)
                .await
                .context("filter_log_events sample first")?;
            if let Some(events) = resp.events {
                for e in events {
                    if let (Some(id), Some(_ts)) = (e.event_id, e.timestamp) {
                        if first.len() < limit {
                            first.push(id.clone());
                        }
                    }
                    if first.len() >= limit {
                        break;
                    }
                }
            }
            if first.len() >= limit {
                break;
            }
            next_token = resp.next_token;
            if next_token.is_none() {
                break;
            }
        }

        // Last chunk: scan the window and retain only the last N seen
        let mut last = Vec::new();
        let mut next_tail: Option<String> = None;
        loop {
            let mut req = self
                .client
                .filter_log_events()
                .log_group_name(&self.cfg.log_group)
                .start_time(start_ms)
                .end_time(end_ms)
                .limit(10_000);
            if let Some(t) = &next_tail {
                req = req.next_token(t);
            }
            let resp = send_with_backoff(req)
                .await
                .context("filter_log_events sample last")?;
            if let Some(events) = resp.events {
                for e in events {
                    if let (Some(id), Some(_ts)) = (e.event_id, e.timestamp) {
                        last.push(id.clone());
                        if last.len() > limit {
                            last.remove(0);
                        }
                    }
                }
            }
            next_tail = resp.next_token;
            if next_tail.is_none() {
                break;
            }
        }
        Ok((first, last))
    }

    /// Sample IDs within a time window.
    pub async fn sample_ids_window(
        &self,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .filter_log_events()
                .log_group_name(&self.cfg.log_group)
                .start_time(start_ms)
                .end_time(end_ms)
                .limit(10_000);
            if let Some(t) = &next_token {
                req = req.next_token(t);
            }
            let resp = send_with_backoff(req)
                .await
                .context("filter_log_events sample window")?;
            if let Some(events) = resp.events {
                for e in events {
                    if let (Some(id), Some(_ts)) = (e.event_id, e.timestamp) {
                        ids.push(id);
                        if ids.len() >= limit {
                            return Ok(ids);
                        }
                    }
                }
            }
            next_token = resp.next_token;
            if next_token.is_none() {
                break;
            }
        }
        Ok(ids)
    }
}

fn current_time_ms() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0));
    now.as_millis() as i64
}

/// Fetch a single time chunk sequentially with pagination
async fn send_with_backoff(
    req: aws_sdk_cloudwatchlogs::operation::filter_log_events::builders::FilterLogEventsFluentBuilder,
) -> Result<aws_sdk_cloudwatchlogs::operation::filter_log_events::FilterLogEventsOutput> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match req.clone().send().await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                let msg = format!("{err:?}");
                let is_retryable = msg.contains("ThrottlingException")
                    || msg.contains("ServiceUnavailable")
                    || msg.contains("dispatch failure")
                    || msg.contains("SendRequest");
                if is_retryable && attempt < 20 {
                    let backoff = Duration::from_millis(500 * (attempt as u64).min(10));
                    warn!(
                        "CW throttled/retryable attempt={}: retrying in {:?}",
                        attempt, backoff
                    );
                    sleep(backoff).await;
                    continue;
                }
                return Err(err).context("filter_log_events throttled/failed");
            }
        }
    }
}
