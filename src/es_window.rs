use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Clone)]
pub struct EsWindowClient {
    client: Client,
    base_url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
}

impl EsWindowClient {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            base_url: base_url.into(),
            user: user.into(),
            pass: pass.into(),
        })
    }

    pub async fn data_stream_backing_indices(&self, stream: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/_data_stream/{}",
            self.base_url.trim_end_matches('/'),
            stream
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .send()
            .await
            .context("get data stream")?;

        if !resp.status().is_success() {
            anyhow::bail!("data stream lookup failed status={}", resp.status());
        }

        let parsed: DataStreamGetResp = resp.json().await.unwrap_or_default();
        let indices = parsed
            .data_streams
            .into_iter()
            .flat_map(|ds| ds.indices.into_iter().map(|i| i.index_name))
            .collect::<Vec<String>>();
        Ok(indices)
    }

    pub async fn index_time_bounds_ms(&self, index: &str) -> Result<Option<TimeBoundsMs>> {
        let url = format!("{}/{}/_search", self.base_url.trim_end_matches('/'), index);
        let body = serde_json::json!({
            "size": 0,
            "aggs": {
                "min_ts": { "min": { "field": "@timestamp" } },
                "max_ts": { "max": { "field": "@timestamp" } }
            }
        });

        let resp = self
            .client
            .post(&url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .json(&body)
            .send()
            .await
            .context("index bounds search")?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let parsed: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        let min = parsed
            .pointer("/aggregations/min_ts/value")
            .and_then(|v| v.as_f64())
            .map(|v| v as i64);
        let max = parsed
            .pointer("/aggregations/max_ts/value")
            .and_then(|v| v.as_f64())
            .map(|v| v as i64);

        match (min, max) {
            (Some(min_ms), Some(max_ms)) if min_ms > 0 && max_ms > 0 => {
                Ok(Some(TimeBoundsMs { min_ms, max_ms }))
            }
            _ => Ok(None),
        }
    }

    pub async fn indices_overlapping_window(
        &self,
        stream: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let indices = self.data_stream_backing_indices(stream).await?;
        let mut overlapping = Vec::new();
        for idx in indices {
            let Some(bounds) = self.index_time_bounds_ms(&idx).await? else {
                continue;
            };
            if bounds.overlaps(start_ms, end_ms) {
                overlapping.push(idx);
            }
        }
        Ok(overlapping)
    }

    pub async fn indices_fully_within_window(
        &self,
        stream: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>> {
        let indices = self.data_stream_backing_indices(stream).await?;
        let mut selected = Vec::new();
        for idx in indices {
            let Some(bounds) = self.index_time_bounds_ms(&idx).await? else {
                continue;
            };
            if bounds.min_ms >= start_ms && bounds.max_ms <= end_ms {
                selected.push(idx);
            }
        }
        Ok(selected)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TimeBoundsMs {
    pub min_ms: i64,
    pub max_ms: i64,
}

impl TimeBoundsMs {
    pub fn overlaps(&self, start_ms: i64, end_ms: i64) -> bool {
        self.max_ms >= start_ms && self.min_ms <= end_ms
    }
}

#[derive(Default, Deserialize)]
struct DataStreamGetResp {
    #[serde(default)]
    data_streams: Vec<DataStreamEntry>,
}

#[derive(Default, Deserialize)]
struct DataStreamEntry {
    #[serde(default)]
    indices: Vec<DataStreamIndex>,
}

#[derive(Default, Deserialize)]
struct DataStreamIndex {
    #[serde(default)]
    index_name: String,
}
