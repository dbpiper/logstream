use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Method;
use serde::Deserialize;
use std::collections::BTreeMap;

use crate::es_http::EsHttp;
use crate::es_query;

#[derive(Clone)]
pub struct EsWindowClient {
    http: EsHttp,
}

impl EsWindowClient {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            http: EsHttp::new(base_url, user, pass, timeout, true)?,
        })
    }

    pub fn from_http(http: EsHttp) -> Self {
        Self { http }
    }

    pub async fn data_stream_backing_indices(&self, stream: &str) -> Result<Vec<String>> {
        let parsed: DataStreamGetResp = self
            .http
            .get_json(&format!("_data_stream/{}", stream), "get data stream")
            .await?;
        let indices = parsed
            .data_streams
            .into_iter()
            .flat_map(|ds| ds.indices.into_iter().map(|i| i.index_name))
            .collect::<Vec<String>>();
        Ok(indices)
    }

    pub async fn index_time_bounds_ms(&self, index: &str) -> Result<Option<TimeBoundsMs>> {
        let body = es_query::min_max_ts_aggs_body();

        let resp = self
            .http
            .request(Method::POST, &format!("{}/_search", index))
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

    pub async fn backing_index_max_timestamp_ms(
        &self,
        stream: &str,
    ) -> Result<BTreeMap<String, i64>> {
        let body = es_query::max_ts_by_index_aggs_body();

        let parsed: StreamIndexMaxTsResp = self
            .http
            .post_json(
                &format!("{}/_search", stream),
                &body,
                "stream max_ts by backing index search",
            )
            .await?;
        Ok(parsed
            .aggregations
            .by_index
            .buckets
            .into_iter()
            .filter_map(|b| b.max_ts.value.map(|v| (b.key, v as i64)))
            .collect())
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

#[derive(Default, Deserialize)]
struct StreamIndexMaxTsResp {
    #[serde(default)]
    aggregations: StreamIndexMaxTsAggs,
}

#[derive(Default, Deserialize)]
struct StreamIndexMaxTsAggs {
    #[serde(default)]
    by_index: StreamIndexMaxTsByIndex,
}

#[derive(Default, Deserialize)]
struct StreamIndexMaxTsByIndex {
    #[serde(default)]
    buckets: Vec<StreamIndexMaxTsBucket>,
}

#[derive(Default, Deserialize)]
struct StreamIndexMaxTsBucket {
    #[serde(default)]
    key: String,
    #[serde(default)]
    max_ts: StreamIndexMaxTsValue,
}

#[derive(Default, Deserialize)]
struct StreamIndexMaxTsValue {
    #[serde(default)]
    value: Option<f64>,
}
