use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct CountResp {
    count: u64,
}

#[derive(Debug, Deserialize)]
struct SearchResp {
    hits: Hits,
}

#[derive(Debug, Deserialize)]
struct Hits {
    hits: Vec<Hit>,
}

#[derive(Debug, Deserialize)]
struct Hit {
    #[serde(default)]
    _source: Option<HitSource>,
}

#[derive(Debug, Deserialize)]
struct HitSource {
    #[serde(default)]
    event: Option<EventMeta>,
}

#[derive(Debug, Deserialize)]
struct EventMeta {
    #[serde(default)]
    id: Option<String>,
}

#[derive(Clone)]
pub struct EsCounter {
    client: Client,
    base_url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
    index_prefix: Arc<str>,
}

impl EsCounter {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
        index_prefix: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            base_url: base_url.into(),
            user: user.into(),
            pass: pass.into(),
            index_prefix: index_prefix.into(),
        })
    }

    pub async fn count_range(&self, start_ms: i64, end_ms: i64) -> Result<u64> {
        let url = format!("{}/{}-*/_count", self.base_url, self.index_prefix);
        let body = serde_json::json!({
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_ms,
                        "lte": end_ms,
                        "format": "epoch_millis"
                    }
                }
            }
        });
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await
            .context("es count request")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("es count status {}", status);
        }
        let parsed: CountResp = resp.json().await.context("es count parse")?;
        Ok(parsed.count)
    }

    pub async fn count_ids_in_range(
        &self,
        start_ms: i64,
        end_ms: i64,
        ids: &[String],
    ) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let url = format!("{}/{}-*/_search", self.base_url, self.index_prefix);
        let body = serde_json::json!({
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        { "terms": { "event.id": ids } },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_ms,
                                    "lte": end_ms,
                                    "format": "epoch_millis"
                                }
                            }
                        }
                    ]
                }
            }
        });
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await
            .context("es terms search request")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("es terms search status {}", status);
        }
        let v: serde_json::Value = resp.json().await.context("es terms parse")?;
        let count = v
            .get("hits")
            .and_then(|h| h.get("total"))
            .and_then(|t| t.get("value"))
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        Ok(count)
    }

    pub async fn delete_range(&self, start_ms: i64, end_ms: i64) -> Result<()> {
        let url = format!(
            "{}/{}-*/_delete_by_query?conflicts=proceed",
            self.base_url, self.index_prefix
        );
        let body = serde_json::json!({
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_ms,
                        "lte": end_ms,
                        "format": "epoch_millis"
                    }
                }
            }
        });
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await
            .context("es delete_by_query request")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("es delete_by_query status {}", status);
        }
        Ok(())
    }

    pub async fn sample_ids(
        &self,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let first = self
            .search_ids(start_ms, end_ms, limit, true)
            .await
            .context("es sample first")?;
        let last = self
            .search_ids(start_ms, end_ms, limit, false)
            .await
            .context("es sample last")?;
        Ok((first, last))
    }

    async fn search_ids(
        &self,
        start_ms: i64,
        end_ms: i64,
        limit: usize,
        asc: bool,
    ) -> Result<Vec<String>> {
        let url = format!("{}/{}-*/_search", self.base_url, self.index_prefix);
        let sort_dir = if asc { "asc" } else { "desc" };
        let body = serde_json::json!({
            "size": limit,
            "sort": [
                { "@timestamp": { "order": sort_dir } },
                { "event.id": { "order": sort_dir } }
            ],
            "_source": ["event.id"],
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_ms,
                        "lte": end_ms,
                        "format": "epoch_millis"
                    }
                }
            }
        });
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await
            .context("es search request")?;
        let status = resp.status();
        if !status.is_success() {
            anyhow::bail!("es search status {}", status);
        }
        let parsed: SearchResp = resp.json().await.context("es search parse")?;
        let mut ids = Vec::new();
        for hit in parsed.hits.hits {
            if let Some(src) = hit._source {
                if let Some(ev) = src.event {
                    if let Some(id) = ev.id {
                        ids.push(id);
                    }
                }
            }
        }
        Ok(ids)
    }
}
