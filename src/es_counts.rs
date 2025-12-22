use std::sync::Arc;
use std::time::Duration;

use crate::es_http::EsHttp;
use crate::es_query;
use anyhow::{Context, Result};
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
    _id: String,
}

#[derive(Clone)]
pub struct EsCounter {
    http: EsHttp,
    target: Arc<str>,
}

impl EsCounter {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
        target: impl Into<Arc<str>>,
    ) -> Result<Self> {
        Ok(Self {
            http: EsHttp::new(base_url, user, pass, timeout, true)?,
            target: target.into(),
        })
    }

    pub fn from_http(http: EsHttp, target: impl Into<Arc<str>>) -> Self {
        Self {
            http,
            target: target.into(),
        }
    }

    pub async fn count_range(&self, start_ms: i64, end_ms: i64) -> Result<u64> {
        if end_ms <= start_ms {
            return Ok(0);
        }
        let end_inclusive_ms = end_ms.saturating_sub(1);
        let body = serde_json::json!({
            "query": es_query::ts_range_query(start_ms, end_inclusive_ms)
        });
        let parsed: CountResp = self
            .http
            .post_json(
                &format!("{}/_count", self.target),
                &body,
                "es count request",
            )
            .await
            .context("es count parse")?;
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
        if end_ms <= start_ms {
            return Ok(0);
        }
        let end_inclusive_ms = end_ms.saturating_sub(1);
        let body = serde_json::json!({
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        { "terms": { "_id": ids } },
                        es_query::ts_range_query(start_ms, end_inclusive_ms)
                    ]
                }
            }
        });
        let v: serde_json::Value = self
            .http
            .post_value(
                &format!("{}/_search", self.target),
                &body,
                "es terms search request",
            )
            .await
            .context("es terms parse")?;
        let count = v
            .get("hits")
            .and_then(|h| h.get("total"))
            .and_then(|t| t.get("value"))
            .and_then(|n| n.as_u64())
            .unwrap_or(0);
        Ok(count)
    }

    pub async fn delete_range(&self, start_ms: i64, end_ms: i64) -> Result<()> {
        if end_ms <= start_ms {
            return Ok(());
        }
        let end_inclusive_ms = end_ms.saturating_sub(1);
        let body = serde_json::json!({
            "query": es_query::ts_range_query(start_ms, end_inclusive_ms)
        });
        let _: serde_json::Value = self
            .http
            .post_value(
                &format!("{}/_delete_by_query?conflicts=proceed", self.target),
                &body,
                "es delete_by_query request",
            )
            .await?;
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
        if end_ms <= start_ms {
            return Ok(Vec::new());
        }
        let end_inclusive_ms = end_ms.saturating_sub(1);
        let sort_dir = if asc { "asc" } else { "desc" };
        let body = serde_json::json!({
            "size": limit,
            "sort": [
                { "@timestamp": { "order": sort_dir } },
                { "_id": { "order": sort_dir } }
            ],
            "_source": false,
            "query": es_query::ts_range_query(start_ms, end_inclusive_ms)
        });
        let parsed: SearchResp = self
            .http
            .post_json(
                &format!("{}/_search", self.target),
                &body,
                "es search request",
            )
            .await
            .context("es search parse")?;
        Ok(parsed.hits.hits.into_iter().map(|h| h._id).collect())
    }

    /// Get all event IDs in a time range (for orphan detection).
    pub async fn get_ids_in_range(&self, start_ms: i64, end_ms: i64) -> Result<Vec<String>> {
        if end_ms <= start_ms {
            return Ok(Vec::new());
        }
        let end_inclusive_ms = end_ms.saturating_sub(1);
        let mut all_ids = Vec::new();
        let mut search_after: Option<(i64, String)> = None;
        const BATCH_SIZE: usize = 5000;

        loop {
            let mut body = serde_json::json!({
                "size": BATCH_SIZE,
                "sort": [
                    { "@timestamp": { "order": "asc" } },
                    { "_id": { "order": "asc" } }
                ],
                "_source": false,
                "query": es_query::ts_range_query(start_ms, end_inclusive_ms)
            });

            if let Some((ts, id)) = &search_after {
                body["search_after"] = serde_json::json!([ts, id]);
            }

            let parsed: serde_json::Value = self
                .http
                .post_value(
                    &format!("{}/_search", self.target),
                    &body,
                    "es get_ids_in_range request",
                )
                .await
                .context("es get_ids_in_range parse")?;
            let hits = parsed["hits"]["hits"].as_array();

            let Some(hits) = hits else {
                break;
            };

            if hits.is_empty() {
                break;
            }

            for hit in hits {
                if let Some(id) = hit["_id"].as_str() {
                    all_ids.push(id.to_string());
                }
                // Update search_after for pagination
                if let Some(sort) = hit["sort"].as_array() {
                    if sort.len() >= 2 {
                        let ts = sort[0].as_i64().unwrap_or(0);
                        let id_val = sort[1]
                            .as_str()
                            .or_else(|| hit["_id"].as_str())
                            .unwrap_or("")
                            .to_string();
                        search_after = Some((ts, id_val));
                    }
                }
            }

            if hits.len() < BATCH_SIZE {
                break;
            }
        }

        Ok(all_ids)
    }

    /// Delete specific event IDs (for orphan cleanup).
    pub async fn delete_ids(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let body = serde_json::json!({
            "query": {
                "terms": {
                    "_id": ids
                }
            }
        });
        let _: serde_json::Value = self
            .http
            .post_value(
                &format!("{}/_delete_by_query?conflicts=proceed", self.target),
                &body,
                "es delete_ids request",
            )
            .await?;

        Ok(())
    }
}
