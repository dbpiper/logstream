use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use tracing::{info, warn};

use crate::es_http::EsHttp;

#[derive(Clone)]
pub struct EsConflictResolver {
    http: EsHttp,
    index_prefix: Arc<str>,
}

impl EsConflictResolver {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
        index_prefix: impl Into<Arc<str>>,
    ) -> Result<Self> {
        Ok(Self {
            http: EsHttp::new(base_url, user, pass, timeout, true)?,
            index_prefix: index_prefix.into(),
        })
    }

    pub async fn run_conflict_reindex(&self) -> Result<()> {
        let indices = self.conflict_indices().await?;
        if indices.is_empty() {
            info!("conflict scan: none detected");
            return Ok(());
        }
        info!("conflict scan: reindexing {} indices", indices.len());
        for idx in indices {
            if let Err(err) = self.reindex_index(&idx).await {
                warn!("conflict reindex failed for {}: {err:?}", idx);
            }
        }
        Ok(())
    }

    pub async fn conflict_indices(&self) -> Result<Vec<String>> {
        let v: Value = self
            .http
            .get_value(
                &format!("{}-*/_field_caps?fields=*", self.index_prefix),
                "field_caps request",
            )
            .await
            .context("field_caps parse")?;
        let mut indices: HashSet<String> = HashSet::new();
        if let Some(fields) = v.get("fields").and_then(|f| f.as_object()) {
            for value in fields.values() {
                if let Some(type_map) = value.as_object() {
                    if type_map.len() <= 1 {
                        continue;
                    }
                    for tm in type_map.values() {
                        if let Some(idx_list) = tm.get("indices").and_then(|i| i.as_array()) {
                            for idx in idx_list {
                                if let Some(s) = idx.as_str() {
                                    indices.insert(s.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(indices.into_iter().collect())
    }

    pub async fn reindex_index(&self, index: &str) -> Result<()> {
        let temp = format!("{index}-conflict-temp");

        // Best-effort cleanup
        let _ = self.http.delete_allow_404(&temp, "delete temp index").await;

        // Create temp index with tolerant settings
        let create_body = serde_json::json!({
            "settings": {
                "index": {
                    "mapping.coerce": true,
                    "mapping.ignore_malformed": true,
                    "number_of_replicas": 0,
                    "refresh_interval": "-1"
                }
            }
        });
        let _: Value = self
            .http
            .put_value(&temp, &create_body, "create temp index")
            .await?;

        // Reindex original -> temp
        self.reindex(index, &temp)
            .await
            .context("reindex to temp")?;

        // Delete original (best effort)
        let _ = self
            .http
            .delete_allow_404(index, "delete original index")
            .await;

        // Reindex temp -> original
        self.reindex(&temp, index).await.context("reindex back")?;

        // Cleanup temp
        let _ = self
            .http
            .delete_allow_404(&temp, "cleanup temp index")
            .await;

        info!("conflict reindex complete for {}", index);
        Ok(())
    }

    async fn reindex(&self, source: &str, dest: &str) -> Result<()> {
        let body = serde_json::json!({
            "source": { "index": source },
            "dest": { "index": dest }
        });
        let _: Value = self
            .http
            .post_value(
                "_reindex?wait_for_completion=true",
                &body,
                "reindex request",
            )
            .await?;
        Ok(())
    }
}
