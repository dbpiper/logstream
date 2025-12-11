use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

pub struct EsConflictResolver {
    client: Client,
    base_url: String,
    user: String,
    pass: String,
    index_prefix: String,
}
impl Clone for EsConflictResolver {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            user: self.user.clone(),
            pass: self.pass.clone(),
            index_prefix: self.index_prefix.clone(),
        }
    }
}

impl EsConflictResolver {
    pub fn new(
        base_url: String,
        user: String,
        pass: String,
        timeout: Duration,
        index_prefix: String,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            base_url,
            user,
            pass,
            index_prefix,
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
        let url = format!(
            "{}/{}-*/_field_caps?fields=*",
            self.base_url, self.index_prefix
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("field_caps request")?;
        if !resp.status().is_success() {
            anyhow::bail!("field_caps status {}", resp.status());
        }
        let v: Value = resp.json().await.context("field_caps parse")?;
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
        let temp_url = format!("{}/{}", self.base_url, temp);
        let index_url = format!("{}/{}", self.base_url, index);

        // Best-effort cleanup
        let _ = self
            .client
            .delete(&temp_url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await;

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
        let resp = self
            .client
            .put(&temp_url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&create_body)
            .send()
            .await
            .context("create temp index")?;
        if !resp.status().is_success() {
            anyhow::bail!("create temp index status {}", resp.status());
        }

        // Reindex original -> temp
        self.reindex(index, &temp)
            .await
            .context("reindex to temp")?;

        // Delete original (best effort)
        let _ = self
            .client
            .delete(&index_url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await;

        // Reindex temp -> original
        self.reindex(&temp, index).await.context("reindex back")?;

        // Cleanup temp
        let _ = self
            .client
            .delete(&temp_url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await;

        info!("conflict reindex complete for {}", index);
        Ok(())
    }

    async fn reindex(&self, source: &str, dest: &str) -> Result<()> {
        let url = format!("{}/_reindex?wait_for_completion=true", self.base_url);
        let body = serde_json::json!({
            "source": { "index": source },
            "dest": { "index": dest }
        });
        let resp = self
            .client
            .post(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await
            .context("reindex request")?;
        if !resp.status().is_success() {
            anyhow::bail!("reindex status {}", resp.status());
        }
        Ok(())
    }
}
