use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{info, warn};

use crate::es_counts::EsCounter;
use crate::es_window::EsWindowClient;

fn end_inclusive_for_exclusive_end(start_ms: i64, end_ms: i64) -> Option<i64> {
    (end_ms > start_ms).then_some(end_ms.saturating_sub(1))
}

#[derive(Clone)]
pub struct EsRepairClient {
    client: Client,
    base_url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
    timeout: Duration,
}

impl EsRepairClient {
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
            timeout,
        })
    }

    pub async fn ensure_data_stream(&self, name: &str) -> Result<()> {
        let url = format!(
            "{}/_data_stream/{}",
            self.base_url.trim_end_matches('/'),
            name
        );
        let resp = self
            .client
            .put(&url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .send()
            .await?;

        if resp.status().is_success() || resp.status().as_u16() == 409 {
            return Ok(());
        }
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if text.contains("resource_already_exists_exception") || text.contains("already exists") {
            return Ok(());
        }
        anyhow::bail!(
            "ensure data stream failed status={} body_sample={}",
            status,
            &text[..text.len().min(500)]
        );
    }

    pub async fn reindex_window(
        &self,
        source: &str,
        dest: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<()> {
        let url = format!("{}/_reindex", self.base_url.trim_end_matches('/'));
        let body = serde_json::json!({
            "conflicts": "proceed",
            "source": {
                "index": source,
                "query": {
                    "range": {
                        "@timestamp": { "gte": start_ms, "lt": end_ms, "format": "epoch_millis" }
                    }
                }
            },
            "dest": { "index": dest, "op_type": "create" }
        });

        let resp = self
            .client
            .post(&url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .json(&body)
            .send()
            .await
            .context("reindex request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "reindex failed status={} body_sample={}",
                status,
                &text[..text.len().min(500)]
            );
        }
        Ok(())
    }

    pub async fn verify_window_equivalent(
        &self,
        source: &str,
        dest: &str,
        start_ms: i64,
        end_ms: i64,
        sample: usize,
    ) -> Result<bool> {
        let Some(end_inclusive_ms) = end_inclusive_for_exclusive_end(start_ms, end_ms) else {
            return Ok(true);
        };
        let src = EsCounter::new(
            self.base_url.clone(),
            self.user.clone(),
            self.pass.clone(),
            self.timeout,
            Arc::<str>::from(source),
        )?;
        let dst = EsCounter::new(
            self.base_url.clone(),
            self.user.clone(),
            self.pass.clone(),
            self.timeout,
            Arc::<str>::from(dest),
        )?;

        let src_count = src.count_range(start_ms, end_inclusive_ms).await?;
        let dst_count = dst.count_range(start_ms, end_inclusive_ms).await?;
        if src_count != dst_count {
            warn!(
                "repair verify: count mismatch source={} dest={} src_count={} dst_count={}",
                source, dest, src_count, dst_count
            );
            return Ok(false);
        }

        let (src_first, src_last) = src.sample_ids(start_ms, end_inclusive_ms, sample).await?;
        let (dst_first, dst_last) = dst.sample_ids(start_ms, end_inclusive_ms, sample).await?;
        if src_first != dst_first || src_last != dst_last {
            warn!(
                "repair verify: boundary mismatch source={} dest={}",
                source, dest
            );
            return Ok(false);
        }
        Ok(true)
    }

    pub async fn cutover_alias(&self, alias: &str, new_target: &str) -> Result<()> {
        let base = self.base_url.trim_end_matches('/');
        let get_url = format!("{}/_alias/{}", base, alias);
        let resp = self
            .client
            .get(&get_url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .send()
            .await?;

        let mut actions: Vec<serde_json::Value> = Vec::new();
        if resp.status().is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            if let Some(obj) = body.as_object() {
                for (idx, _) in obj.iter() {
                    actions.push(serde_json::json!({
                        "remove": { "index": idx, "alias": alias }
                    }));
                }
            }
        }
        actions.push(serde_json::json!({
            "add": { "index": new_target, "alias": alias, "is_write_index": true }
        }));

        let update_url = format!("{}/_aliases", base);
        let payload = serde_json::json!({ "actions": actions });
        let update_resp = self
            .client
            .post(&update_url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .json(&payload)
            .send()
            .await?;

        if !update_resp.status().is_success() {
            let status = update_resp.status();
            let text = update_resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "alias cutover failed status={} body_sample={}",
                status,
                &text[..text.len().min(500)]
            );
        }
        Ok(())
    }

    pub async fn resolve_alias_single_target(&self, alias: &str) -> Result<Option<String>> {
        let base = self.base_url.trim_end_matches('/');
        let get_url = format!("{}/_alias/{}", base, alias);
        let resp = self
            .client
            .get(&get_url)
            .basic_auth(&*self.user, Some(&*self.pass))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(None);
        }
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        let Some(obj) = body.as_object() else {
            return Ok(None);
        };
        let mut keys = obj.keys();
        let first = keys.next().map(|s| s.to_string());
        if keys.next().is_some() {
            return Ok(None);
        }
        Ok(first)
    }

    pub async fn cleanup_old_window_indices(
        &self,
        source_stream: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<()> {
        let Some(end_inclusive_ms) = end_inclusive_for_exclusive_end(start_ms, end_ms) else {
            return Ok(());
        };
        let window = EsWindowClient::new(
            self.base_url.clone(),
            self.user.clone(),
            self.pass.clone(),
            self.timeout,
        )?;
        let indices = window
            .indices_fully_within_window(source_stream, start_ms, end_inclusive_ms)
            .await?;
        for idx in indices {
            let url = format!("{}/{}", self.base_url.trim_end_matches('/'), idx);
            let resp = self
                .client
                .delete(&url)
                .basic_auth(&*self.user, Some(&*self.pass))
                .send()
                .await?;
            if !resp.status().is_success() && resp.status().as_u16() != 404 {
                warn!("cleanup: failed to delete {} status={}", idx, resp.status());
            }
        }
        Ok(())
    }

    pub async fn repair_window(
        &self,
        alias: &str,
        source_stream: &str,
        dest_stream: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<()> {
        self.ensure_data_stream(dest_stream).await?;
        self.reindex_window(source_stream, dest_stream, start_ms, end_ms)
            .await?;
        let ok = self
            .verify_window_equivalent(source_stream, dest_stream, start_ms, end_ms, 64)
            .await?;
        if !ok {
            anyhow::bail!(
                "repair verification failed for window {}-{}",
                start_ms,
                end_ms
            );
        }
        self.cutover_alias(alias, dest_stream).await?;
        self.cleanup_old_window_indices(source_stream, start_ms, end_ms)
            .await?;
        info!(
            "repair complete: alias={} source={} dest={} window={}..{}",
            alias, source_stream, dest_stream, start_ms, end_ms
        );
        Ok(())
    }
}

pub fn next_versioned_stream(current: &str, stable_alias: &str) -> String {
    if let Some((prefix, suffix)) = current.rsplit_once('-') {
        if suffix == "v1" {
            return format!("{prefix}-v2");
        }
        if suffix == "v2" {
            return format!("{prefix}-v1");
        }
    }
    format!("{stable_alias}-v2")
}
