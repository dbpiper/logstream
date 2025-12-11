use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tracing::{info, warn};

/// Detects and fixes Elasticsearch mapping drift using sampling-based binary search.
#[derive(Clone)]
pub struct SchemaHealer {
    client: Client,
    base_url: String,
    user: String,
    pass: String,
    timeout: Duration,
    index_prefix: String,
}

impl SchemaHealer {
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
            timeout,
            index_prefix,
        })
    }

    /// Wait for ES to be ready before proceeding.
    async fn wait_for_ready(&self) -> Result<()> {
        // Derive max attempts proportional to timeout (1 attempt per 2 seconds of timeout)
        let max_attempts = (self.timeout.as_secs() / 2).max(10) as u32;
        let wait_secs = 2u64;

        for attempt in 1..=max_attempts {
            let url = format!("{}/_cluster/health", self.base_url.trim_end_matches('/'));
            match self
                .client
                .get(&url)
                .basic_auth(&self.user, Some(&self.pass))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!("schema_heal: ES ready after {} attempts", attempt);
                    return Ok(());
                }
                Ok(_) | Err(_) => {
                    warn!(
                        "schema_heal: ES not ready (attempt {}/{}), waiting {}s",
                        attempt, max_attempts, wait_secs
                    );
                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                }
            }
        }
        anyhow::bail!("ES not ready after {} attempts", max_attempts)
    }

    /// Run schema healing: detect drift and reindex affected indices.
    /// Uses O(log n) binary search after proportional sampling.
    pub async fn heal(&self) -> Result<usize> {
        self.wait_for_ready().await?;
        let pattern = self.index_pattern();
        let indices = self.list_indices(&pattern).await?;
        if indices.is_empty() {
            return Ok(0);
        }

        let expected_schema = self.fetch_template_schema().await?;
        let drifted = self
            .detect_drifted_indices(&indices, &expected_schema)
            .await?;

        if drifted.is_empty() {
            info!(
                "schema_heal: no drift detected across {} indices",
                indices.len()
            );
            return Ok(0);
        }

        info!(
            "schema_heal: detected {} drifted indices, reindexing newest first",
            drifted.len()
        );

        // Process newest indices first (they're at the end, sorted by date)
        let mut fixed = 0;
        for idx in drifted.iter().rev() {
            if let Err(err) = self.reindex_with_correct_schema(idx).await {
                warn!("schema_heal: failed to reindex {}: {err:?}", idx);
            } else {
                fixed += 1;
            }
        }

        Ok(fixed)
    }

    /// List indices matching pattern, sorted by name (date order).
    async fn list_indices(&self, pattern: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/_cat/indices/{}?format=json&s=index",
            self.base_url.trim_end_matches('/'),
            pattern
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("list indices")?;

        if !resp.status().is_success() {
            return Ok(vec![]);
        }

        let items: Vec<Value> = resp.json().await.unwrap_or_default();
        let indices: Vec<String> = items
            .iter()
            .filter_map(|v| v.get("index").and_then(|i| i.as_str()).map(String::from))
            .collect();

        Ok(indices)
    }

    /// Fetch expected schema from index template.
    async fn fetch_template_schema(&self) -> Result<Value> {
        let template_name = format!("{}-template", self.index_prefix);
        let url = format!(
            "{}/_index_template/{}",
            self.base_url.trim_end_matches('/'),
            template_name
        );

        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("fetch template")?;

        if !resp.status().is_success() {
            anyhow::bail!("template not found: {}", template_name);
        }

        let body: Value = resp.json().await.context("parse template")?;
        let mappings = body
            .pointer("/index_templates/0/index_template/template/mappings")
            .cloned()
            .unwrap_or(Value::Null);

        Ok(mappings)
    }

    /// Detect drifted indices using monotonic suffix property with almost-sure sampling.
    /// Binary search finds the boundary where clean indices start; O(log n) steps,
    /// each sampling O(log m) positions for almost-sure detection.
    async fn detect_drifted_indices(
        &self,
        indices: &[String],
        expected: &Value,
    ) -> Result<Vec<String>> {
        let n = indices.len();
        if n == 0 {
            return Ok(vec![]);
        }

        info!(
            "schema_heal: checking {} indices for drift using monotonic suffix search",
            n
        );

        // Quick check: is the entire range almost-surely clean?
        if self
            .suffix_is_almost_surely_clean(indices, expected)
            .await?
        {
            info!("schema_heal: entire range is almost-surely clean, no drift detected");
            return Ok(vec![]);
        }

        // Binary search for clean suffix boundary
        let boundary = self
            .binary_search_clean_suffix_start(indices, expected)
            .await?;

        // Everything in [0, boundary) has drift
        let drifted_indices: Vec<String> = indices[..boundary].to_vec();

        info!(
            "schema_heal: found {} drifted indices out of {} (clean suffix starts at index {})",
            drifted_indices.len(),
            n,
            boundary
        );

        Ok(drifted_indices)
    }

    /// Binary search for smallest i where suffix [i, n) passes sampling check.
    async fn binary_search_clean_suffix_start(
        &self,
        indices: &[String],
        expected: &Value,
    ) -> Result<usize> {
        let n = indices.len();
        let mut lo = 0;
        let mut hi = n;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let suffix = &indices[mid..];

            if self.suffix_is_almost_surely_clean(suffix, expected).await? {
                // Suffix [mid, n) is clean, try to find earlier boundary
                hi = mid;
            } else {
                // Suffix [mid, n) has drift, boundary must be after mid
                lo = mid + 1;
            }
        }

        Ok(lo)
    }

    /// Sample O(log m) positions to check if suffix is clean.
    async fn suffix_is_almost_surely_clean(
        &self,
        suffix: &[String],
        expected: &Value,
    ) -> Result<bool> {
        let m = suffix.len();
        if m == 0 {
            return Ok(true);
        }

        // Sample log(m) positions
        let sample_count = ((m as f64).ln().ceil() as usize).max(1);
        let step = m / sample_count.max(1);

        for i in 0..sample_count {
            let pos = (i * step).min(m - 1);
            if self.index_has_drift(&suffix[pos], expected).await? {
                return Ok(false); // Found drift, suffix is NOT clean
            }
        }

        // No drift in sampled positions
        Ok(true)
    }

    /// Check if an index's mapping differs from expected template.
    async fn index_has_drift(&self, index: &str, expected: &Value) -> Result<bool> {
        let actual = self.fetch_index_mapping(index).await?;

        // Extract expected properties
        let expected_props = expected
            .get("properties")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        // Check each expected property exists with correct type
        if let Some(exp_map) = expected_props.as_object() {
            for (field, exp_mapping) in exp_map {
                let actual_field = actual.pointer(&format!("/properties/{}", field));
                match actual_field {
                    None => return Ok(true), // Missing field = drift
                    Some(act) => {
                        // Compare type
                        let exp_type = exp_mapping.get("type");
                        let act_type = act.get("type");
                        if exp_type != act_type {
                            return Ok(true);
                        }
                        // Check for unwanted subfields (like message.keyword)
                        if exp_mapping.get("fields").is_none() && act.get("fields").is_some() {
                            return Ok(true);
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    /// Fetch actual mapping for an index.
    async fn fetch_index_mapping(&self, index: &str) -> Result<Value> {
        let url = format!("{}/{}/_mapping", self.base_url.trim_end_matches('/'), index);

        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("fetch mapping")?;

        if !resp.status().is_success() {
            anyhow::bail!("failed to fetch mapping for {}", index);
        }

        let body: Value = resp.json().await.context("parse mapping")?;
        let mappings = body
            .pointer(&format!("/{}/mappings", index))
            .cloned()
            .unwrap_or(Value::Null);

        Ok(mappings)
    }

    /// Reindex an index to fix schema drift.
    async fn reindex_with_correct_schema(&self, index: &str) -> Result<()> {
        let temp_index = format!("{}-heal-temp", index);

        // Cleanup any leftover temp index from previous attempts
        let _ = self.delete_index(&temp_index).await;

        // Create temp index (will use current template)
        let create_url = format!("{}/{}", self.base_url.trim_end_matches('/'), temp_index);
        let create_resp = self
            .client
            .put(&create_url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("create temp index")?;

        if !create_resp.status().is_success() {
            let status = create_resp.status();
            let body = create_resp.text().await.unwrap_or_default();
            anyhow::bail!("failed to create temp index: {} - {}", status, body);
        }

        // Reindex from old to temp with wait_for_completion
        let reindex_url = format!(
            "{}/_reindex?wait_for_completion=true",
            self.base_url.trim_end_matches('/')
        );
        let reindex_body = serde_json::json!({
            "conflicts": "proceed",
            "source": { "index": index },
            "dest": { "index": temp_index }
        });

        let reindex_resp = self
            .client
            .post(&reindex_url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&reindex_body)
            .send()
            .await
            .context("reindex")?;

        let status = reindex_resp.status();
        let body: Value = reindex_resp.json().await.unwrap_or(Value::Null);

        // Check for failures in the response body (ES returns 200 even with failures)
        let failure_count = body
            .get("failures")
            .and_then(|f| f.as_array())
            .map(|f| f.len() as u64)
            .unwrap_or(0);
        let total = body.get("total").and_then(|t| t.as_u64()).unwrap_or(0);

        // Allow proportional failures - if less than 5% failed, consider it success
        let failure_threshold = (total / 20).max(1); // 5% of total, minimum 1
        if !status.is_success() || (total > 0 && failure_count > failure_threshold) {
            let _ = self.delete_index(&temp_index).await;
            anyhow::bail!(
                "reindex had {} failures out of {} (threshold {})",
                failure_count,
                total,
                failure_threshold
            );
        }
        if failure_count > 0 {
            warn!(
                "schema_heal: {} allowed {} doc failures during reindex",
                index, failure_count
            );
        }

        // Delete old index
        self.delete_index(index).await?;

        // Rename temp to original using reindex (ES doesn't have rename)
        let rename_body = serde_json::json!({
            "conflicts": "proceed",
            "source": { "index": temp_index },
            "dest": { "index": index }
        });

        let rename_resp = self
            .client
            .post(&reindex_url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&rename_body)
            .send()
            .await
            .context("rename reindex")?;

        let rename_status = rename_resp.status();
        let rename_body: Value = rename_resp.json().await.unwrap_or(Value::Null);
        let rename_failures = rename_body.get("failures").and_then(|f| f.as_array());
        if !rename_status.is_success() || rename_failures.map(|f| !f.is_empty()).unwrap_or(false) {
            anyhow::bail!("rename reindex failed");
        }

        // Cleanup temp
        let _ = self.delete_index(&temp_index).await;

        info!("schema_heal: reindexed {} with correct schema", index);
        Ok(())
    }

    async fn delete_index(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), index);
        let resp = self
            .client
            .delete(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("delete index")?;

        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            anyhow::bail!("failed to delete {}", index);
        }
        Ok(())
    }

    fn index_pattern(&self) -> String {
        format!("{}-*", self.index_prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_healer_new() {
        let healer = SchemaHealer::new(
            "http://localhost:9200".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
            "logs".to_string(),
        );

        assert!(healer.is_ok());
        let h = healer.unwrap();
        assert_eq!(h.base_url, "http://localhost:9200");
        assert_eq!(h.index_prefix, "logs");
    }

    #[test]
    fn test_index_pattern() {
        let healer = SchemaHealer::new(
            "http://localhost:9200".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
            "myapp".to_string(),
        )
        .unwrap();

        assert_eq!(healer.index_pattern(), "myapp-*");
    }

    #[test]
    fn test_index_pattern_logs() {
        let healer = SchemaHealer::new(
            "http://localhost:9200".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
            "logs".to_string(),
        )
        .unwrap();

        assert_eq!(healer.index_pattern(), "logs-*");
    }

    #[test]
    fn test_healer_stores_credentials() {
        let healer = SchemaHealer::new(
            "http://es.example.com:9200".to_string(),
            "admin".to_string(),
            "secret123".to_string(),
            Duration::from_secs(60),
            "audit".to_string(),
        )
        .unwrap();

        assert_eq!(healer.user, "admin");
        assert_eq!(healer.pass, "secret123");
        assert_eq!(healer.timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_healer_with_trailing_slash() {
        let healer = SchemaHealer::new(
            "http://localhost:9200/".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
            "logs".to_string(),
        )
        .unwrap();

        // The base_url preserves the trailing slash, but methods should handle it
        assert!(healer.base_url.ends_with('/'));
    }
}
