//! Elasticsearch index management operations.
//! Handles index cleanup, settings, and maintenance.

use anyhow::Result;
use reqwest::Client;
use std::time::Duration;
use tracing::info;

const INDEX_FIELD_LIMIT: u32 = 10000;

// ============================================================================
// Index Operations
// ============================================================================

/// Drop an index if it exists. Returns Ok if index doesn't exist or was deleted.
pub async fn drop_index_if_exists(
    base_url: &str,
    user: &str,
    pass: &str,
    index: &str,
) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let url = format!("{}/{}", base_url.trim_end_matches('/'), index);
    let resp = client
        .delete(&url)
        .basic_auth(user, Some(pass))
        .send()
        .await?;

    if resp.status().as_u16() == 404 {
        return Ok(());
    }
    if !resp.status().is_success() {
        anyhow::bail!("drop index {} status {}", index, resp.status());
    }
    Ok(())
}

/// Check if an index should be cleaned up based on its name and health.
pub fn should_cleanup_index(name: &str, docs: &str, health: &str) -> bool {
    name.contains("-temp") || (docs == "0" && health == "red") || name.ends_with("-temp")
}

/// Delete temp and broken indices with the given prefix.
pub async fn cleanup_problematic_indices(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<Vec<String>> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    // Get all indices with the target prefix
    let url = format!(
        "{}/_cat/indices/{}-*?format=json",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        return Ok(Vec::new()); // Skip if we can't list indices
    }

    let indices: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
    let mut cleaned = Vec::new();

    for idx in indices {
        let name = idx.get("index").and_then(|v| v.as_str()).unwrap_or("");
        let docs = idx
            .get("docs.count")
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        let health = idx.get("health").and_then(|v| v.as_str()).unwrap_or("");

        if should_cleanup_index(name, docs, health) {
            tracing::info!(
                "cleaning up problematic index: {} (docs={}, health={})",
                name,
                docs,
                health
            );
            if drop_index_if_exists(base_url, user, pass, name)
                .await
                .is_ok()
            {
                cleaned.push(name.to_string());
            }
        }
    }

    Ok(cleaned)
}

/// Configure ES indices for bulk ingest with optimized settings.
pub async fn apply_backfill_settings(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
    refresh_interval: &str,
    replicas: &str,
) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let url = format!(
        "{}/{}-*/_settings",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let body = serde_json::json!({
        "index": {
            "refresh_interval": refresh_interval,
            "number_of_replicas": replicas
        }
    });
    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("apply backfill settings status {}", resp.status());
    }
    Ok(())
}

/// Reset indices to normal settings after backfill.
pub async fn restore_normal_settings(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<()> {
    apply_backfill_settings(base_url, user, pass, index_prefix, "1s", "1").await
}

/// Get index count for a prefix.
pub async fn count_indices(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<usize> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let url = format!(
        "{}/_cat/indices/{}-*?format=json",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        return Ok(0);
    }

    let indices: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
    Ok(indices.len())
}

/// Check if ES cluster is healthy.
pub async fn check_cluster_health(base_url: &str, user: &str, pass: &str) -> Result<String> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let url = format!("{}/_cluster/health", base_url.trim_end_matches('/'));
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("cluster health check failed: {}", resp.status());
    }

    let body: serde_json::Value = resp.json().await?;
    let status = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    Ok(status.to_string())
}

/// Ensure index template exists with appropriate settings including high field limit.
pub async fn ensure_index_template(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
    let template_name = format!("{}-template", index_prefix);
    let url = format!(
        "{}/_index_template/{}",
        base_url.trim_end_matches('/'),
        template_name
    );

    let template_body = serde_json::json!({
        "index_patterns": [format!("{}-*", index_prefix)],
        "priority": 100,
        "template": {
            "settings": {
                "index": {
                    "mapping": {
                        "total_fields": {
                            "limit": INDEX_FIELD_LIMIT
                        },
                        "ignore_malformed": true
                    },
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "dynamic": "true",
                "properties": {
                    "@timestamp": { "type": "date" },
                    "timestamp": { "type": "date" },
                    "message": { "type": "text" },
                    "level": { "type": "keyword" },
                    "service_name": { "type": "keyword" },
                    "log_group": { "type": "keyword" },
                    "log_stream": { "type": "keyword" },
                    "event_id": { "type": "keyword" },
                    "tags": { "type": "keyword" }
                }
            }
        }
    });

    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .json(&template_body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "failed to create index template {}: {} - {}",
            template_name,
            status,
            body
        );
    }

    info!(
        "index template {} ensured with field_limit={}",
        template_name, INDEX_FIELD_LIMIT
    );
    Ok(())
}

/// Update field limit on existing indices.
pub async fn update_existing_indices_field_limit(
    base_url: &str,
    user: &str,
    pass: &str,
    index_prefix: &str,
) -> Result<usize> {
    let client = Client::builder().timeout(Duration::from_secs(60)).build()?;

    let url = format!(
        "{}/{}-*/_settings",
        base_url.trim_end_matches('/'),
        index_prefix
    );
    let body = serde_json::json!({
        "index": {
            "mapping": {
                "total_fields": {
                    "limit": INDEX_FIELD_LIMIT
                }
            }
        }
    });

    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await?;

    if resp.status().as_u16() == 404 {
        return Ok(0);
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "failed to update field limit on existing indices: {} - {}",
            status,
            body_text
        );
    }

    let count = count_indices(base_url, user, pass, index_prefix).await?;
    info!(
        "updated field_limit={} on {} existing indices",
        INDEX_FIELD_LIMIT, count
    );
    Ok(count)
}
