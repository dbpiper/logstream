//! Elasticsearch index management operations.
//! Handles index cleanup, settings, and maintenance.

use anyhow::Result;
use reqwest::Client;
use std::time::Duration;

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

pub async fn ensure_index_refresh_interval(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index: &str,
    refresh_interval: &str,
) -> Result<bool> {
    let current = get_index_refresh_interval(client, base_url, user, pass, index).await?;
    if current.as_deref() == Some(refresh_interval) {
        return Ok(false);
    }
    set_index_refresh_interval(client, base_url, user, pass, index, refresh_interval).await
}

async fn get_index_refresh_interval(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index: &str,
) -> Result<Option<String>> {
    let url = format!(
        "{}/{}/_settings?filter_path=**.refresh_interval",
        base_url.trim_end_matches('/'),
        index
    );
    let resp = client.get(&url).basic_auth(user, Some(pass)).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    let interval = v
        .as_object()
        .and_then(|o| o.values().next())
        .and_then(|x| x.pointer("/settings/index/refresh_interval"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    Ok(interval)
}

async fn set_index_refresh_interval(
    client: &Client,
    base_url: &str,
    user: &str,
    pass: &str,
    index: &str,
    refresh_interval: &str,
) -> Result<bool> {
    let url = format!("{}/{}/_settings", base_url.trim_end_matches('/'), index);
    let body = serde_json::json!({
        "index": {
            "refresh_interval": refresh_interval
        }
    });
    let resp = client
        .put(&url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await?;
    if resp.status().as_u16() == 404 {
        // Backing indices can roll over or be deleted between discovery and update.
        return Ok(false);
    }
    if !resp.status().is_success() {
        anyhow::bail!(
            "set refresh interval index={} status={}",
            index,
            resp.status()
        );
    }
    Ok(true)
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
