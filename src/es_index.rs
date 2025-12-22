//! Elasticsearch index management operations.
//! Handles index cleanup, settings, and maintenance.

use crate::es_http::EsHttp;
use anyhow::Result;

// ============================================================================
// Index Operations
// ============================================================================

/// Drop an index if it exists. Returns Ok if index doesn't exist or was deleted.
pub async fn drop_index_if_exists(http: &EsHttp, index: &str) -> Result<()> {
    http.delete_allow_404(index, "drop index").await?;
    Ok(())
}

/// Check if an index should be cleaned up based on its name and health.
pub fn should_cleanup_index(name: &str, docs: &str, health: &str) -> bool {
    name.contains("-temp") || (docs == "0" && health == "red") || name.ends_with("-temp")
}

/// Delete temp and broken indices with the given prefix.
pub async fn cleanup_problematic_indices(http: &EsHttp, index_prefix: &str) -> Result<Vec<String>> {
    // Get all indices with the target prefix
    let indices: Vec<serde_json::Value> = match http
        .get_json(
            &format!("_cat/indices/{}-*?format=json", index_prefix),
            "cat indices request",
        )
        .await
    {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
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
            if drop_index_if_exists(http, name).await.is_ok() {
                cleaned.push(name.to_string());
            }
        }
    }

    Ok(cleaned)
}

/// Configure ES indices for bulk ingest with optimized settings.
pub async fn apply_backfill_settings(
    http: &EsHttp,
    index_prefix: &str,
    refresh_interval: &str,
    replicas: &str,
) -> Result<()> {
    let body = serde_json::json!({
        "index": {
            "refresh_interval": refresh_interval,
            "number_of_replicas": replicas
        }
    });
    let _: serde_json::Value = http
        .put_value(
            &format!("{}-*/_settings", index_prefix),
            &body,
            "apply backfill settings",
        )
        .await?;
    Ok(())
}

pub async fn ensure_index_refresh_interval(
    http: &EsHttp,
    index: &str,
    refresh_interval: &str,
) -> Result<bool> {
    let current = get_index_refresh_interval(http, index).await?;
    if current.as_deref() == Some(refresh_interval) {
        return Ok(false);
    }
    set_index_refresh_interval(http, index, refresh_interval).await
}

async fn get_index_refresh_interval(http: &EsHttp, index: &str) -> Result<Option<String>> {
    let resp = http
        .request(
            reqwest::Method::GET,
            &format!("{}/_settings?filter_path=**.refresh_interval", index),
        )
        .send()
        .await?;
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
    http: &EsHttp,
    index: &str,
    refresh_interval: &str,
) -> Result<bool> {
    let body = serde_json::json!({
        "index": {
            "refresh_interval": refresh_interval
        }
    });
    let resp = http
        .send_expect(
            http.request(reqwest::Method::PUT, &format!("{}/_settings", index))
                .json(&body),
            "set refresh interval",
            |s| s.is_success() || s.as_u16() == 404,
        )
        .await?;
    if resp.status().as_u16() == 404 {
        // Backing indices can roll over or be deleted between discovery and update.
        return Ok(false);
    }
    Ok(true)
}

/// Reset indices to normal settings after backfill.
pub async fn restore_normal_settings(http: &EsHttp, index_prefix: &str) -> Result<()> {
    apply_backfill_settings(http, index_prefix, "1s", "1").await
}

/// Check if ES cluster is healthy.
pub async fn check_cluster_health(http: &EsHttp) -> Result<String> {
    let body: serde_json::Value = http
        .get_value("_cluster/health", "cluster health check")
        .await?;
    let status = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    Ok(status.to_string())
}
