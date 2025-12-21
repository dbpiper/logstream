use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::info;

use logstream::config::Config;
use logstream::enrich::sanitize_log_group_name;
use logstream::es_repair::EsRepairClient;

pub async fn needs_migration(
    cfg: &Config,
    es_url: &str,
    es_user: &str,
    es_pass: &str,
) -> Result<bool> {
    let client = Client::builder().timeout(cfg.http_timeout()).build()?;
    for group in cfg.effective_log_groups() {
        let slug = sanitize_log_group_name(&group);
        let pattern = format!("{}-{}-*", cfg.index_prefix, slug);
        let url = format!(
            "{}/_cat/indices/{}?format=json&h=index&s=index",
            es_url.trim_end_matches('/'),
            pattern
        );
        let resp = client
            .get(&url)
            .basic_auth(es_user, Some(es_pass))
            .send()
            .await
            .context("cat indices")?;

        if !resp.status().is_success() {
            continue;
        }
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        let Some(items) = body.as_array() else {
            continue;
        };
        let has_any_daily = items.iter().any(|item| {
            item.get("index")
                .and_then(|v| v.as_str())
                .is_some_and(|idx| day_window_from_index_suffix(idx).is_some())
        });
        if has_any_daily {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn run_migrate_daily_indices(
    cfg: &Config,
    es_url: Arc<str>,
    es_user: Arc<str>,
    es_pass: Arc<str>,
) -> Result<()> {
    let client = Client::builder().timeout(cfg.http_timeout()).build()?;
    let repair = EsRepairClient::new(
        es_url.clone(),
        es_user.clone(),
        es_pass.clone(),
        cfg.http_timeout(),
    )?;

    for group in cfg.effective_log_groups() {
        let slug = sanitize_log_group_name(&group);
        let stable = format!("{}-{}", cfg.index_prefix, slug);
        let v1 = format!("{stable}-v1");
        let dest_stream = repair
            .resolve_alias_single_target(&stable)
            .await?
            .unwrap_or_else(|| v1.clone());

        let pattern = format!("{}-{}-*", cfg.index_prefix, slug);
        let url = format!(
            "{}/_cat/indices/{}?format=json&s=index",
            es_url.trim_end_matches('/'),
            pattern
        );
        let resp = client
            .get(&url)
            .basic_auth(&*es_user, Some(&*es_pass))
            .send()
            .await
            .context("cat indices")?;

        if !resp.status().is_success() {
            continue;
        }

        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        let Some(items) = body.as_array() else {
            continue;
        };

        for item in items {
            let Some(index) = item.get("index").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some((start_ms, end_ms)) = day_window_from_index_suffix(index) else {
                continue;
            };

            repair.ensure_data_stream(&dest_stream).await?;
            repair
                .reindex_window(index, &dest_stream, start_ms, end_ms)
                .await
                .with_context(|| format!("reindex {}", index))?;

            let ok = repair
                .verify_window_equivalent(index, &dest_stream, start_ms, end_ms, 64)
                .await
                .with_context(|| format!("verify {}", index))?;
            if !ok {
                anyhow::bail!("migration verify failed index={}", index);
            }

            info!("migration: deleting legacy index {}", index);
            let delete_url = format!("{}/{}", es_url.trim_end_matches('/'), index);
            let del = client
                .delete(&delete_url)
                .basic_auth(&*es_user, Some(&*es_pass))
                .send()
                .await
                .context("delete legacy index")?;
            if !del.status().is_success() && del.status().as_u16() != 404 {
                anyhow::bail!(
                    "migration: failed to delete legacy index {} status={}",
                    index,
                    del.status()
                );
            }
        }
    }

    Ok(())
}

fn day_window_from_index_suffix(index: &str) -> Option<(i64, i64)> {
    let suffix = index.rsplit_once('-')?.1;
    let day = chrono::NaiveDate::parse_from_str(suffix, "%Y.%m.%d").ok()?;
    let start = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        day.and_hms_opt(0, 0, 0)?,
        chrono::Utc,
    )
    .timestamp_millis();
    let end = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        (day + chrono::Duration::days(1)).and_hms_opt(0, 0, 0)?,
        chrono::Utc,
    )
    .timestamp_millis();
    Some((start, end))
}
