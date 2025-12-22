use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use reqwest::Method;
use serde::Deserialize;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::config::Config;
use crate::enrich::sanitize_log_group_name;
use crate::es_http::EsHttp;

#[derive(Clone)]
struct EsBootstrapConfig {
    log_groups: Vec<Arc<str>>,
    index_prefix: Arc<str>,
    target_replicas: usize,
    rollover_max_age: Arc<str>,
    rollover_max_primary_shard_size: Arc<str>,
    enable_delete_phase: bool,
    delete_min_age: Arc<str>,
}

#[derive(Clone)]
pub struct EsBootstrapHandle {
    notify: Arc<Notify>,
}

impl EsBootstrapHandle {
    pub fn notify(&self) {
        self.notify.notify_one();
    }

    pub fn notifier(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

pub fn start_es_bootstrap(
    cfg: &Config,
    es_url: Arc<str>,
    es_user: Arc<str>,
    es_pass: Arc<str>,
) -> Option<EsBootstrapHandle> {
    if !cfg.enable_es_bootstrap {
        return None;
    }

    let notify = Arc::new(Notify::new());
    let handle = EsBootstrapHandle {
        notify: notify.clone(),
    };

    let bs_cfg = EsBootstrapConfig {
        log_groups: cfg.log_groups.clone(),
        index_prefix: cfg.index_prefix.clone(),
        target_replicas: cfg.es_target_replicas,
        rollover_max_age: cfg.ilm_rollover_max_age.clone(),
        rollover_max_primary_shard_size: cfg.ilm_rollover_max_primary_shard_size.clone(),
        enable_delete_phase: cfg.ilm_enable_delete_phase,
        delete_min_age: cfg.ilm_delete_min_age.clone(),
    };

    let http = EsHttp::new(es_url, es_user, es_pass, cfg.http_timeout(), true).ok()?;

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(900));
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = notify.notified() => {}
            }

            let res = bootstrap_once(&http, &bs_cfg).await;

            if let Err(err) = res {
                warn!("es bootstrap failed: {err:?}");
            }
        }
    });

    Some(handle)
}

pub async fn bootstrap_now(
    cfg: &Config,
    es_url: Arc<str>,
    es_user: Arc<str>,
    es_pass: Arc<str>,
) -> Result<()> {
    let bs_cfg = EsBootstrapConfig {
        log_groups: cfg.log_groups.clone(),
        index_prefix: cfg.index_prefix.clone(),
        target_replicas: cfg.es_target_replicas,
        rollover_max_age: cfg.ilm_rollover_max_age.clone(),
        rollover_max_primary_shard_size: cfg.ilm_rollover_max_primary_shard_size.clone(),
        enable_delete_phase: cfg.ilm_enable_delete_phase,
        delete_min_age: cfg.ilm_delete_min_age.clone(),
    };
    let http = EsHttp::new(es_url, es_user, es_pass, cfg.http_timeout(), true)?;
    bootstrap_once(&http, &bs_cfg).await
}

#[derive(Deserialize)]
struct ClusterHealth {
    number_of_data_nodes: u64,
}

async fn bootstrap_once(http: &EsHttp, cfg: &EsBootstrapConfig) -> Result<()> {
    let data_nodes = fetch_data_nodes(http).await?;
    let replicas = compute_replicas(cfg.target_replicas, data_nodes);

    let ilm_policy_name = ilm_policy_name(&cfg.index_prefix);
    let template_name = index_template_name(&cfg.index_prefix);

    ensure_ilm_policy(
        http,
        &ilm_policy_name,
        &cfg.rollover_max_age,
        &cfg.rollover_max_primary_shard_size,
        cfg.enable_delete_phase,
        &cfg.delete_min_age,
    )
    .await?;

    ensure_index_template(
        http,
        &template_name,
        &cfg.index_prefix,
        &ilm_policy_name,
        replicas,
    )
    .await?;

    for group in cfg.log_groups.iter() {
        let stable = stable_alias_name(&cfg.index_prefix, group);
        let v1 = versioned_stream_name(&stable, "v1");
        let v2 = versioned_stream_name(&stable, "v2");
        ensure_data_stream(http, &v1).await?;
        ensure_data_stream(http, &v2).await?;
        ensure_alias_exists(http, &stable, &v1).await?;
    }

    info!(
        "es bootstrap ok: data_nodes={} target_replicas={} applied_replicas={} template={} ilm_policy={}",
        data_nodes, cfg.target_replicas, replicas, template_name, ilm_policy_name
    );

    Ok(())
}

async fn fetch_data_nodes(http: &EsHttp) -> Result<u64> {
    let health: ClusterHealth = http
        .get_json("_cluster/health", "es bootstrap health")
        .await?;
    Ok(health.number_of_data_nodes)
}

pub fn ilm_policy_name(index_prefix: &str) -> String {
    format!("logstream-{}-ilm", index_prefix)
}

pub fn index_template_name(index_prefix: &str) -> String {
    format!("logstream-{}-template", index_prefix)
}

pub fn compute_replicas(target_replicas: usize, data_nodes: u64) -> usize {
    let max_replicas = data_nodes.saturating_sub(1) as usize;
    target_replicas.min(max_replicas)
}

async fn ensure_ilm_policy(
    http: &EsHttp,
    policy_name: &str,
    rollover_max_age: &str,
    rollover_max_primary_shard_size: &str,
    enable_delete_phase: bool,
    delete_min_age: &str,
) -> Result<()> {
    let body = build_ilm_policy_body(
        rollover_max_age,
        rollover_max_primary_shard_size,
        enable_delete_phase,
        delete_min_age,
    );
    let _: serde_json::Value = http
        .put_value(
            &format!("_ilm/policy/{}", policy_name),
            &body,
            "es bootstrap ilm policy",
        )
        .await?;

    Ok(())
}

pub fn build_ilm_policy_body(
    rollover_max_age: &str,
    rollover_max_primary_shard_size: &str,
    enable_delete_phase: bool,
    delete_min_age: &str,
) -> serde_json::Value {
    let mut phases = serde_json::json!({
        "hot": {
            "actions": {
                "rollover": {
                    "max_age": rollover_max_age,
                    "max_primary_shard_size": rollover_max_primary_shard_size
                }
            }
        }
    });

    if enable_delete_phase {
        phases["delete"] = serde_json::json!({
            "min_age": delete_min_age,
            "actions": {
                "delete": {}
            }
        });
    }

    serde_json::json!({
        "policy": {
            "phases": phases
        }
    })
}

async fn ensure_index_template(
    http: &EsHttp,
    template_name: &str,
    index_prefix: &str,
    ilm_policy_name: &str,
    replicas: usize,
) -> Result<()> {
    let body = build_index_template_body(index_prefix, ilm_policy_name, replicas);
    let _: serde_json::Value = http
        .put_value(
            &format!("_index_template/{}", template_name),
            &body,
            "es bootstrap index template",
        )
        .await?;

    Ok(())
}

pub fn build_index_template_body(
    index_prefix: &str,
    ilm_policy_name: &str,
    replicas: usize,
) -> serde_json::Value {
    let pattern = format!("{}-*", index_prefix);
    serde_json::json!({
        "index_patterns": [pattern],
        "priority": 600,
        "data_stream": {},
        "template": {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": replicas,
                "index.lifecycle.name": ilm_policy_name
            },
            "mappings": {
                "properties": {
                    "@timestamp": { "type": "date" }
                }
            }
        }
    })
}

fn stable_alias_name(index_prefix: &str, log_group: &str) -> String {
    let slug = sanitize_log_group_name(log_group);
    format!("{index_prefix}-{slug}")
}

fn versioned_stream_name(stable_alias: &str, version: &str) -> String {
    format!("{stable_alias}-{version}")
}

async fn ensure_data_stream(http: &EsHttp, name: &str) -> Result<()> {
    let resp = http
        .request(Method::PUT, &format!("_data_stream/{}", name))
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    if status.as_u16() == 400 {
        // Already exists can show up as 400 depending on ES build; treat as OK if message indicates it exists.
        let text = resp.text().await.unwrap_or_default();
        if text.contains("resource_already_exists_exception") || text.contains("already exists") {
            return Ok(());
        }
        anyhow::bail!(
            "es bootstrap create data stream failed status={} body_sample={}",
            status,
            &text[..text.len().min(500)]
        );
    }
    if status.as_u16() == 409 {
        return Ok(());
    }
    let text = resp.text().await.unwrap_or_default();
    anyhow::bail!(
        "es bootstrap create data stream failed status={} body_sample={}",
        status,
        &text[..text.len().min(500)]
    );
}

async fn reset_alias_to_target(http: &EsHttp, alias: &str, target: &str) -> Result<()> {
    let resp = http
        .request(Method::GET, &format!("_alias/{}", alias))
        .send()
        .await?;

    let mut remove_actions: Vec<serde_json::Value> = Vec::new();
    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        if let Some(obj) = body.as_object() {
            for (idx, _) in obj.iter() {
                remove_actions.push(serde_json::json!({
                    "remove": { "index": idx, "alias": alias }
                }));
            }
        }
    }

    let mut actions = remove_actions;
    actions.push(serde_json::json!({
        "add": { "index": target, "alias": alias, "is_write_index": true }
    }));

    let payload = serde_json::json!({ "actions": actions });
    let _: serde_json::Value = http
        .post_value("_aliases", &payload, "es bootstrap alias update")
        .await?;
    Ok(())
}

async fn ensure_alias_exists(http: &EsHttp, alias: &str, default_target: &str) -> Result<()> {
    let resp = http
        .request(Method::GET, &format!("_alias/{}", alias))
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        let Some(obj) = body.as_object() else {
            return Ok(());
        };
        if !obj.is_empty() {
            // Alias already exists and points somewhere. Do not override: repairs rely on alias cutovers.
            return Ok(());
        }
    }

    reset_alias_to_target(http, alias, default_target).await
}
