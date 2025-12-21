use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use tracing::{info, warn};

use crate::config::Config;
use crate::enrich::sanitize_log_group_name;
use crate::es_index::ensure_index_refresh_interval;
use crate::es_window::EsWindowClient;

#[derive(Debug, Clone)]
pub struct RefreshTunerConfig {
    pub hot_refresh_interval: Arc<str>,
    pub cold_refresh_interval: Arc<str>,
    pub cold_age_days: u64,
    pub interval_secs: u64,
}

pub fn start_refresh_tuner(
    cfg: Config,
    es_url: Arc<str>,
    es_user: Arc<str>,
    es_pass: Arc<str>,
    timeout: Duration,
    tune_cfg: RefreshTunerConfig,
) {
    tokio::spawn(async move {
        let client = Client::builder().timeout(timeout).build();
        let window = EsWindowClient::new(es_url.clone(), es_user.clone(), es_pass.clone(), timeout);
        let (Ok(client), Ok(window)) = (client, window) else {
            warn!("refresh_tuner: failed to create clients");
            return;
        };

        let mut interval = tokio::time::interval(Duration::from_secs(tune_cfg.interval_secs));
        loop {
            interval.tick().await;
            if let Err(err) = tune_once(
                &client, &window, &cfg, &es_url, &es_user, &es_pass, &tune_cfg,
            )
            .await
            {
                warn!("refresh_tuner: tune failed: {err:?}");
            }
        }
    });
}

async fn tune_once(
    client: &Client,
    window: &EsWindowClient,
    cfg: &Config,
    es_url: &str,
    es_user: &str,
    es_pass: &str,
    tune_cfg: &RefreshTunerConfig,
) -> anyhow::Result<()> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let cutoff_ms = now_ms.saturating_sub((tune_cfg.cold_age_days as i64) * 86_400_000);

    for group in cfg.effective_log_groups() {
        let slug = sanitize_log_group_name(&group);
        let stable = format!("{}-{}", cfg.index_prefix, slug);
        let v1 = format!("{stable}-v1");
        let v2 = format!("{stable}-v2");

        for stream in [v1, v2] {
            let max_by_index = match window.backing_index_max_timestamp_ms(&stream).await {
                Ok(v) => v,
                Err(err) => {
                    warn!("refresh_tuner: stream={stream} max_ts_by_index failed: {err:?}");
                    continue;
                }
            };

            let indices = match window.data_stream_backing_indices(&stream).await {
                Ok(v) => v,
                Err(err) => {
                    warn!("refresh_tuner: stream={stream} list backing indices failed: {err:?}");
                    continue;
                }
            };

            for index in indices {
                let desired = desired_refresh_interval(
                    max_by_index.get(&index).copied(),
                    cutoff_ms,
                    &tune_cfg.hot_refresh_interval,
                    &tune_cfg.cold_refresh_interval,
                );
                let changed = ensure_index_refresh_interval(
                    client, es_url, es_user, es_pass, &index, desired,
                )
                .await?;
                if changed {
                    info!(
                        "refresh_tuner: set index={} refresh_interval={}",
                        index, desired
                    );
                }
            }
        }
    }

    Ok(())
}

fn desired_refresh_interval<'a>(
    max_ts_ms: Option<i64>,
    cutoff_ms: i64,
    hot: &'a str,
    cold: &'a str,
) -> &'a str {
    match max_ts_ms {
        None => hot,
        Some(max_ms) if max_ms >= cutoff_ms => hot,
        Some(_) => cold,
    }
}

pub fn is_hot_index(max_ts_ms: Option<i64>, cutoff_ms: i64) -> bool {
    match max_ts_ms {
        None => true,
        Some(max_ms) => max_ms >= cutoff_ms,
    }
}
