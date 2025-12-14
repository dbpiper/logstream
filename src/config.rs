use std::sync::Arc;
use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::Result;
use directories::ProjectDirs;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Config {
    pub log_groups: Vec<Arc<str>>,
    pub log_group: Arc<str>,
    pub region: Arc<str>,
    pub index_prefix: Arc<str>,
    pub batch_size: usize,
    pub max_in_flight: usize,
    pub poll_interval_secs: u64,
    pub reconcile_interval_secs: u64,
    pub backfill_days: u32,
    pub checkpoint_path: PathBuf,
    pub http_timeout_secs: u64,
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default)]
    log_groups: Vec<String>,
    region: String,
    index_prefix: String,
    batch_size: usize,
    max_in_flight: usize,
    poll_interval_secs: u64,
    reconcile_interval_secs: u64,
    backfill_days: u32,
    checkpoint_path: PathBuf,
    http_timeout_secs: u64,
    backoff_base_ms: u64,
    backoff_max_ms: u64,
}

impl From<RawConfig> for Config {
    fn from(raw: RawConfig) -> Self {
        let log_groups: Vec<Arc<str>> = collect_log_groups(raw.log_groups);
        let log_group = log_groups.first().cloned().unwrap_or_else(|| Arc::from(""));
        Self {
            log_groups,
            log_group,
            region: raw.region.into(),
            index_prefix: raw.index_prefix.into(),
            batch_size: raw.batch_size,
            max_in_flight: raw.max_in_flight,
            poll_interval_secs: raw.poll_interval_secs,
            reconcile_interval_secs: raw.reconcile_interval_secs,
            backfill_days: raw.backfill_days,
            checkpoint_path: raw.checkpoint_path,
            http_timeout_secs: raw.http_timeout_secs,
            backoff_base_ms: raw.backoff_base_ms,
            backoff_max_ms: raw.backoff_max_ms,
        }
    }
}

impl Config {
    pub fn load(path: Option<PathBuf>) -> Result<Self> {
        let mut cfg = if let Some(path) = path {
            let raw = fs::read_to_string(path)?;
            Config::from(toml::from_str::<RawConfig>(&raw)?)
        } else {
            let default_path = default_config_path();
            if default_path.exists() {
                let raw = fs::read_to_string(&default_path)?;
                Config::from(toml::from_str::<RawConfig>(&raw)?)
            } else {
                Self::default_from_env()?
            }
        };

        if let Ok(groups) = env::var("LOG_GROUPS") {
            let parsed = parse_log_groups(&groups);
            if let Some(first) = parsed.first() {
                cfg.log_group = first.clone();
                cfg.log_groups = parsed;
            }
        }
        if let Ok(v) = env::var("AWS_REGION") {
            cfg.region = v.into();
        }
        if let Ok(v) = env::var("INDEX_PREFIX") {
            cfg.index_prefix = v.into();
        }
        maybe_env_usize(&mut cfg.batch_size, "BATCH_SIZE");
        maybe_env_usize(&mut cfg.max_in_flight, "MAX_IN_FLIGHT");
        maybe_env_u64(&mut cfg.poll_interval_secs, "POLL_INTERVAL_SECS");
        maybe_env_u64(&mut cfg.reconcile_interval_secs, "RECONCILE_INTERVAL_SECS");
        maybe_env_u64(&mut cfg.http_timeout_secs, "HTTP_TIMEOUT_SECS");
        maybe_env_u64(&mut cfg.backoff_base_ms, "BACKOFF_BASE_MS");
        maybe_env_u64(&mut cfg.backoff_max_ms, "BACKOFF_MAX_MS");
        if let Ok(v) = env::var("BACKFILL_DAYS") {
            if let Ok(n) = v.parse::<u32>() {
                cfg.backfill_days = n;
            }
        }
        if let Ok(p) = env::var("CHECKPOINT_PATH") {
            cfg.checkpoint_path = PathBuf::from(p);
        }
        validate_required(&cfg)?;
        Ok(cfg)
    }

    pub fn http_timeout(&self) -> Duration {
        Duration::from_secs(self.http_timeout_secs)
    }
}

impl Config {
    fn default_from_env() -> Result<Self> {
        let dirs = default_state_dir();
        let checkpoint_path = dirs.join("checkpoints.json");
        let groups = parse_log_groups(&env_required("LOG_GROUPS")?);
        let primary_group = groups.first().cloned().unwrap_or_else(|| Arc::from(""));
        Ok(Self {
            log_groups: groups,
            log_group: primary_group,
            region: env_required("AWS_REGION")?.into(),
            index_prefix: env::var("INDEX_PREFIX")
                .unwrap_or_else(|_| "cloudwatch".into())
                .into(),
            batch_size: env_usize("BATCH_SIZE", 100),
            max_in_flight: env_usize("MAX_IN_FLIGHT", 2),
            poll_interval_secs: env_u64("POLL_INTERVAL_SECS", 15),
            reconcile_interval_secs: env_u64("RECONCILE_INTERVAL_SECS", 900),
            backfill_days: env_u64("BACKFILL_DAYS", 30) as u32,
            checkpoint_path,
            http_timeout_secs: env_u64("HTTP_TIMEOUT_SECS", 30),
            backoff_base_ms: env_u64("BACKOFF_BASE_MS", 200),
            backoff_max_ms: env_u64("BACKOFF_MAX_MS", 10_000),
        })
    }
}

impl Config {
    pub fn effective_log_groups(&self) -> Vec<Arc<str>> {
        self.log_groups.clone()
    }

    pub fn with_log_group(&self, group: Arc<str>, checkpoint_path: PathBuf) -> Self {
        let mut c = self.clone();
        c.log_group = group;
        c.checkpoint_path = checkpoint_path;
        c
    }
}

fn default_config_path() -> PathBuf {
    default_state_dir().join("config.toml")
}

fn default_state_dir() -> PathBuf {
    ProjectDirs::from("com", "dbpiper", "logstream")
        .map(|p| p.config_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from(".logstream"))
}

fn validate_required(cfg: &Config) -> Result<()> {
    if cfg.log_groups.is_empty() || cfg.log_group.trim().is_empty() {
        anyhow::bail!("LOG_GROUPS is required (set via env or config)");
    }
    if cfg.region.trim().is_empty() {
        anyhow::bail!("AWS_REGION is required (set via env or config)");
    }
    if cfg.index_prefix.trim().is_empty() {
        anyhow::bail!("INDEX_PREFIX is required (set via env or config)");
    }
    Ok(())
}

fn maybe_env_usize(val: &mut usize, key: &str) {
    if let Ok(v) = env::var(key) {
        if let Ok(n) = v.parse::<usize>() {
            *val = n;
        }
    }
}

fn maybe_env_u64(val: &mut u64, key: &str) {
    if let Ok(v) = env::var(key) {
        if let Ok(n) = v.parse::<u64>() {
            *val = n;
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_required(key: &str) -> Result<String> {
    let val = env::var(key).unwrap_or_default();
    if val.trim().is_empty() {
        anyhow::bail!("{key} is required");
    }
    Ok(val)
}

fn collect_log_groups(groups: Vec<String>) -> Vec<Arc<str>> {
    groups
        .into_iter()
        .map(|g| g.trim().to_string())
        .filter(|g| !g.is_empty())
        .map(Arc::from)
        .collect()
}

fn parse_log_groups(raw: &str) -> Vec<Arc<str>> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(Arc::from)
        .collect()
}
