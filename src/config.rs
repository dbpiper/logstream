use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::Result;
use directories::ProjectDirs;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_group: String,
    #[serde(default)]
    pub log_groups: Vec<String>,
    pub region: String,
    pub index_prefix: String,
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

impl Config {
    pub fn load(path: Option<PathBuf>) -> Result<Self> {
        let mut cfg = if let Some(path) = path {
            let raw = fs::read_to_string(path)?;
            toml::from_str::<Config>(&raw)?
        } else {
            // Fallback to default path if present
            let default_path = default_config_path();
            if default_path.exists() {
                let raw = fs::read_to_string(&default_path)?;
                toml::from_str::<Config>(&raw)?
            } else {
                Self::default_from_env()?
            }
        };

        // Env overrides
        maybe_env(&mut cfg.log_group, "LOG_GROUP");
        if let Ok(groups) = env::var("LOG_GROUPS") {
            let parsed: Vec<String> = groups
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !parsed.is_empty() {
                cfg.log_groups = parsed;
            }
        }
        maybe_env(&mut cfg.region, "AWS_REGION");
        maybe_env(&mut cfg.index_prefix, "INDEX_PREFIX");
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
        Ok(Self {
            log_group: env_required("LOG_GROUP")?,
            region: env_required("AWS_REGION")?,
            index_prefix: env::var("INDEX_PREFIX").unwrap_or_else(|_| "logs".into()),
            batch_size: env_usize("BATCH_SIZE", 100),
            max_in_flight: env_usize("MAX_IN_FLIGHT", 2),
            poll_interval_secs: env_u64("POLL_INTERVAL_SECS", 15),
            reconcile_interval_secs: env_u64("RECONCILE_INTERVAL_SECS", 900),
            backfill_days: env_u64("BACKFILL_DAYS", 30) as u32,
            checkpoint_path,
            http_timeout_secs: env_u64("HTTP_TIMEOUT_SECS", 30),
            backoff_base_ms: env_u64("BACKOFF_BASE_MS", 200),
            backoff_max_ms: env_u64("BACKOFF_MAX_MS", 10_000),
            log_groups: vec![],
        })
    }
}

impl Config {
    pub fn effective_log_groups(&self) -> Vec<String> {
        if !self.log_groups.is_empty() {
            self.log_groups.clone()
        } else {
            vec![self.log_group.clone()]
        }
    }

    pub fn with_log_group(&self, group: String, checkpoint_path: PathBuf) -> Self {
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
    if cfg.log_group.trim().is_empty() {
        anyhow::bail!("LOG_GROUP is required (set via env or config)");
    }
    if cfg.region.trim().is_empty() {
        anyhow::bail!("AWS_REGION is required (set via env or config)");
    }
    if cfg.index_prefix.trim().is_empty() {
        anyhow::bail!("INDEX_PREFIX is required (set via env or config)");
    }
    Ok(())
}

fn maybe_env(val: &mut String, key: &str) {
    if let Ok(v) = env::var(key) {
        *val = v;
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn sample_config_toml() -> &'static str {
        r#"
log_group = "/ecs/test-service"
log_groups = []
region = "us-east-1"
index_prefix = "logs"
batch_size = 1000
max_in_flight = 4
poll_interval_secs = 10
reconcile_interval_secs = 600
backfill_days = 30
checkpoint_path = "/tmp/checkpoints.json"
http_timeout_secs = 60
backoff_base_ms = 100
backoff_max_ms = 5000
"#
    }

    #[test]
    fn test_load_from_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, sample_config_toml()).unwrap();

        let cfg = Config::load(Some(path)).unwrap();
        assert_eq!(cfg.log_group, "/ecs/test-service");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.index_prefix, "logs");
        assert_eq!(cfg.batch_size, 1000);
        assert_eq!(cfg.max_in_flight, 4);
        assert_eq!(cfg.poll_interval_secs, 10);
        assert_eq!(cfg.backfill_days, 30);
    }

    #[test]
    fn test_effective_log_groups_single() {
        let cfg = Config {
            log_group: "/ecs/main".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let groups = cfg.effective_log_groups();
        assert_eq!(groups, vec!["/ecs/main".to_string()]);
    }

    #[test]
    fn test_effective_log_groups_multiple() {
        let cfg = Config {
            log_group: "/ecs/main".to_string(),
            log_groups: vec!["/ecs/svc1".to_string(), "/ecs/svc2".to_string()],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let groups = cfg.effective_log_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"/ecs/svc1".to_string()));
        assert!(groups.contains(&"/ecs/svc2".to_string()));
    }

    #[test]
    fn test_with_log_group() {
        let cfg = Config {
            log_group: "/ecs/original".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/original.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let new_cfg = cfg.with_log_group("/ecs/new".to_string(), PathBuf::from("/tmp/new.json"));
        assert_eq!(new_cfg.log_group, "/ecs/new");
        assert_eq!(new_cfg.checkpoint_path, PathBuf::from("/tmp/new.json"));
        // Original unchanged
        assert_eq!(cfg.log_group, "/ecs/original");
    }

    #[test]
    fn test_http_timeout() {
        let cfg = Config {
            log_group: "/ecs/test".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 45,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        assert_eq!(cfg.http_timeout(), Duration::from_secs(45));
    }

    #[test]
    fn test_validate_required_empty_log_group() {
        let cfg = Config {
            log_group: "   ".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let result = validate_required(&cfg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("LOG_GROUP"));
    }

    #[test]
    fn test_validate_required_empty_region() {
        let cfg = Config {
            log_group: "/ecs/test".to_string(),
            log_groups: vec![],
            region: "".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let result = validate_required(&cfg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("AWS_REGION"));
    }

    #[test]
    fn test_validate_required_empty_index_prefix() {
        let cfg = Config {
            log_group: "/ecs/test".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "  ".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        let result = validate_required(&cfg);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("INDEX_PREFIX"));
    }

    #[test]
    fn test_validate_required_success() {
        let cfg = Config {
            log_group: "/ecs/test".to_string(),
            log_groups: vec![],
            region: "us-east-1".to_string(),
            index_prefix: "logs".to_string(),
            batch_size: 100,
            max_in_flight: 2,
            poll_interval_secs: 15,
            reconcile_interval_secs: 900,
            backfill_days: 30,
            checkpoint_path: PathBuf::from("/tmp/cp.json"),
            http_timeout_secs: 30,
            backoff_base_ms: 200,
            backoff_max_ms: 10000,
        };

        assert!(validate_required(&cfg).is_ok());
    }

    #[test]
    fn test_env_usize_with_default() {
        // Unset env var should return default
        let result = env_usize("NONEXISTENT_VAR_12345", 42);
        assert_eq!(result, 42);
    }

    #[test]
    fn test_env_u64_with_default() {
        let result = env_u64("NONEXISTENT_VAR_67890", 100);
        assert_eq!(result, 100);
    }
}
