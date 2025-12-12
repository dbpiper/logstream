//! Tests for configuration loading.

use logstream::config::Config;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
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
