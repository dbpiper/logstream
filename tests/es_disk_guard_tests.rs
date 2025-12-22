use std::sync::Arc;
use std::time::Duration;

use logstream::config::Config;
use logstream::es_disk_guard::{start_es_disk_guard, EsDiskGuardConfig};
use logstream::prune_state::PruneState;
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn minimal_cfg(tmp: &TempDir) -> Config {
    Config {
        log_groups: vec![
            "/ecs/example-service".into(),
            "/ecs/example-service-worker".into(),
        ],
        log_group: "/ecs/example-service".into(),
        region: "us-east-1".into(),
        index_prefix: "cloudwatch".into(),
        enable_es_bootstrap: true,
        es_target_replicas: 0,
        ilm_rollover_max_age: "1d".into(),
        ilm_rollover_max_primary_shard_size: "25gb".into(),
        ilm_enable_delete_phase: false,
        ilm_delete_min_age: "30d".into(),
        batch_size: 100,
        max_in_flight: 2,
        poll_interval_secs: 15,
        reconcile_interval_secs: 900,
        backfill_days: 30,
        checkpoint_path: tmp.path().join("checkpoints.json"),
        http_timeout_secs: 5,
        backoff_base_ms: 200,
        backoff_max_ms: 10000,
    }
}

#[tokio::test]
async fn disk_guard_deletes_oldest_backing_index_when_below_high_watermark() {
    let server = MockServer::start().await;
    let tmp = TempDir::new().unwrap();
    let cfg = minimal_cfg(&tmp);

    // Report disk pressure (available less than high watermark required).
    Mock::given(method("GET"))
        .and(path("/_nodes/stats/fs"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{
              "nodes": {
                "n1": { "fs": { "data": [ { "available_in_bytes": 1, "high_watermark_free_space_in_bytes": 2 } ] } }
              }
            }"#,
        ))
        .mount(&server)
        .await;

    // Data stream backing indices: oldest then newest(write).
    Mock::given(method("GET"))
        .and(path_regex("/_data_stream/.*cloudwatch-ecs-example-service-v1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"data_streams":[{"indices":[{"index_name":".ds-cloudwatch-ecs-example-service-v1-2025.12.01-000001"},{"index_name":".ds-cloudwatch-ecs-example-service-v1-2025.12.22-000002"}]}]}"#,
        ))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex("/_data_stream/.*cloudwatch-ecs-example-service-v2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"data_streams":[{"indices":[{"index_name":".ds-cloudwatch-ecs-example-service-v2-2025.12.22-000001"}]}]}"#,
        ))
        .mount(&server)
        .await;

    // max_ts-by-index aggregation for v1 stream marks the first backing index as old (ts=0).
    Mock::given(method("POST"))
        .and(path("/cloudwatch-ecs-example-service-v1/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"aggregations":{"by_index":{"buckets":[{"key":".ds-cloudwatch-ecs-example-service-v1-2025.12.01-000001","max_ts":{"value":0}},{"key":".ds-cloudwatch-ecs-example-service-v1-2025.12.22-000002","max_ts":{"value":9999999999999}}]}}}"#,
        ))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/cloudwatch-ecs-example-service-v2/_search"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"aggregations":{"by_index":{"buckets":[]}}}"#),
        )
        .mount(&server)
        .await;

    // For other groups, return empty data stream responses.
    Mock::given(method("GET"))
        .and(path_regex(
            "/_data_stream/.*cloudwatch-ecs-example-service-worker-.*",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"data_streams":[{"indices":[]}]} "#),
        )
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path_regex(
            "/cloudwatch-ecs-example-service-worker-.*_search",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"aggregations":{"by_index":{"buckets":[]}}}"#),
        )
        .mount(&server)
        .await;

    // Expect the old backing index to be deleted.
    Mock::given(method("DELETE"))
        .and(path(
            "/.ds-cloudwatch-ecs-example-service-v1-2025.12.01-000001",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"acknowledged":true}"#))
        .mount(&server)
        .await;

    // Run one tick quickly.
    let prune_state_path = cfg.checkpoint_path.with_file_name("prune_watermarks.json");
    let prune_state = Arc::new(PruneState::load_or_new(prune_state_path).unwrap());
    start_es_disk_guard(
        cfg,
        server.uri().into(),
        "user".into(),
        "pass".into(),
        Duration::from_secs(5),
        EsDiskGuardConfig {
            enabled: true,
            interval_secs: 1,
            free_buffer_gb: 0,
            min_keep_days: 7,
        },
        prune_state.clone(),
    );

    // Give the background task time to tick once.
    let stable = "cloudwatch-ecs-example-service";
    let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
    loop {
        if prune_state.min_supported_ms(stable).await > 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Confirm prune watermark was advanced for the group stable alias.
    let min_ms = prune_state.min_supported_ms(stable).await;
    assert!(min_ms > 0);
}
