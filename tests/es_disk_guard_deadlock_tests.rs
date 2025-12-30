use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logstream::config::Config;
use logstream::es_disk_guard::{run_disk_guard_once, EsDiskGuardConfig};
use logstream::es_http::EsHttp;
use logstream::es_window::EsWindowClient;
use logstream::prune_state::PruneState;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn build_test_config(log_group: &str, index_prefix: &str, checkpoint_path: PathBuf) -> Config {
    Config {
        log_groups: vec![Arc::<str>::from(log_group)],
        log_group: Arc::<str>::from(log_group),
        region: Arc::<str>::from("us-east-1"),
        index_prefix: Arc::<str>::from(index_prefix),
        enable_es_bootstrap: false,
        es_target_replicas: 0,
        ilm_rollover_max_age: Arc::<str>::from("1d"),
        ilm_rollover_max_primary_shard_size: Arc::<str>::from("25gb"),
        ilm_enable_delete_phase: true,
        ilm_delete_min_age: Arc::<str>::from("30d"),
        batch_size: 100,
        max_in_flight: 1,
        poll_interval_secs: 1,
        reconcile_interval_secs: 60,
        backfill_days: 0,
        checkpoint_path,
        http_timeout_secs: 5,
        backoff_base_ms: 20,
        backoff_max_ms: 500,
    }
}

#[tokio::test]
async fn disk_guard_breaks_watermark_deadlock_even_when_metadata_calls_fail() {
    let mock_server = MockServer::start().await;

    let low_disk_body = serde_json::json!({
        "nodes": {
            "node-1": {
                "fs": {
                    "data": [{
                        "available_in_bytes": 10_u64,
                        "high_watermark_free_space_in_bytes": 100_u64
                    }]
                }
            }
        }
    });
    Mock::given(method("GET"))
        .and(path("/_nodes/stats/fs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(low_disk_body))
        .mount(&mock_server)
        .await;

    let stable = "cloudwatch-ecs-test";
    let v1 = format!("{stable}-v1");
    let backing_indices = [
        format!(".ds-{v1}-2025.12.25-000001"),
        format!(".ds-{v1}-2025.12.26-000002"),
        format!(".ds-{v1}-2025.12.27-000003"),
    ];
    let data_stream_body = serde_json::json!({
        "data_streams": [{
            "indices": backing_indices
                .iter()
                .map(|idx| serde_json::json!({"index_name": idx}))
                .collect::<Vec<_>>()
        }]
    });
    Mock::given(method("GET"))
        .and(path(format!("/_data_stream/{v1}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(data_stream_body))
        .mount(&mock_server)
        .await;

    // v2 not found: build_delete_candidates should ignore it.
    Mock::given(method("GET"))
        .and(path(format!("/_data_stream/{stable}-v2")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Any per-index metadata search fails (simulates partial shard failures / timeouts).
    for idx in backing_indices.iter() {
        Mock::given(method("POST"))
            .and(path(format!("/{idx}/_search")))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;
    }

    // Expect the oldest non-write backing index to be deleted.
    Mock::given(method("DELETE"))
        .and(path(format!("/{}", backing_indices[0])))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let http = EsHttp::new(
        mock_server.uri(),
        Arc::<str>::from("user"),
        Arc::<str>::from("pass"),
        Duration::from_secs(2),
        true,
    )
    .expect("http client");
    let window = EsWindowClient::from_http(http.clone());

    let tmp_dir = tempfile::tempdir().expect("tmp dir");
    let checkpoint_path = tmp_dir.path().join("checkpoints.json");
    let cfg = build_test_config("ecs-test", "cloudwatch", checkpoint_path);

    let prune_state_path = tmp_dir.path().join("prune.json");
    let prune_state = PruneState::load_or_new(prune_state_path).expect("prune state");

    run_disk_guard_once(
        &http,
        &window,
        &cfg,
        &prune_state,
        &EsDiskGuardConfig {
            enabled: true,
            interval_secs: 1,
            free_buffer_gb: 0,
            min_keep_days: 7,
        },
        1_767_000_000_000, // fixed now_ms to keep test deterministic
    )
    .await
    .expect("disk guard once");
}
