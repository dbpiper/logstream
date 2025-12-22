use std::time::Duration;

use logstream::es_counts::EsCounter;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn get_ids_in_range_sorts_by_id_and_paginates_with_search_after() {
    let server = MockServer::start().await;

    let start_ms = 1_700_000_000_000_i64;
    let end_ms = 1_700_000_100_000_i64;
    let end_inclusive_ms = end_ms - 1;

    let first_body = serde_json::json!({
        "size": 5000,
        "sort": [
            { "@timestamp": { "order": "asc" } },
            { "_id": { "order": "asc" } }
        ],
        "_source": false,
        "query": {
            "range": {
                "@timestamp": {
                    "gte": start_ms,
                    "lte": end_inclusive_ms,
                    "format": "epoch_millis"
                }
            }
        }
    });

    // get_ids_in_range only paginates when it receives a full batch (BATCH_SIZE=5000).
    // So to verify search_after pagination, we return 5000 hits for the first request.
    let first_hits = (0..5000)
        .map(|i| {
            let id = format!("id-{i}");
            serde_json::json!({ "_id": id, "sort": [start_ms + i as i64, format!("id-{i}")] })
        })
        .collect::<Vec<serde_json::Value>>();
    let first_resp = serde_json::json!({ "hits": { "hits": first_hits } });

    Mock::given(method("POST"))
        .and(path("/test-target/_search"))
        .and(body_json(first_body))
        .respond_with(ResponseTemplate::new(200).set_body_json(first_resp))
        .mount(&server)
        .await;

    let second_body = serde_json::json!({
        "size": 5000,
        "sort": [
            { "@timestamp": { "order": "asc" } },
            { "_id": { "order": "asc" } }
        ],
        "_source": false,
        "query": {
            "range": {
                "@timestamp": {
                    "gte": start_ms,
                    "lte": end_inclusive_ms,
                    "format": "epoch_millis"
                }
            }
        },
        "search_after": [start_ms + 4999, "id-4999"]
    });

    let second_resp = serde_json::json!({ "hits": { "hits": [{ "_id": "id-5000", "sort": [start_ms + 5000, "id-5000"] }] } });

    Mock::given(method("POST"))
        .and(path("/test-target/_search"))
        .and(body_json(second_body))
        .respond_with(ResponseTemplate::new(200).set_body_json(second_resp))
        .mount(&server)
        .await;

    let counter = EsCounter::new(
        server.uri(),
        "user",
        "pass",
        Duration::from_secs(5),
        "test-target",
    )
    .expect("EsCounter");
    let ids = counter
        .get_ids_in_range(start_ms, end_ms)
        .await
        .expect("get_ids_in_range");

    assert_eq!(ids.len(), 5001);
    assert_eq!(ids[0], "id-0");
    assert_eq!(ids[1], "id-1");
    assert_eq!(ids[4999], "id-4999");
    assert_eq!(ids[5000], "id-5000");
}
