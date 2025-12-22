use serde_json::Value;

pub fn ts_range_query(start_ms: i64, end_inclusive_ms: i64) -> Value {
    serde_json::json!({
        "range": {
            "@timestamp": {
                "gte": start_ms,
                "lte": end_inclusive_ms,
                "format": "epoch_millis"
            }
        }
    })
}

pub fn ts_range_lt_query(start_ms: i64, end_ms: i64) -> Value {
    serde_json::json!({
        "range": {
            "@timestamp": { "gte": start_ms, "lt": end_ms, "format": "epoch_millis" }
        }
    })
}

pub fn min_max_ts_aggs_body() -> Value {
    serde_json::json!({
        "size": 0,
        "aggs": {
            "min_ts": { "min": { "field": "@timestamp" } },
            "max_ts": { "max": { "field": "@timestamp" } }
        }
    })
}

pub fn max_ts_by_index_aggs_body() -> Value {
    serde_json::json!({
        "size": 0,
        "aggs": {
            "by_index": {
                "terms": { "field": "_index", "size": 10000 },
                "aggs": {
                    "max_ts": { "max": { "field": "@timestamp" } }
                }
            }
        }
    })
}
