//! Tests for Elasticsearch bulk sink.

use logstream::es_bulk_sink::{resolve_index, EsBulkConfig, EsBulkSink};
use logstream::types::{EnrichedEvent, EventMeta};
use std::time::Duration;

fn sample_event(timestamp: &str, target_index: Option<String>) -> EnrichedEvent {
    EnrichedEvent {
        timestamp: timestamp.to_string(),
        event: EventMeta {
            id: "test-id".to_string(),
        },
        message: serde_json::Value::String("test".to_string()),
        parsed: None,
        target_index,
        tags: vec![],
    }
}

#[test]
fn test_resolve_index_with_target() {
    let ev = sample_event("2025-12-11T12:00:00Z", Some("custom-index".to_string()));
    let idx = resolve_index(&ev, "logs");
    assert_eq!(idx, "custom-index");
}

#[test]
fn test_resolve_index_from_timestamp() {
    let ev = sample_event("2025-12-11T12:00:00+00:00", None);
    let idx = resolve_index(&ev, "logs");
    assert_eq!(idx, "logs-2025.12.11");
}

#[test]
fn test_resolve_index_different_prefix() {
    let ev = sample_event("2024-01-15T08:30:00Z", None);
    let idx = resolve_index(&ev, "myapp");
    assert_eq!(idx, "myapp-2024.01.15");
}

#[test]
fn test_resolve_index_invalid_timestamp() {
    let ev = sample_event("not-a-timestamp", None);
    let idx = resolve_index(&ev, "logs");
    assert_eq!(idx, "logs-default");
}

#[test]
fn test_resolve_index_empty_timestamp() {
    let ev = sample_event("", None);
    let idx = resolve_index(&ev, "logs");
    assert_eq!(idx, "logs-default");
}

#[test]
fn test_es_bulk_config_clone() {
    let cfg = EsBulkConfig {
        url: "http://localhost:9200".into(),
        user: "elastic".into(),
        pass: "password".into(),
        batch_size: 100,
        max_batch_size: 1000,
        timeout: Duration::from_secs(30),
        gzip: true,
        index_prefix: "logs".into(),
    };

    let cloned = cfg.clone();
    assert_eq!(cloned.url, cfg.url);
    assert_eq!(cloned.batch_size, cfg.batch_size);
    assert_eq!(cloned.gzip, cfg.gzip);
}

#[test]
fn test_es_bulk_sink_new() {
    let cfg = EsBulkConfig {
        url: "http://localhost:9200".into(),
        user: "elastic".into(),
        pass: "password".into(),
        batch_size: 100,
        max_batch_size: 1000,
        timeout: Duration::from_secs(30),
        gzip: true,
        index_prefix: "logs".into(),
    };

    let sink = EsBulkSink::new(cfg);
    assert!(sink.is_ok());
}

#[test]
fn test_resolve_index_with_timezone_offset() {
    let ev = sample_event("2025-06-15T10:30:00-05:00", None);
    let idx = resolve_index(&ev, "logs");
    // Should be UTC: 15:30 UTC on 2025-06-15
    assert_eq!(idx, "logs-2025.06.15");
}

#[test]
fn test_resolve_index_target_takes_precedence() {
    let ev = sample_event("2025-12-11T12:00:00Z", Some("override-index".to_string()));
    let idx = resolve_index(&ev, "logs");
    assert_eq!(idx, "override-index");
}
