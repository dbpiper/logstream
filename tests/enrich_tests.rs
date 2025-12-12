//! Tests for log event enrichment.

use logstream::enrich::{enrich_event, sanitize_key};
use logstream::types::LogEvent;

#[test]
fn test_enrich_basic_event() {
    let raw = LogEvent {
        id: "test-id-123".to_string(),
        timestamp_ms: 1733900000000,
        message: "Hello world".to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert_eq!(enriched.event.id, "test-id-123");
    assert!(enriched.timestamp.contains("2024") || enriched.timestamp.contains("2025"));
    assert!(enriched.tags.contains(&"sync".to_string()));
    assert!(enriched.tags.contains(&"synced".to_string()));
}

#[test]
fn test_enrich_json_message() {
    let raw = LogEvent {
        id: "json-event".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"level":"info","msg":"test"}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert!(enriched.parsed.is_some());
    assert!(enriched.tags.contains(&"json_parsed".to_string()));

    let parsed = enriched.parsed.unwrap();
    assert_eq!(parsed["level"], "info");
    assert_eq!(parsed["msg"], "test");
}

#[test]
fn test_enrich_non_json_message() {
    let raw = LogEvent {
        id: "plain-event".to_string(),
        timestamp_ms: 1733900000000,
        message: "This is plain text".to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert!(enriched.parsed.is_none());
    assert!(enriched.tags.contains(&"not_json_message".to_string()));
}

#[test]
fn test_enrich_empty_message_returns_none() {
    let raw = LogEvent {
        id: "empty-event".to_string(),
        timestamp_ms: 1733900000000,
        message: "   ".to_string(),
    };

    let result = enrich_event(raw, None);
    assert!(result.is_none());
}

#[test]
fn test_enrich_with_target_index() {
    let raw = LogEvent {
        id: "indexed-event".to_string(),
        timestamp_ms: 1733900000000,
        message: "test".to_string(),
    };

    let enriched = enrich_event(raw, Some("custom-index".to_string())).unwrap();
    assert_eq!(enriched.target_index, Some("custom-index".to_string()));
}

#[test]
fn test_enrich_generates_index_from_timestamp() {
    let raw = LogEvent {
        id: "auto-index".to_string(),
        timestamp_ms: 1733900000000,
        message: "test".to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let idx = enriched.target_index.unwrap();
    assert!(idx.starts_with("logs-"));
    assert!(idx.contains('.'));
}

#[test]
fn test_enrich_strips_carriage_return() {
    let raw = LogEvent {
        id: "cr-event".to_string(),
        timestamp_ms: 1733900000000,
        message: "line1\r\nline2\r\n".to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let msg = enriched.message.as_str().unwrap();
    assert!(!msg.contains('\r'));
}

#[test]
fn test_sanitize_key_alphanumeric() {
    assert_eq!(sanitize_key("validKey123"), "validKey123");
}

#[test]
fn test_sanitize_key_with_special_chars() {
    assert_eq!(sanitize_key("key.with.dots"), "key_with_dots");
    assert_eq!(sanitize_key("key-with-dashes"), "key_with_dashes");
    assert_eq!(sanitize_key("key@special#chars!"), "key_special_chars_");
}

#[test]
fn test_sanitize_key_preserves_underscores() {
    assert_eq!(sanitize_key("key_with_underscores"), "key_with_underscores");
}

#[test]
fn test_normalize_large_numbers_to_strings() {
    let raw = LogEvent {
        id: "big-num".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"bigNum": 9999999999999999}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    assert!(parsed["bigNum"].is_string());
}

#[test]
fn test_normalize_removes_null_values() {
    let raw = LogEvent {
        id: "null-vals".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"key": "value", "nullKey": null}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    assert!(!parsed.as_object().unwrap().contains_key("nullKey"));
}

#[test]
fn test_normalize_nested_json() {
    let raw = LogEvent {
        id: "nested".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"outer": {"inner.key": "value"}}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    assert!(parsed["outer"]
        .as_object()
        .unwrap()
        .contains_key("inner_key"));
}

#[test]
fn test_enrich_array_message() {
    let raw = LogEvent {
        id: "array".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"[1, 2, 3]"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert!(enriched.parsed.is_some());
    assert!(enriched.tags.contains(&"json_parsed".to_string()));
}

#[test]
fn test_enrich_invalid_json() {
    let raw = LogEvent {
        id: "invalid".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"unclosed": "#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert!(enriched.parsed.is_none());
    assert!(enriched.tags.contains(&"json_failure".to_string()));
}
