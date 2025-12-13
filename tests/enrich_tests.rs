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
    assert_eq!(sanitize_key("validKey123"), "validkey123");
}

#[test]
fn test_sanitize_key_with_special_chars() {
    assert_eq!(sanitize_key("key.with.dots"), "key_with_dots");
    assert_eq!(sanitize_key("key-with-dashes"), "key_with_dashes");
    assert_eq!(sanitize_key("key@special#chars!"), "key_special_chars");
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
    assert!(parsed["bignum"].is_string());
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

#[test]
fn test_reserved_field_sanitized() {
    let raw = LogEvent {
        id: "reserved".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"_id": "user-id", "_source": "custom"}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    let obj = parsed.as_object().unwrap();
    assert!(obj.contains_key("id") || obj.contains_key("field_id"));
    assert!(obj.contains_key("source") || obj.contains_key("field_source"));
}

#[test]
fn test_heterogeneous_array_flattened() {
    let raw = LogEvent {
        id: "hetero".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"mixed": [1, "two", {"three": 3}]}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    let arr = parsed["mixed"].as_array().unwrap();
    assert!(arr.iter().all(|v| v.is_string()));
}

#[test]
fn test_deep_nesting_flattened() {
    let mut msg = String::from("{");
    for i in 0..25 {
        msg.push_str(&format!("\"level{}\": {{", i));
    }
    msg.push_str("\"deep\": \"value\"");
    for _ in 0..25 {
        msg.push('}');
    }
    msg.push('}');

    let raw = LogEvent {
        id: "deep".to_string(),
        timestamp_ms: 1733900000000,
        message: msg,
    };

    let enriched = enrich_event(raw, None).unwrap();
    assert!(enriched.parsed.is_some());
}

#[test]
fn test_type_conflict_field_stringified() {
    let raw = LogEvent {
        id: "conflict".to_string(),
        timestamp_ms: 1733900000000,
        message: r#"{"id": {"nested": "object"}, "status": [1, 2, 3]}"#.to_string(),
    };

    let enriched = enrich_event(raw, None).unwrap();
    let parsed = enriched.parsed.unwrap();
    assert!(parsed["id"].is_string());
    assert!(parsed["status"].is_string());
}

#[test]
fn test_sanitize_key_leading_digit() {
    assert_eq!(sanitize_key("123abc"), "field_123abc");
}

#[test]
fn test_sanitize_key_empty() {
    assert_eq!(sanitize_key("..."), "field_");
}

#[test]
fn test_normalize_nan_becomes_null() {
    use logstream::enrich::normalize_for_es;
    use serde_json::json;

    let mut val = json!({"num": f64::NAN});
    normalize_for_es(&mut val);
    assert!(val["num"].is_null());
}

#[test]
fn test_normalize_infinity_becomes_null() {
    use logstream::enrich::normalize_for_es;
    use serde_json::json;

    let mut val = json!({"num": f64::INFINITY});
    normalize_for_es(&mut val);
    assert!(val["num"].is_null());
}

#[test]
fn test_flatten_all_objects_stringifies_nested() {
    use logstream::enrich::flatten_all_objects;
    use serde_json::json;

    // flatten_all_objects should stringify ALL nested objects
    let mut val = json!({
        "user": {
            "profile": {"name": "test", "age": 30}
        },
        "simple": "value"
    });
    flatten_all_objects(&mut val);

    // Top-level object remains, but nested "profile" is stringified
    assert!(val["user"].is_object());
    assert!(val["user"]["profile"].is_string());
    assert!(val["simple"].is_string());
}

#[test]
fn test_flatten_all_objects_handles_arrays_of_objects() {
    use logstream::enrich::flatten_all_objects;
    use serde_json::json;

    let mut val = json!({
        "items": [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"}
        ]
    });
    flatten_all_objects(&mut val);

    // Arrays containing objects should be stringified
    assert!(val["items"].is_string());
}

#[test]
fn test_flatten_all_objects_preserves_primitive_arrays() {
    use logstream::enrich::flatten_all_objects;
    use serde_json::json;

    let mut val = json!({
        "tags": ["a", "b", "c"],
        "numbers": [1, 2, 3]
    });
    flatten_all_objects(&mut val);

    // Arrays of primitives should remain as arrays
    assert!(val["tags"].is_array());
    assert!(val["numbers"].is_array());
}

#[test]
fn test_normalize_keeps_nested_objects_initially() {
    use logstream::enrich::normalize_for_es;
    use serde_json::json;

    // normalize_for_es should keep nested objects (less aggressive)
    // flatten_all_objects is used on retry after mapping errors
    let mut val = json!({"userProfile": {"name": "test", "age": 30}});
    normalize_for_es(&mut val);
    assert!(val["userprofile"].is_object());

    let mut val = json!({"settings": {"enabled": true}});
    normalize_for_es(&mut val);
    assert!(val["settings"].is_object());
}

#[test]
fn test_known_problematic_keys_stringified() {
    use logstream::enrich::normalize_for_es;
    use serde_json::json;

    // These keys (id, type, status, code, version) should always be primitives
    // If they're objects, they get stringified
    let mut val = json!({"id": {"nested": "object"}});
    normalize_for_es(&mut val);
    assert!(val["id"].is_string());

    let mut val = json!({"type": {"kind": "something"}});
    normalize_for_es(&mut val);
    assert!(val["type"].is_string());

    let mut val = json!({"status": ["pending", "active"]});
    normalize_for_es(&mut val);
    assert!(val["status"].is_string());
}
