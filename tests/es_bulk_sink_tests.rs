//! Tests for Elasticsearch bulk sink.

use logstream::es_bulk_sink::{
    classify_error, create_fallback_event, resolve_index, BulkItemError, EsBulkConfig, EsBulkSink,
    FailureKind,
};
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

mod failure_classification_tests {
    use super::*;

    fn make_error(error_type: &str, reason: &str) -> BulkItemError {
        BulkItemError {
            error_type: error_type.to_string(),
            reason: reason.to_string(),
        }
    }

    #[test]
    fn test_mapper_parsing_exception_is_mapping() {
        let err = make_error("mapper_parsing_exception", "failed to parse field");
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_illegal_argument_exception_is_mapping() {
        let err = make_error("illegal_argument_exception", "unknown field");
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_strict_dynamic_mapping_exception_is_mapping() {
        let err = make_error(
            "strict_dynamic_mapping_exception",
            "mapping set to strict, dynamic field",
        );
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_reason_with_mapper_is_mapping() {
        let err = make_error("some_exception", "mapper failed to parse");
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_reason_with_dynamic_is_mapping() {
        let err = make_error("some_exception", "dynamic template issue");
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_reason_with_type_is_mapping() {
        let err = make_error("some_exception", "type mismatch for field");
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_version_conflict_is_version_conflict() {
        let err = make_error(
            "version_conflict_engine_exception",
            "[doc_id]: version conflict, current version [5] is different than the one provided [4]",
        );
        assert_eq!(classify_error(&err), FailureKind::VersionConflict);
    }

    #[test]
    fn test_circuit_breaker_is_retryable() {
        let err = make_error(
            "circuit_breaker_exception",
            "[parent] Data too large, data for [<http_request>] would be larger than limit",
        );
        assert_eq!(classify_error(&err), FailureKind::Retryable);
    }

    #[test]
    fn test_timeout_exception_is_retryable() {
        let err = make_error("timeout_exception", "request timed out");
        assert_eq!(classify_error(&err), FailureKind::Retryable);
    }

    #[test]
    fn test_es_rejected_execution_is_retryable() {
        let err = make_error(
            "es_rejected_execution_exception",
            "rejected execution of coordinating operation",
        );
        assert_eq!(classify_error(&err), FailureKind::Retryable);
    }

    #[test]
    fn test_cluster_block_is_retryable() {
        let err = make_error(
            "cluster_block_exception",
            "blocked by: [FORBIDDEN/12/index read-only / allow delete (api)]",
        );
        assert_eq!(classify_error(&err), FailureKind::Retryable);
    }

    #[test]
    fn test_unknown_exception_is_other() {
        let err = make_error("unknown_exception", "something unexpected happened");
        assert_eq!(classify_error(&err), FailureKind::Other);
    }

    #[test]
    fn test_document_missing_is_other() {
        let err = make_error("document_missing_exception", "[doc_id]: document missing");
        assert_eq!(classify_error(&err), FailureKind::Other);
    }

    #[test]
    fn test_empty_error_type_is_other() {
        let err = make_error("", "some reason");
        assert_eq!(classify_error(&err), FailureKind::Other);
    }

    #[test]
    fn test_version_conflict_should_be_skipped_not_retried() {
        let err = make_error(
            "version_conflict_engine_exception",
            "document already exists",
        );
        let kind = classify_error(&err);
        assert_eq!(kind, FailureKind::VersionConflict);
        assert_ne!(kind, FailureKind::Retryable);
        assert_ne!(kind, FailureKind::Mapping);
    }

    #[test]
    fn test_mapping_takes_precedence_over_version_conflict_in_reason() {
        let err = make_error(
            "mapper_parsing_exception",
            "version conflict in type mapping",
        );
        assert_eq!(classify_error(&err), FailureKind::Mapping);
    }

    #[test]
    fn test_real_world_mobile_shift_called_off_scenario() {
        let err = make_error(
            "version_conflict_engine_exception",
            "[39373209126831426785568878854856779300090876776318369911]: version conflict, required seqNo [123], primary term [1]. current document has seqNo [124] and primary term [1]",
        );
        assert_eq!(classify_error(&err), FailureKind::VersionConflict);
    }

    #[test]
    fn test_field_limit_exceeded_is_field_limit() {
        let err = make_error(
            "document_parsing_exception",
            "[1:1850] failed to parse: Limit of total fields [1000] has been exceeded while adding new fields [2]",
        );
        assert_eq!(classify_error(&err), FailureKind::FieldLimitExceeded);
    }

    #[test]
    fn test_field_limit_exceeded_different_limit() {
        let err = make_error(
            "document_parsing_exception",
            "[1:2500] failed to parse: Limit of total fields [2000] has been exceeded while adding new fields [5]",
        );
        assert_eq!(classify_error(&err), FailureKind::FieldLimitExceeded);
    }

    #[test]
    fn test_document_parsing_without_field_limit_is_other() {
        let err = make_error(
            "document_parsing_exception",
            "[1:100] failed to parse: some other parsing error",
        );
        assert_eq!(classify_error(&err), FailureKind::Other);
    }

    #[test]
    fn test_field_limit_is_not_mapping() {
        let err = make_error(
            "document_parsing_exception",
            "Limit of total fields [1000] has been exceeded",
        );
        let kind = classify_error(&err);
        assert_eq!(kind, FailureKind::FieldLimitExceeded);
        assert_ne!(kind, FailureKind::Mapping);
        assert_ne!(kind, FailureKind::Other);
    }

    #[test]
    fn test_real_world_field_limit_error() {
        let err = make_error(
            "document_parsing_exception",
            "[1:1702] failed to parse: Limit of total fields [1000] has been exceeded while adding new fields [2]",
        );
        assert_eq!(classify_error(&err), FailureKind::FieldLimitExceeded);
    }
}

mod failure_kind_behavior_tests {
    use super::*;

    #[test]
    fn test_failure_kind_copy() {
        let kind = FailureKind::Mapping;
        let copied = kind;
        assert_eq!(kind, copied);
    }

    #[test]
    fn test_failure_kind_clone() {
        let kind = FailureKind::Retryable;
        #[allow(clippy::clone_on_copy)]
        let cloned = kind.clone();
        assert_eq!(kind, cloned);
    }

    #[test]
    fn test_failure_kind_equality() {
        assert_eq!(FailureKind::Mapping, FailureKind::Mapping);
        assert_eq!(FailureKind::VersionConflict, FailureKind::VersionConflict);
        assert_eq!(FailureKind::Retryable, FailureKind::Retryable);
        assert_eq!(
            FailureKind::FieldLimitExceeded,
            FailureKind::FieldLimitExceeded
        );
        assert_eq!(FailureKind::Other, FailureKind::Other);
    }

    #[test]
    fn test_failure_kind_inequality() {
        assert_ne!(FailureKind::Mapping, FailureKind::VersionConflict);
        assert_ne!(FailureKind::Mapping, FailureKind::Retryable);
        assert_ne!(FailureKind::Mapping, FailureKind::FieldLimitExceeded);
        assert_ne!(FailureKind::Mapping, FailureKind::Other);
        assert_ne!(FailureKind::VersionConflict, FailureKind::Retryable);
        assert_ne!(
            FailureKind::VersionConflict,
            FailureKind::FieldLimitExceeded
        );
        assert_ne!(FailureKind::VersionConflict, FailureKind::Other);
        assert_ne!(FailureKind::Retryable, FailureKind::FieldLimitExceeded);
        assert_ne!(FailureKind::Retryable, FailureKind::Other);
        assert_ne!(FailureKind::FieldLimitExceeded, FailureKind::Other);
    }

    #[test]
    fn test_all_variants_are_distinct() {
        let variants = [
            FailureKind::Mapping,
            FailureKind::VersionConflict,
            FailureKind::Retryable,
            FailureKind::FieldLimitExceeded,
            FailureKind::Other,
        ];
        for (i, a) in variants.iter().enumerate() {
            for (j, b) in variants.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }
}

mod fallback_event_tests {
    use super::*;

    fn make_event_with_parsed() -> EnrichedEvent {
        EnrichedEvent {
            timestamp: "2025-12-12T16:09:19.519Z".to_string(),
            event: EventMeta {
                id: "test-event-123".to_string(),
            },
            message: serde_json::json!({
                "timestamp": "2025-12-12 16:09:19.519",
                "level": "info",
                "service_name": "gigworx-node",
                "event": "domain_event",
                "metadata": {
                    "domain_event": {
                        "name": "mobile_shift_called_off",
                        "action": "call_off_shift"
                    }
                }
            }),
            parsed: Some(serde_json::json!({
                "event": "domain_event",
                "level": "info",
                "service_name": "gigworx-node",
                "metadata": {
                    "domain_event": {
                        "name": "mobile_shift_called_off"
                    }
                }
            })),
            target_index: None,
            tags: vec!["production".to_string()],
        }
    }

    #[test]
    fn test_fallback_preserves_timestamp() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "some reason");
        assert_eq!(fallback.timestamp, original.timestamp);
    }

    #[test]
    fn test_fallback_preserves_event_id() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "some reason");
        assert_eq!(fallback.event.id, original.event.id);
    }

    #[test]
    fn test_fallback_preserves_raw_message() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "some reason");
        assert_eq!(fallback.message, original.message);
    }

    #[test]
    fn test_fallback_adds_ingestion_error_tag() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "some reason");
        assert!(fallback.tags.contains(&"ingestion_error".to_string()));
        assert!(fallback.tags.contains(&"production".to_string()));
    }

    #[test]
    fn test_fallback_includes_error_type() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "mapper_parsing_exception", "some reason");
        let parsed = fallback.parsed.unwrap();
        let error = &parsed["_ingestion_error"];
        assert_eq!(error["type"], "mapper_parsing_exception");
    }

    #[test]
    fn test_fallback_includes_error_reason() {
        let original = make_event_with_parsed();
        let fallback =
            create_fallback_event(&original, "some_error", "field [foo] is not a number");
        let parsed = fallback.parsed.unwrap();
        let error = &parsed["_ingestion_error"];
        assert_eq!(error["reason"], "field [foo] is not a number");
    }

    #[test]
    fn test_fallback_preserves_event_field() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "reason");
        let parsed = fallback.parsed.unwrap();
        assert_eq!(parsed["event"], "domain_event");
    }

    #[test]
    fn test_fallback_preserves_level_field() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "reason");
        let parsed = fallback.parsed.unwrap();
        assert_eq!(parsed["level"], "info");
    }

    #[test]
    fn test_fallback_preserves_service_name() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "reason");
        let parsed = fallback.parsed.unwrap();
        assert_eq!(parsed["service_name"], "gigworx-node");
    }

    #[test]
    fn test_fallback_truncates_long_error_reason() {
        let original = make_event_with_parsed();
        let long_reason = "x".repeat(1000);
        let fallback = create_fallback_event(&original, "some_error", &long_reason);
        let parsed = fallback.parsed.unwrap();
        let reason = parsed["_ingestion_error"]["reason"].as_str().unwrap();
        assert_eq!(reason.len(), 500);
    }

    #[test]
    fn test_fallback_includes_message_preview() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "reason");
        let parsed = fallback.parsed.unwrap();
        let preview = &parsed["_ingestion_error"]["original_message_preview"];
        assert!(preview.as_str().is_some());
    }

    #[test]
    fn test_fallback_without_parsed_still_works() {
        let original = EnrichedEvent {
            timestamp: "2025-12-12T16:09:19.519Z".to_string(),
            event: EventMeta {
                id: "test-event-123".to_string(),
            },
            message: serde_json::Value::String("raw log line".to_string()),
            parsed: None,
            target_index: None,
            tags: vec![],
        };
        let fallback = create_fallback_event(&original, "some_error", "reason");
        assert!(fallback.parsed.is_some());
        let parsed = fallback.parsed.unwrap();
        assert!(parsed.get("_ingestion_error").is_some());
    }

    #[test]
    fn test_fallback_is_valid_json() {
        let original = make_event_with_parsed();
        let fallback = create_fallback_event(&original, "some_error", "reason");
        let json_str = serde_json::to_string(&fallback);
        assert!(json_str.is_ok());
    }

    #[test]
    fn test_real_world_mobile_shift_called_off_fallback() {
        let original = EnrichedEvent {
            timestamp: "2025-12-12T16:09:19.519Z".to_string(),
            event: EventMeta {
                id: "39373209126831426785568878854856779300090876776318369911".to_string(),
            },
            message: serde_json::json!({
                "timestamp": "2025-12-12 16:09:19.519",
                "level": "info",
                "service_name": "gigworx-node",
                "event": "domain_event",
                "metadata": {
                    "domain_event": {
                        "name": "mobile_shift_called_off",
                        "action": "call_off_shift",
                        "metadata": {
                            "calledOffReason": "I broke a tooth"
                        }
                    }
                }
            }),
            parsed: Some(serde_json::json!({
                "event": "domain_event",
                "level": "info",
                "service_name": "gigworx-node"
            })),
            target_index: None,
            tags: vec![],
        };

        let fallback = create_fallback_event(
            &original,
            "unknown_exception",
            "some ES error we don't recognize",
        );

        assert_eq!(fallback.event.id, original.event.id);
        assert!(fallback.tags.contains(&"ingestion_error".to_string()));

        let parsed = fallback.parsed.unwrap();
        assert_eq!(parsed["event"], "domain_event");
        assert_eq!(parsed["_ingestion_error"]["type"], "unknown_exception");
    }
}

mod field_limit_reduction_tests {
    use logstream::es_bulk_sink::reduce_fields_to_limit;
    use serde_json::json;

    fn count_fields(value: &serde_json::Value) -> usize {
        match value {
            serde_json::Value::Object(map) => {
                map.len() + map.values().map(count_fields).sum::<usize>()
            }
            serde_json::Value::Array(arr) => arr.iter().map(count_fields).sum(),
            _ => 0,
        }
    }

    #[test]
    fn test_small_object_unchanged() {
        let mut value = json!({
            "level": "info",
            "message": "hello world",
            "timestamp": "2025-01-01T00:00:00Z"
        });
        let original = value.clone();
        reduce_fields_to_limit(&mut value);
        assert_eq!(value, original);
    }

    #[test]
    fn test_preserves_top_level_fields() {
        let mut value = json!({
            "level": "info",
            "message": "test",
            "nested": {
                "deep": {
                    "data": "value"
                }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert!(value.get("level").is_some());
        assert!(value.get("message").is_some());
        assert!(value.get("nested").is_some());
    }

    #[test]
    fn test_flattens_deeply_nested_first() {
        let mut value = json!({
            "top": "preserved",
            "nested": {
                "mid": {
                    "deep": {
                        "deeper": {
                            "deepest": "value"
                        }
                    }
                }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert_eq!(value["top"], "preserved");
        assert!(value.get("nested").is_some());
    }

    #[test]
    fn test_result_under_field_limit() {
        let mut large_nested = serde_json::Map::new();
        for i in 0..100 {
            let mut inner = serde_json::Map::new();
            for j in 0..20 {
                inner.insert(format!("field_{}", j), json!(format!("value_{}_{}", i, j)));
            }
            large_nested.insert(format!("obj_{}", i), serde_json::Value::Object(inner));
        }
        let mut value = serde_json::Value::Object(large_nested);

        let before = count_fields(&value);
        assert!(before > 900);

        reduce_fields_to_limit(&mut value);

        let after = count_fields(&value);
        assert!(after <= 900, "field count {} should be <= 900", after);
    }

    #[test]
    fn test_preserves_primitive_values() {
        let mut value = json!({
            "string": "hello",
            "number": 42,
            "boolean": true,
            "null_val": null,
            "nested": { "a": { "b": { "c": "deep" } } }
        });
        reduce_fields_to_limit(&mut value);
        assert_eq!(value["string"], "hello");
        assert_eq!(value["number"], 42);
        assert_eq!(value["boolean"], true);
        assert!(value["null_val"].is_null());
    }

    #[test]
    fn test_handles_arrays() {
        let mut value = json!({
            "items": [
                { "nested": { "deep": { "value": 1 } } },
                { "nested": { "deep": { "value": 2 } } }
            ]
        });
        reduce_fields_to_limit(&mut value);
        assert!(value.get("items").is_some());
    }

    #[test]
    fn test_empty_object_unchanged() {
        let mut value = json!({});
        reduce_fields_to_limit(&mut value);
        assert_eq!(value, json!({}));
    }

    #[test]
    fn test_single_level_object_preserved() {
        let mut value = json!({
            "a": "1", "b": "2", "c": "3", "d": "4", "e": "5"
        });
        let original = value.clone();
        reduce_fields_to_limit(&mut value);
        assert_eq!(value, original);
    }

    #[test]
    fn test_extreme_nesting_reduced() {
        let mut value = json!({
            "l1": {
                "l2": {
                    "l3": {
                        "l4": {
                            "l5": {
                                "l6": {
                                    "l7": {
                                        "l8": {
                                            "l9": {
                                                "l10": "deepest"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert!(value.get("l1").is_some());
    }

    #[test]
    fn test_real_world_complex_event() {
        let mut value = json!({
            "timestamp": "2025-12-12T16:09:19.519Z",
            "level": "info",
            "service_name": "gigworx-node",
            "event": "domain_event",
            "metadata": {
                "domain_event": {
                    "name": "mobile_shift_called_off",
                    "action": "call_off_shift",
                    "metadata": {
                        "calledOffReason": "I broke a tooth",
                        "userData": {
                            "id": 123,
                            "profile": {
                                "name": "John",
                                "settings": {
                                    "notifications": true
                                }
                            }
                        }
                    }
                }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert_eq!(value["level"], "info");
        assert_eq!(value["service_name"], "gigworx-node");
        assert_eq!(value["event"], "domain_event");
        assert!(value.get("metadata").is_some());
    }

    #[test]
    fn test_stringified_nested_is_valid_json() {
        let mut value = json!({
            "top": "value",
            "nested": { "a": { "b": { "c": { "d": { "e": "deep" } } } } }
        });
        reduce_fields_to_limit(&mut value);
        if let serde_json::Value::String(s) = &value["nested"] {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(s);
            assert!(parsed.is_ok(), "stringified value should be valid JSON");
        }
    }

    #[test]
    fn test_many_shallow_fields_preserved() {
        let mut map = serde_json::Map::new();
        for i in 0..500 {
            map.insert(format!("field_{}", i), json!(format!("value_{}", i)));
        }
        let mut value = serde_json::Value::Object(map);
        let original = value.clone();
        reduce_fields_to_limit(&mut value);
        assert_eq!(value, original);
    }

    #[test]
    fn test_mixed_depth_preserves_shallow() {
        let mut value = json!({
            "shallow1": "preserved",
            "shallow2": 42,
            "deep": {
                "level2": {
                    "level3": {
                        "level4": {
                            "level5": "stringified"
                        }
                    }
                }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert_eq!(value["shallow1"], "preserved");
        assert_eq!(value["shallow2"], 42);
    }

    #[test]
    fn test_never_gives_up_extreme_case() {
        let mut nested = serde_json::Map::new();
        for i in 0..200 {
            let mut inner = serde_json::Map::new();
            for j in 0..50 {
                inner.insert(format!("f_{}", j), json!(format!("v_{}_{}", i, j)));
            }
            nested.insert(format!("obj_{}", i), serde_json::Value::Object(inner));
        }
        let mut value = serde_json::Value::Object(nested);

        let before = count_fields(&value);
        assert!(before > 5000, "should start with many fields: {}", before);

        reduce_fields_to_limit(&mut value);

        let after = count_fields(&value);
        assert!(after <= 900, "must be under limit: {}", after);
    }

    #[test]
    fn test_binary_search_finds_optimal_depth() {
        let mut value = json!({
            "l1": "top",
            "data": {
                "l2a": { "l3a": { "l4a": "deep1" } },
                "l2b": { "l3b": { "l4b": "deep2" } }
            }
        });
        reduce_fields_to_limit(&mut value);
        assert_eq!(value["l1"], "top");
        assert!(value.get("data").is_some());
    }

    #[test]
    fn test_handles_pathological_wide_shallow() {
        let mut map = serde_json::Map::new();
        for i in 0..2000 {
            map.insert(format!("f{}", i), json!(i));
        }
        let mut value = serde_json::Value::Object(map);

        reduce_fields_to_limit(&mut value);

        let after = count_fields(&value);
        assert!(
            after <= 900 || value.is_string(),
            "should be under limit or stringified"
        );
    }

    #[test]
    fn test_largest_subtree_targeted_first() {
        let mut value = json!({
            "small": { "a": 1, "b": 2 },
            "large": {
                "x": { "y": { "z": 1 } },
                "data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            },
            "tiny": { "c": 3 }
        });
        reduce_fields_to_limit(&mut value);
        assert!(value.get("small").is_some());
        assert!(value.get("large").is_some());
        assert!(value.get("tiny").is_some());
    }

    #[test]
    fn test_array_with_many_objects() {
        let items: Vec<_> = (0..300)
            .map(|i| {
                json!({
                    "id": i,
                    "name": format!("item_{}", i),
                    "nested": { "data": { "value": i * 2, "extra": "more" } }
                })
            })
            .collect();
        let mut value = json!({ "items": items });

        let before = count_fields(&value);
        assert!(before > 900, "expected >900 fields, got {}", before);

        reduce_fields_to_limit(&mut value);

        let after = count_fields(&value);
        assert!(after <= 900, "array reduction failed: {}", after);
    }

    #[test]
    fn test_result_always_valid_json() {
        let mut complex = serde_json::Map::new();
        for i in 0..100 {
            let mut nested = serde_json::Map::new();
            for j in 0..30 {
                nested.insert(format!("n_{}", j), json!(format!("{}:{}", i, j)));
            }
            complex.insert(format!("key_{}", i), serde_json::Value::Object(nested));
        }
        let mut value = serde_json::Value::Object(complex);

        reduce_fields_to_limit(&mut value);

        let serialized = serde_json::to_string(&value);
        assert!(serialized.is_ok());
        let reparsed: Result<serde_json::Value, _> = serde_json::from_str(&serialized.unwrap());
        assert!(reparsed.is_ok());
    }
}
