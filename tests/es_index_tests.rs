//! Tests for Elasticsearch index management.

use logstream::es_index::should_cleanup_index;

mod index_template_tests {
    const INDEX_FIELD_LIMIT: u32 = 10000;

    fn build_template_body(index_prefix: &str) -> serde_json::Value {
        serde_json::json!({
            "index_patterns": [format!("{}-*", index_prefix)],
            "priority": 100,
            "template": {
                "settings": {
                    "index": {
                        "mapping": {
                            "total_fields": {
                                "limit": INDEX_FIELD_LIMIT
                            },
                            "ignore_malformed": true
                        },
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    }
                },
                "mappings": {
                    "dynamic": "true",
                    "properties": {
                        "@timestamp": { "type": "date" },
                        "timestamp": { "type": "date" },
                        "message": { "type": "text" },
                        "level": { "type": "keyword" },
                        "service_name": { "type": "keyword" },
                        "log_group": { "type": "keyword" },
                        "log_stream": { "type": "keyword" },
                        "event_id": { "type": "keyword" },
                        "tags": { "type": "keyword" }
                    }
                }
            }
        })
    }

    #[test]
    fn test_template_has_high_field_limit() {
        let template = build_template_body("logs");
        let limit = template
            .pointer("/template/settings/index/mapping/total_fields/limit")
            .and_then(|v| v.as_u64());
        assert_eq!(limit, Some(INDEX_FIELD_LIMIT as u64));
    }

    #[test]
    fn test_template_field_limit_is_10000() {
        assert_eq!(INDEX_FIELD_LIMIT, 10000);
    }

    #[test]
    fn test_template_has_ignore_malformed() {
        let template = build_template_body("logs");
        let ignore = template
            .pointer("/template/settings/index/mapping/ignore_malformed")
            .and_then(|v| v.as_bool());
        assert_eq!(ignore, Some(true));
    }

    #[test]
    fn test_template_has_correct_index_pattern() {
        let template = build_template_body("myprefix");
        let patterns = template
            .get("index_patterns")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());
        assert_eq!(patterns, Some(vec!["myprefix-*"]));
    }

    #[test]
    fn test_template_has_priority() {
        let template = build_template_body("logs");
        let priority = template.get("priority").and_then(|v| v.as_u64());
        assert_eq!(priority, Some(100));
    }

    #[test]
    fn test_template_has_timestamp_mapping() {
        let template = build_template_body("logs");
        let ts_type = template
            .pointer("/template/mappings/properties/@timestamp/type")
            .and_then(|v| v.as_str());
        assert_eq!(ts_type, Some("date"));
    }

    #[test]
    fn test_template_has_message_mapping() {
        let template = build_template_body("logs");
        let msg_type = template
            .pointer("/template/mappings/properties/message/type")
            .and_then(|v| v.as_str());
        assert_eq!(msg_type, Some("text"));
    }

    #[test]
    fn test_template_has_keyword_fields() {
        let template = build_template_body("logs");
        let keyword_fields = [
            "level",
            "service_name",
            "log_group",
            "log_stream",
            "event_id",
            "tags",
        ];
        for field in keyword_fields {
            let field_type = template
                .pointer(&format!("/template/mappings/properties/{}/type", field))
                .and_then(|v| v.as_str());
            assert_eq!(
                field_type,
                Some("keyword"),
                "field {} should be keyword",
                field
            );
        }
    }

    #[test]
    fn test_template_has_dynamic_mapping() {
        let template = build_template_body("logs");
        let dynamic = template
            .pointer("/template/mappings/dynamic")
            .and_then(|v| v.as_str());
        assert_eq!(dynamic, Some("true"));
    }

    #[test]
    fn test_template_has_shard_settings() {
        let template = build_template_body("logs");
        let shards = template
            .pointer("/template/settings/index/number_of_shards")
            .and_then(|v| v.as_u64());
        let replicas = template
            .pointer("/template/settings/index/number_of_replicas")
            .and_then(|v| v.as_u64());
        assert_eq!(shards, Some(1));
        assert_eq!(replicas, Some(1));
    }

    #[test]
    fn test_field_limit_settings_body() {
        let body = serde_json::json!({
            "index": {
                "mapping": {
                    "total_fields": {
                        "limit": INDEX_FIELD_LIMIT
                    }
                }
            }
        });
        let limit = body
            .pointer("/index/mapping/total_fields/limit")
            .and_then(|v| v.as_u64());
        assert_eq!(limit, Some(INDEX_FIELD_LIMIT as u64));
    }

    #[test]
    fn test_different_prefix_generates_correct_pattern() {
        let template = build_template_body("cloudwatch");
        let patterns = template
            .get("index_patterns")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());
        assert_eq!(patterns, Some(vec!["cloudwatch-*"]));
    }
}

// ============================================================================
// should_cleanup_index Tests
// ============================================================================

#[test]
fn test_should_cleanup_temp_index() {
    assert!(should_cleanup_index("logs-2024-temp", "0", "green"));
    assert!(should_cleanup_index("logs-temp-2024", "100", "green"));
    assert!(should_cleanup_index("logs-2024-01-01-temp", "0", "yellow"));
}

#[test]
fn test_should_cleanup_empty_red_index() {
    assert!(should_cleanup_index("logs-2024-01-01", "0", "red"));
}

#[test]
fn test_should_not_cleanup_healthy_index() {
    assert!(!should_cleanup_index("logs-2024-01-01", "1000", "green"));
    assert!(!should_cleanup_index("logs-2024-01-01", "1", "yellow"));
}

#[test]
fn test_should_not_cleanup_empty_green_index() {
    assert!(!should_cleanup_index("logs-2024-01-01", "0", "green"));
}

#[test]
fn test_should_not_cleanup_populated_red_index() {
    // Index with documents should not be deleted even if red
    assert!(!should_cleanup_index("logs-2024-01-01", "1", "red"));
}

#[test]
fn test_temp_suffix_cleanup() {
    assert!(should_cleanup_index("myindex-temp", "500", "green"));
}

#[test]
fn test_temp_in_middle_cleanup() {
    assert!(should_cleanup_index("my-temp-index", "500", "green"));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_index_name() {
    assert!(!should_cleanup_index("", "0", "green"));
}

#[test]
fn test_unknown_health() {
    assert!(!should_cleanup_index("logs-2024", "0", "unknown"));
}

#[test]
fn test_empty_docs_string() {
    // Empty string for docs should not match "0"
    assert!(!should_cleanup_index("logs-2024", "", "red"));
}
