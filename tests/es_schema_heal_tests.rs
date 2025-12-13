//! Tests for ES schema healing.

use logstream::es_schema_heal::SchemaHealer;
use std::time::Duration;

#[test]
fn test_schema_healer_new() {
    let healer = SchemaHealer::new(
        "http://localhost:9200".to_string(),
        "elastic".to_string(),
        "password".to_string(),
        Duration::from_secs(30),
        "logs".to_string(),
    );

    assert!(healer.is_ok());
    let h = healer.unwrap();
    assert_eq!(h.base_url(), "http://localhost:9200");
    assert_eq!(h.index_prefix(), "logs");
}

#[test]
fn test_index_pattern() {
    let healer = SchemaHealer::new(
        "http://localhost:9200".to_string(),
        "elastic".to_string(),
        "password".to_string(),
        Duration::from_secs(30),
        "myapp".to_string(),
    )
    .unwrap();

    assert_eq!(healer.index_pattern(), "myapp-*");
}

#[test]
fn test_index_pattern_logs() {
    let healer = SchemaHealer::new(
        "http://localhost:9200".to_string(),
        "elastic".to_string(),
        "password".to_string(),
        Duration::from_secs(30),
        "logs".to_string(),
    )
    .unwrap();

    assert_eq!(healer.index_pattern(), "logs-*");
}

#[test]
fn test_healer_stores_credentials() {
    let healer = SchemaHealer::new(
        "http://es.example.com:9200".to_string(),
        "admin".to_string(),
        "secret123".to_string(),
        Duration::from_secs(60),
        "audit".to_string(),
    )
    .unwrap();

    assert_eq!(healer.user(), "admin");
    assert_eq!(healer.pass(), "secret123");
    assert_eq!(healer.timeout(), Duration::from_secs(60));
}

#[test]
fn test_healer_with_trailing_slash() {
    let healer = SchemaHealer::new(
        "http://localhost:9200/".to_string(),
        "elastic".to_string(),
        "password".to_string(),
        Duration::from_secs(30),
        "logs".to_string(),
    )
    .unwrap();

    assert!(healer.base_url().ends_with('/'));
}

mod field_type_analysis {
    use logstream::es_schema_heal::{analyze_field_type, FieldType};
    use serde_json::json;

    #[test]
    fn test_analyze_mostly_strings() {
        let samples = vec![
            json!("hello"),
            json!("world"),
            json!("test"),
            json!(null),
            json!("more"),
        ];
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }

    #[test]
    fn test_analyze_mostly_numbers() {
        let samples = vec![json!(1), json!(2), json!(3), json!(null), json!(5)];
        assert_eq!(analyze_field_type(&samples), FieldType::Number);
    }

    #[test]
    fn test_analyze_mostly_objects() {
        let samples = vec![
            json!({"a": 1}),
            json!({"b": 2}),
            json!("string"),
            json!({"c": 3}),
        ];
        assert_eq!(analyze_field_type(&samples), FieldType::Object);
    }

    #[test]
    fn test_analyze_mixed_with_object_majority() {
        let samples = vec![
            json!({"nested": "value"}),
            json!("2024-01-01"),
            json!({"other": "object"}),
            json!({"third": "obj"}),
        ];
        // 3 objects vs 1 string
        assert_eq!(analyze_field_type(&samples), FieldType::Object);
    }

    #[test]
    fn test_analyze_mixed_with_string_majority() {
        let samples = vec![
            json!("2024-01-01"),
            json!("2024-01-02"),
            json!("2024-01-03"),
            json!({"value": "2024-01-04"}),
        ];
        // 3 strings vs 1 object
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }

    #[test]
    fn test_analyze_dates() {
        let samples = vec![
            json!("2024-01-01T00:00:00Z"),
            json!("2024-01-02T12:30:00Z"),
            json!("2024-01-03T18:45:00Z"),
        ];
        assert_eq!(analyze_field_type(&samples), FieldType::Date);
    }

    #[test]
    fn test_analyze_empty() {
        let samples: Vec<serde_json::Value> = vec![];
        assert_eq!(analyze_field_type(&samples), FieldType::Unknown);
    }

    #[test]
    fn test_analyze_all_nulls() {
        let samples = vec![json!(null), json!(null), json!(null)];
        assert_eq!(analyze_field_type(&samples), FieldType::Unknown);
    }
}
