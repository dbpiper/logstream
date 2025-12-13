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
    );

    assert!(healer.is_ok());
    let h = healer.unwrap();
    assert_eq!(h.base_url(), "http://localhost:9200");
}

#[test]
fn test_healer_stores_credentials() {
    let healer = SchemaHealer::new(
        "http://es.example.com:9200".to_string(),
        "admin".to_string(),
        "secret123".to_string(),
        Duration::from_secs(60),
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
    )
    .unwrap();

    assert!(healer.base_url().ends_with('/'));
}

mod field_type_analysis {
    use logstream::es_schema_heal::{analyze_field_type, FieldType};
    use serde_json::json;

    #[test]
    fn test_analyze_string_type() {
        let samples = vec![json!("hello"), json!("world"), json!("test")];
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }

    #[test]
    fn test_analyze_number_type() {
        let samples = vec![json!(1), json!(2), json!(3.5)];
        assert_eq!(analyze_field_type(&samples), FieldType::Number);
    }

    #[test]
    fn test_analyze_boolean_type() {
        let samples = vec![json!(true), json!(false), json!(true)];
        assert_eq!(analyze_field_type(&samples), FieldType::Boolean);
    }

    #[test]
    fn test_analyze_object_type() {
        let samples = vec![json!({"a": 1}), json!({"b": 2})];
        assert_eq!(analyze_field_type(&samples), FieldType::Object);
    }

    #[test]
    fn test_analyze_date_type() {
        let samples = vec![json!("2024-01-01T00:00:00Z"), json!("2024-06-15T12:30:00Z")];
        assert_eq!(analyze_field_type(&samples), FieldType::Date);
    }

    #[test]
    fn test_analyze_mixed_with_nulls() {
        let samples = vec![json!(null), json!("hello"), json!(null), json!("world")];
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }

    #[test]
    fn test_analyze_empty() {
        let samples: Vec<serde_json::Value> = vec![];
        assert_eq!(analyze_field_type(&samples), FieldType::Unknown);
    }

    #[test]
    fn test_analyze_majority_wins() {
        let samples = vec![
            json!("string"),
            json!("string"),
            json!(123),
            json!("string"),
        ];
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }
}

mod system_index_detection {
    use logstream::es_schema_heal::SchemaHealer;
    use std::time::Duration;

    #[test]
    fn test_healer_creation_succeeds() {
        let healer = SchemaHealer::new(
            "http://localhost:9200".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
        );
        assert!(healer.is_ok());
    }

    #[test]
    fn test_healer_cloneable() {
        let healer = SchemaHealer::new(
            "http://localhost:9200".to_string(),
            "elastic".to_string(),
            "password".to_string(),
            Duration::from_secs(30),
        )
        .unwrap();

        let cloned = healer.clone();
        assert_eq!(healer.base_url(), cloned.base_url());
    }
}
