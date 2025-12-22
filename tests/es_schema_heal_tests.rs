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

    assert_eq!(healer.base_url(), "http://localhost:9200");
}

mod field_type_analysis {
    use logstream::es_schema_heal::{analyze_field_type, is_reliable_inferred_type, FieldType};
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

    #[test]
    fn test_array_of_strings_is_string() {
        // ES treats arrays transparently - ["a", "b"] in a keyword field
        let samples = vec![json!(["hello", "world"]), json!(["test"])];
        assert_eq!(analyze_field_type(&samples), FieldType::String);
    }

    #[test]
    fn test_array_of_numbers_is_number() {
        let samples = vec![json!([1, 2, 3]), json!([4, 5])];
        assert_eq!(analyze_field_type(&samples), FieldType::Number);
    }

    #[test]
    fn test_array_of_objects_is_object() {
        let samples = vec![json!([{"a": 1}]), json!([{"b": 2}])];
        assert_eq!(analyze_field_type(&samples), FieldType::Object);
    }

    #[test]
    fn test_empty_array_is_unknown() {
        let samples = vec![json!([])];
        assert_eq!(analyze_field_type(&samples), FieldType::Unknown);
    }

    #[test]
    fn test_is_reliable_inferred_type() {
        assert!(!is_reliable_inferred_type(FieldType::Unknown));
        assert!(!is_reliable_inferred_type(FieldType::Null));
        assert!(is_reliable_inferred_type(FieldType::String));
        assert!(is_reliable_inferred_type(FieldType::Number));
        assert!(is_reliable_inferred_type(FieldType::Boolean));
        assert!(is_reliable_inferred_type(FieldType::Date));
        assert!(is_reliable_inferred_type(FieldType::Object));
        assert!(is_reliable_inferred_type(FieldType::Array));
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

mod cross_index_conflicts {
    use logstream::es_schema_heal::{extract_all_field_types, find_minority_type_indices};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_extract_field_types_simple() {
        let mapping = json!({
            "properties": {
                "name": { "type": "keyword" },
                "age": { "type": "long" },
                "active": { "type": "boolean" }
            }
        });

        let fields = extract_all_field_types(&mapping);
        assert_eq!(fields.len(), 3);
        assert!(fields.contains(&("name".to_string(), "keyword".to_string())));
        assert!(fields.contains(&("age".to_string(), "long".to_string())));
        assert!(fields.contains(&("active".to_string(), "boolean".to_string())));
    }

    #[test]
    fn test_extract_field_types_nested() {
        let mapping = json!({
            "properties": {
                "user": {
                    "properties": {
                        "name": { "type": "keyword" },
                        "email": { "type": "text" }
                    }
                }
            }
        });

        let fields = extract_all_field_types(&mapping);
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&("user.name".to_string(), "keyword".to_string())));
        assert!(fields.contains(&("user.email".to_string(), "text".to_string())));
    }

    #[test]
    fn test_extract_field_types_deep_nesting() {
        let mapping = json!({
            "properties": {
                "level1": {
                    "properties": {
                        "level2": {
                            "properties": {
                                "value": { "type": "long" }
                            }
                        }
                    }
                }
            }
        });

        let fields = extract_all_field_types(&mapping);
        assert_eq!(fields.len(), 1);
        assert!(fields.contains(&("level1.level2.value".to_string(), "long".to_string())));
    }

    #[test]
    fn test_extract_field_types_empty() {
        let mapping = json!({
            "properties": {}
        });

        let fields = extract_all_field_types(&mapping);
        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_extract_field_types_no_properties() {
        let mapping = json!({});

        let fields = extract_all_field_types(&mapping);
        assert_eq!(fields.len(), 0);
    }

    #[test]
    fn test_find_minority_clear_majority() {
        let mut type_map = HashMap::new();
        type_map.insert(
            "long".to_string(),
            vec!["idx1", "idx2", "idx3", "idx4", "idx5"],
        );
        type_map.insert("keyword".to_string(), vec!["idx6"]);

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 1);
        assert!(minority.contains(&"idx6"));
    }

    #[test]
    fn test_find_minority_multiple_minorities() {
        let mut type_map = HashMap::new();
        type_map.insert("long".to_string(), vec!["idx1", "idx2", "idx3"]);
        type_map.insert("keyword".to_string(), vec!["idx4"]);
        type_map.insert("text".to_string(), vec!["idx5"]);

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 2);
        assert!(minority.contains(&"idx4"));
        assert!(minority.contains(&"idx5"));
    }

    #[test]
    fn test_find_minority_tie_keeps_all() {
        let mut type_map = HashMap::new();
        type_map.insert("long".to_string(), vec!["idx1"]);
        type_map.insert("keyword".to_string(), vec!["idx2"]);

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 0);
    }

    #[test]
    fn test_find_minority_single_type() {
        let mut type_map = HashMap::new();
        type_map.insert("long".to_string(), vec!["idx1", "idx2"]);

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 0);
    }

    #[test]
    fn test_find_minority_empty() {
        let type_map: HashMap<String, Vec<&str>> = HashMap::new();
        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 0);
    }

    #[test]
    fn test_cross_index_scenario_realistic() {
        let mut type_map = HashMap::new();
        type_map.insert(
            "long".to_string(),
            vec![
                "cloudwatch-2025.12.10",
                "cloudwatch-2025.12.11",
                "cloudwatch-2025.12.12",
                "cloudwatch-2025.12.13",
                "cloudwatch-2025.12.14",
            ],
        );
        type_map.insert(
            "keyword".to_string(),
            vec!["cloudwatch-2025.12.08", "cloudwatch-2025.12.09"],
        );

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 2);
        assert!(minority.contains(&"cloudwatch-2025.12.08"));
        assert!(minority.contains(&"cloudwatch-2025.12.09"));
    }

    #[test]
    fn test_cross_index_three_way_split() {
        let mut type_map = HashMap::new();
        type_map.insert("long".to_string(), vec!["idx1", "idx2", "idx3", "idx4"]);
        type_map.insert("keyword".to_string(), vec!["idx5", "idx6"]);
        type_map.insert("text".to_string(), vec!["idx7"]);

        let minority = find_minority_type_indices(&type_map);
        assert_eq!(minority.len(), 3);
        assert!(minority.contains(&"idx5"));
        assert!(minority.contains(&"idx6"));
        assert!(minority.contains(&"idx7"));
    }
}
