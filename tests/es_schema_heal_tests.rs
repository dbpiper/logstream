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
