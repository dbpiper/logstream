//! Tests for Elasticsearch index management.

use logstream::es_index::should_cleanup_index;

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
