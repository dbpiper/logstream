//! Tests for checkpoint path utilities.

use logstream::checkpoint::{
    checkpoint_path_for, group_from_checkpoint_path, is_checkpoint_file, sanitize_group_name,
};
use std::path::Path;

// ============================================================================
// sanitize_group_name Tests
// ============================================================================

#[test]
fn test_sanitize_group_name_simple() {
    assert_eq!(sanitize_group_name("mygroup"), "mygroup");
}

#[test]
fn test_sanitize_group_name_with_slashes() {
    assert_eq!(sanitize_group_name("my/log/group"), "my-log-group");
}

#[test]
fn test_sanitize_group_name_with_special_chars() {
    assert_eq!(sanitize_group_name("my.log:group"), "my-log-group");
}

#[test]
fn test_sanitize_group_name_with_spaces() {
    assert_eq!(sanitize_group_name("my log group"), "my-log-group");
}

#[test]
fn test_sanitize_group_name_alphanumeric() {
    assert_eq!(sanitize_group_name("log123group"), "log123group");
}

#[test]
fn test_sanitize_group_name_preserves_case() {
    assert_eq!(sanitize_group_name("MyLogGroup"), "MyLogGroup");
}

#[test]
fn test_sanitize_group_name_empty() {
    assert_eq!(sanitize_group_name(""), "");
}

#[test]
fn test_sanitize_group_name_all_special() {
    assert_eq!(sanitize_group_name("/:/./"), "-----");
}

// ============================================================================
// checkpoint_path_for Tests
// ============================================================================

#[test]
fn test_checkpoint_path_for_directory() {
    let base = Path::new("/var/log/checkpoints");
    let path = checkpoint_path_for(base, "my-log-group");
    assert_eq!(
        path,
        Path::new("/var/log/checkpoints/checkpoints-my-log-group.json")
    );
}

#[test]
fn test_checkpoint_path_for_file() {
    let base = Path::new("/var/log/checkpoints.json");
    let path = checkpoint_path_for(base, "my-log-group");
    assert_eq!(path, Path::new("/var/log/checkpoints-my-log-group.json"));
}

#[test]
fn test_checkpoint_path_for_sanitizes_group() {
    let base = Path::new("/var/log");
    let path = checkpoint_path_for(base, "my/log:group");
    assert_eq!(path, Path::new("/var/log/checkpoints-my-log-group.json"));
}

#[test]
fn test_checkpoint_path_for_relative_path() {
    let base = Path::new("./checkpoints");
    let path = checkpoint_path_for(base, "group1");
    assert_eq!(path, Path::new("./checkpoints/checkpoints-group1.json"));
}

#[test]
fn test_checkpoint_path_for_empty_group() {
    let base = Path::new("/var/log");
    let path = checkpoint_path_for(base, "");
    assert_eq!(path, Path::new("/var/log/checkpoints-.json"));
}

// ============================================================================
// group_from_checkpoint_path Tests
// ============================================================================

#[test]
fn test_group_from_checkpoint_path_valid() {
    let path = Path::new("/var/log/checkpoints-my-log-group.json");
    let group = group_from_checkpoint_path(path);
    assert_eq!(group, Some("my-log-group".to_string()));
}

#[test]
fn test_group_from_checkpoint_path_complex_name() {
    let path = Path::new("checkpoints-aws-lambda-my-function.json");
    let group = group_from_checkpoint_path(path);
    assert_eq!(group, Some("aws-lambda-my-function".to_string()));
}

#[test]
fn test_group_from_checkpoint_path_invalid_prefix() {
    let path = Path::new("/var/log/checkpoint-mygroup.json");
    let group = group_from_checkpoint_path(path);
    assert_eq!(group, None);
}

#[test]
fn test_group_from_checkpoint_path_invalid_suffix() {
    let path = Path::new("/var/log/checkpoints-mygroup.txt");
    let group = group_from_checkpoint_path(path);
    assert_eq!(group, None);
}

#[test]
fn test_group_from_checkpoint_path_not_a_file() {
    let path = Path::new("/var/log/");
    let group = group_from_checkpoint_path(path);
    assert_eq!(group, None);
}

// ============================================================================
// is_checkpoint_file Tests
// ============================================================================

#[test]
fn test_is_checkpoint_file_valid() {
    let path = Path::new("checkpoints-mygroup.json");
    assert!(is_checkpoint_file(path));
}

#[test]
fn test_is_checkpoint_file_with_path() {
    let path = Path::new("/var/log/checkpoints-mygroup.json");
    assert!(is_checkpoint_file(path));
}

#[test]
fn test_is_checkpoint_file_wrong_prefix() {
    let path = Path::new("checkpoint-mygroup.json");
    assert!(!is_checkpoint_file(path));
}

#[test]
fn test_is_checkpoint_file_wrong_suffix() {
    let path = Path::new("checkpoints-mygroup.txt");
    assert!(!is_checkpoint_file(path));
}

#[test]
fn test_is_checkpoint_file_no_extension() {
    let path = Path::new("checkpoints-mygroup");
    assert!(!is_checkpoint_file(path));
}

#[test]
fn test_is_checkpoint_file_directory() {
    let path = Path::new("/var/log/");
    assert!(!is_checkpoint_file(path));
}

// ============================================================================
// Roundtrip Tests
// ============================================================================

#[test]
fn test_checkpoint_path_roundtrip() {
    let base = Path::new("/var/log");
    let original_group = "my-log-group";

    let path = checkpoint_path_for(base, original_group);
    let recovered = group_from_checkpoint_path(&path);

    // Note: Can't recover exact original if it had special chars
    assert_eq!(recovered, Some(sanitize_group_name(original_group)));
}

#[test]
fn test_checkpoint_path_is_valid_checkpoint_file() {
    let base = Path::new("/var/log");
    let path = checkpoint_path_for(base, "my-group");
    assert!(is_checkpoint_file(&path));
}
