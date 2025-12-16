//! Tests for checkpoint state management.

use logstream::state::{CheckpointState, StreamCursor};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_stream_cursor_default() {
    let cursor = StreamCursor::default();
    assert!(cursor.next_token.is_none());
    assert!(cursor.next_start_time_ms.is_none());
}

#[test]
fn test_checkpoint_state_default() {
    let state = CheckpointState::default();
    assert!(state.streams.is_empty());
}

#[test]
fn test_cursor_for_missing_stream() {
    let state = CheckpointState::default();
    let cursor = state.cursor_for("nonexistent");
    assert!(cursor.next_token.is_none());
    assert!(cursor.next_start_time_ms.is_none());
}

#[test]
fn test_update_and_get_cursor() {
    let mut state = CheckpointState::default();
    let cursor = StreamCursor {
        next_token: Some("token-123".to_string()),
        next_start_time_ms: Some(1733900000000),
    };

    state.update_cursor("/ecs/my-service", cursor.clone());

    let retrieved = state.cursor_for("/ecs/my-service");
    assert_eq!(retrieved.next_token, Some("token-123".to_string()));
    assert_eq!(retrieved.next_start_time_ms, Some(1733900000000));
}

#[test]
fn test_save_and_load_checkpoint() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let mut state = CheckpointState::default();
    state.update_cursor(
        "/ecs/service-1",
        StreamCursor {
            next_token: Some("tok1".to_string()),
            next_start_time_ms: Some(1000),
        },
    );
    state.update_cursor(
        "/ecs/service-2",
        StreamCursor {
            next_token: None,
            next_start_time_ms: Some(2000),
        },
    );

    state.save(&path).unwrap();
    assert!(path.exists());

    let loaded = CheckpointState::load(&path).unwrap();
    assert_eq!(loaded.streams.len(), 2);

    let c1 = loaded.cursor_for("/ecs/service-1");
    assert_eq!(c1.next_token, Some("tok1".to_string()));
    assert_eq!(c1.next_start_time_ms, Some(1000));

    let c2 = loaded.cursor_for("/ecs/service-2");
    assert!(c2.next_token.is_none());
    assert_eq!(c2.next_start_time_ms, Some(2000));
}

#[test]
fn test_load_nonexistent_returns_default() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("does_not_exist.json");

    let state = CheckpointState::load(&path).unwrap();
    assert!(state.streams.is_empty());
}

#[test]
fn test_save_creates_parent_dirs() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("nested")
        .join("deeper")
        .join("checkpoints.json");

    let state = CheckpointState::default();
    state.save(&path).unwrap();
    assert!(path.exists());
}

#[test]
fn test_checkpoint_serialization_format() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("checkpoints.json");

    let mut state = CheckpointState::default();
    state.update_cursor(
        "test-stream",
        StreamCursor {
            next_token: Some("my-token".to_string()),
            next_start_time_ms: Some(12345),
        },
    );

    state.save(&path).unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(content.contains("test-stream"));
    assert!(content.contains("my-token"));
    assert!(content.contains("12345"));
}

#[test]
fn test_update_cursor_overwrites() {
    let mut state = CheckpointState::default();

    state.update_cursor(
        "stream",
        StreamCursor {
            next_token: Some("first".to_string()),
            next_start_time_ms: Some(100),
        },
    );

    state.update_cursor(
        "stream",
        StreamCursor {
            next_token: Some("second".to_string()),
            next_start_time_ms: Some(200),
        },
    );

    let cursor = state.cursor_for("stream");
    assert_eq!(cursor.next_token, Some("second".to_string()));
    assert_eq!(cursor.next_start_time_ms, Some(200));
}
