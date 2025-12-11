use std::{collections::HashMap, fs, io, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamCursor {
    pub next_token: Option<String>,
    pub next_start_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckpointState {
    pub streams: HashMap<String, StreamCursor>,
}

impl CheckpointState {
    pub fn load(path: &Path) -> Result<Self> {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => return Err(err).context("reading checkpoint file"),
        };
        let state: Self = serde_json::from_slice(&data).context("parsing checkpoint file")?;
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(self).context("serializing checkpoint")?;
        fs::write(&tmp, data).context("writing temp checkpoint")?;
        fs::rename(&tmp, path).context("replacing checkpoint")?;
        Ok(())
    }

    pub fn cursor_for(&self, stream: &str) -> StreamCursor {
        self.streams.get(stream).cloned().unwrap_or_default()
    }

    pub fn update_cursor(&mut self, stream: &str, cursor: StreamCursor) {
        self.streams.insert(stream.to_string(), cursor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let path = dir.path().join("checkpoints.json");

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
}
