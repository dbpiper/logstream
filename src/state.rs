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
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("creating checkpoint directory")?;
        }
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
