use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PruneWatermarksFile {
    #[serde(default)]
    pub by_stable: BTreeMap<String, PruneWatermark>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct PruneWatermark {
    pub min_supported_timestamp_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone)]
pub struct PruneState {
    path: Arc<PathBuf>,
    inner: Arc<RwLock<PruneWatermarksFile>>,
}

impl PruneState {
    pub fn load_or_new(path: PathBuf) -> Result<Self> {
        let file = load_file_or_default(&path)?;
        Ok(Self {
            path: Arc::new(path),
            inner: Arc::new(RwLock::new(file)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn min_supported_ms(&self, stable: &str) -> i64 {
        let guard = self.inner.read().await;
        guard
            .by_stable
            .get(stable)
            .map(|w| w.min_supported_timestamp_ms)
            .unwrap_or(0)
    }

    pub async fn update_monotonic(
        &self,
        stable: &str,
        new_min_supported_ms: i64,
        now_ms: i64,
    ) -> Result<bool> {
        let mut guard = self.inner.write().await;
        let entry = guard.by_stable.entry(stable.to_string()).or_default();
        let next = entry.min_supported_timestamp_ms.max(new_min_supported_ms);
        if next == entry.min_supported_timestamp_ms {
            return Ok(false);
        }
        entry.min_supported_timestamp_ms = next;
        entry.updated_at_ms = now_ms;
        save_file(&guard, &self.path)?;
        Ok(true)
    }

    pub async fn apply_window(
        &self,
        stable: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Option<(i64, i64)> {
        let min_ms = self.min_supported_ms(stable).await;
        if min_ms <= 0 {
            return Some((start_ms, end_ms));
        }
        if end_ms <= min_ms {
            return None;
        }
        Some((start_ms.max(min_ms), end_ms))
    }
}

fn load_file_or_default(path: &Path) -> Result<PruneWatermarksFile> {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok(PruneWatermarksFile::default())
        }
        Err(err) => return Err(err).context("reading prune watermarks file"),
    };
    serde_json::from_slice(&data).context("parsing prune watermarks file")
}

fn save_file(file: &PruneWatermarksFile, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("creating prune state directory")?;
    }
    let tmp = path.with_extension("tmp");
    let data = serde_json::to_vec_pretty(file).context("serializing prune watermarks")?;
    fs::write(&tmp, data).context("writing temp prune watermarks")?;
    fs::rename(&tmp, path).context("replacing prune watermarks")?;
    Ok(())
}
