//! Checkpoint path utilities.
//! Handles checkpoint file naming and path resolution for log groups.

use std::path::{Path, PathBuf};

/// Generate checkpoint file path for a log group.
/// Sanitizes the log group name to create a valid filename.
pub fn checkpoint_path_for(base: &Path, group: &str) -> PathBuf {
    let slug = sanitize_group_name(group);

    if base.extension().is_some() {
        // Base is a file path, use parent directory
        base.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("checkpoints-{slug}.json"))
    } else {
        // Base is a directory
        base.join(format!("checkpoints-{slug}.json"))
    }
}

/// Sanitize a log group name to be safe for filenames.
/// Replaces non-alphanumeric characters with hyphens.
pub fn sanitize_group_name(group: &str) -> String {
    group
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Extract log group name from checkpoint filename.
/// Returns None if the filename doesn't match the expected pattern.
pub fn group_from_checkpoint_path(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;

    if !filename.starts_with("checkpoints-") || !filename.ends_with(".json") {
        return None;
    }

    let slug = filename
        .strip_prefix("checkpoints-")?
        .strip_suffix(".json")?;

    // Can't reliably reverse the sanitization, but return the slug
    Some(slug.to_string())
}

/// Check if a path looks like a checkpoint file.
pub fn is_checkpoint_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.starts_with("checkpoints-") && n.ends_with(".json"))
        .unwrap_or(false)
}

/// List all checkpoint files in a directory.
pub fn list_checkpoint_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(dir)?;
    let checkpoints: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| is_checkpoint_file(p))
        .collect();
    Ok(checkpoints)
}
