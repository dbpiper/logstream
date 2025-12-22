use std::sync::Arc;

use crate::enrich::sanitize_log_group_name;

pub fn stable_alias(index_prefix: &str, log_group: &str) -> Arc<str> {
    Arc::<str>::from(format!(
        "{}-{}",
        index_prefix,
        sanitize_log_group_name(log_group)
    ))
}

pub fn stable_alias_from_slug(index_prefix: &str, slug: &str) -> Arc<str> {
    Arc::<str>::from(format!("{}-{}", index_prefix, slug))
}

pub fn versioned_streams(stable_alias: &str) -> [Arc<str>; 2] {
    [
        Arc::<str>::from(format!("{stable_alias}-v1")),
        Arc::<str>::from(format!("{stable_alias}-v2")),
    ]
}
