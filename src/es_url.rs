use std::sync::Arc;

pub fn normalize_base_url(base_url: impl Into<Arc<str>>) -> Arc<str> {
    let base_url: Arc<str> = base_url.into();
    if base_url.ends_with('/') {
        Arc::<str>::from(base_url.trim_end_matches('/').to_string())
    } else {
        base_url
    }
}
