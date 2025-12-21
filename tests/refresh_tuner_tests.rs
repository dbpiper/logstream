use logstream::es_refresh_tuner::is_hot_index;

#[test]
fn is_hot_index_defaults_hot_when_no_bounds() {
    assert!(is_hot_index(None, 100));
}

#[test]
fn is_hot_index_hot_when_max_is_after_cutoff() {
    assert!(is_hot_index(Some(200), 100));
    assert!(is_hot_index(Some(100), 100));
}

#[test]
fn is_hot_index_cold_when_max_is_before_cutoff() {
    assert!(!is_hot_index(Some(99), 100));
}
