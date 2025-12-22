use std::sync::Arc;

use logstream::prune_state::PruneState;
use tempfile::TempDir;

#[tokio::test]
async fn prune_state_update_is_monotonic() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("prune_watermarks.json");
    let state = PruneState::load_or_new(path).unwrap();

    let stable = "cloudwatch-ecs-test";
    let updated = state.update_monotonic(stable, 100, 1).await.unwrap();
    assert!(updated);
    assert_eq!(state.min_supported_ms(stable).await, 100);

    let updated = state.update_monotonic(stable, 50, 2).await.unwrap();
    assert!(!updated);
    assert_eq!(state.min_supported_ms(stable).await, 100);

    let updated = state.update_monotonic(stable, 150, 3).await.unwrap();
    assert!(updated);
    assert_eq!(state.min_supported_ms(stable).await, 150);

    let state2 = PruneState::load_or_new(state.path().to_path_buf()).unwrap();
    assert_eq!(state2.min_supported_ms(stable).await, 150);
}

#[tokio::test]
async fn prune_state_clamps_or_skips_windows() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("prune_watermarks.json");
    let state = Arc::new(PruneState::load_or_new(path).unwrap());

    let stable = "cloudwatch-ecs-test";
    let _ = state.update_monotonic(stable, 1_000, 1).await.unwrap();

    assert_eq!(
        state.apply_window(stable, 1_000, 2_000).await,
        Some((1_000, 2_000))
    );
    assert_eq!(
        state.apply_window(stable, 500, 2_000).await,
        Some((1_000, 2_000))
    );
    assert_eq!(state.apply_window(stable, 0, 1_000).await, None);
}
