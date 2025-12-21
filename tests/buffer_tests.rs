//! Tests for buffer capacity calculations.

use logstream::buffer::{
    derive_buffer_capacities, scaled_capacity, total_buffer_memory, validate_capacities,
    BufferCapacities, BACKFILL_FLOOR, BACKFILL_INFLIGHT_MULTIPLIER, HEAL_FLOOR,
    HEAL_INFLIGHT_MULTIPLIER, RAW_TAIL_FLOOR, RAW_TAIL_INFLIGHT_MULTIPLIER, RECONCILE_FLOOR,
    RECONCILE_INFLIGHT_MULTIPLIER, RESYNC_PER_SAMPLE_FLOOR, RESYNC_PER_SAMPLE_MULTIPLIER,
    SINK_ENRICHED_MULTIPLIER,
};
use logstream::config::Config;
use std::path::PathBuf;

fn test_config(batch_size: usize, max_in_flight: usize) -> Config {
    Config {
        log_group: "test-group".into(),
        log_groups: vec![],
        region: "us-east-1".into(),
        index_prefix: "logs".into(),
        enable_es_bootstrap: true,
        es_target_replicas: 0,
        ilm_rollover_max_age: "1d".into(),
        ilm_rollover_max_primary_shard_size: "25gb".into(),
        ilm_enable_delete_phase: false,
        ilm_delete_min_age: "30d".into(),
        batch_size,
        max_in_flight,
        poll_interval_secs: 5,
        reconcile_interval_secs: 300,
        backfill_days: 0,
        checkpoint_path: PathBuf::from("/tmp/checkpoint.json"),
        http_timeout_secs: 30,
        backoff_base_ms: 100,
        backoff_max_ms: 10000,
    }
}

/// Default test config.
fn default_test_config() -> Config {
    test_config(1000, 10)
}

// ============================================================================
// Constant Tests
// ============================================================================

#[test]
fn test_constants_are_positive() {
    // Use runtime values to avoid clippy assertions_on_constants
    let sink = SINK_ENRICHED_MULTIPLIER;
    let tail = RAW_TAIL_INFLIGHT_MULTIPLIER;
    let tail_floor = RAW_TAIL_FLOOR;
    let reconcile = RECONCILE_INFLIGHT_MULTIPLIER;
    let reconcile_floor = RECONCILE_FLOOR;
    let heal = HEAL_INFLIGHT_MULTIPLIER;
    let heal_floor = HEAL_FLOOR;
    let backfill = BACKFILL_INFLIGHT_MULTIPLIER;
    let backfill_floor = BACKFILL_FLOOR;
    let resync = RESYNC_PER_SAMPLE_MULTIPLIER;
    let resync_floor = RESYNC_PER_SAMPLE_FLOOR;

    assert!(sink > 0);
    assert!(tail > 0);
    assert!(tail_floor > 0);
    assert!(reconcile > 0);
    assert!(reconcile_floor > 0);
    assert!(heal > 0);
    assert!(heal_floor > 0);
    assert!(backfill > 0);
    assert!(backfill_floor > 0);
    assert!(resync > 0);
    assert!(resync_floor > 0);
}

#[test]
fn test_backfill_multiplier_is_largest() {
    // Backfill needs the most buffer capacity
    let backfill = BACKFILL_INFLIGHT_MULTIPLIER;
    let tail = RAW_TAIL_INFLIGHT_MULTIPLIER;
    let reconcile = RECONCILE_INFLIGHT_MULTIPLIER;
    let heal = HEAL_INFLIGHT_MULTIPLIER;

    assert!(backfill >= tail);
    assert!(backfill >= reconcile);
    assert!(backfill >= heal);
}

// ============================================================================
// scaled_capacity Tests
// ============================================================================

#[test]
fn test_scaled_capacity_basic() {
    // 100 * 5 = 500, which is > floor (1024)? No, so use floor
    let result = scaled_capacity(100, 5, 1024, 0);
    assert_eq!(result, 1024);
}

#[test]
fn test_scaled_capacity_uses_multiplier() {
    // 1000 * 5 = 5000, which is > floor (1024) and > baseline (0)
    let result = scaled_capacity(1000, 5, 1024, 0);
    assert_eq!(result, 5000);
}

#[test]
fn test_scaled_capacity_uses_baseline() {
    // 100 * 5 = 500, floor = 1024, baseline = 2048 -> use baseline
    let result = scaled_capacity(100, 5, 1024, 2048);
    assert_eq!(result, 2048);
}

#[test]
fn test_scaled_capacity_uses_floor() {
    // 10 * 5 = 50, floor = 100, baseline = 0 -> use floor
    let result = scaled_capacity(10, 5, 100, 0);
    assert_eq!(result, 100);
}

#[test]
fn test_scaled_capacity_zero_inflight() {
    let result = scaled_capacity(0, 5, 100, 50);
    assert_eq!(result, 100);
}

#[test]
fn test_scaled_capacity_saturating_mul() {
    // Very large values should not overflow
    let result = scaled_capacity(usize::MAX / 2, 3, 100, 0);
    assert!(result > 0);
}

// ============================================================================
// derive_buffer_capacities Tests
// ============================================================================

#[test]
fn test_derive_buffer_capacities_default_config() {
    let cfg = default_test_config();
    let caps = derive_buffer_capacities(&cfg);

    // All capacities should be > 0
    assert!(caps.sink_enriched > 0);
    assert!(caps.tail_raw > 0);
    assert!(caps.reconcile_raw > 0);
    assert!(caps.backfill_raw > 0);
    assert!(caps.heal_raw > 0);
    assert!(caps.resync_per_sample > 0);
}

#[test]
fn test_derive_buffer_capacities_sink_enriched() {
    let cfg = test_config(1000, 10);
    let caps = derive_buffer_capacities(&cfg);

    assert_eq!(caps.sink_enriched, 1000 * SINK_ENRICHED_MULTIPLIER);
}

#[test]
fn test_derive_buffer_capacities_respects_floors() {
    let cfg = test_config(1, 1);
    let caps = derive_buffer_capacities(&cfg);

    assert!(caps.tail_raw >= RAW_TAIL_FLOOR);
    assert!(caps.reconcile_raw >= RECONCILE_FLOOR);
    assert!(caps.heal_raw >= HEAL_FLOOR);
    assert!(caps.backfill_raw >= BACKFILL_FLOOR);
    assert!(caps.resync_per_sample >= RESYNC_PER_SAMPLE_FLOOR);
}

#[test]
fn test_derive_buffer_capacities_large_batch() {
    let cfg = test_config(50000, 10);
    let caps = derive_buffer_capacities(&cfg);

    // With large batch, capacities should exceed floors
    assert!(caps.tail_raw > RAW_TAIL_FLOOR);
    assert!(caps.reconcile_raw > RECONCILE_FLOOR);
    assert!(caps.backfill_raw > BACKFILL_FLOOR);
}

#[test]
fn test_derive_buffer_capacities_zero_batch_size() {
    let cfg = test_config(0, 10);
    let caps = derive_buffer_capacities(&cfg);

    // Should still have valid capacities (uses max(1))
    assert!(caps.sink_enriched >= SINK_ENRICHED_MULTIPLIER);
}

#[test]
fn test_derive_buffer_capacities_relationships() {
    let cfg = default_test_config();
    let caps = derive_buffer_capacities(&cfg);

    // Backfill should be at least as large as reconcile (it uses reconcile as baseline)
    assert!(caps.backfill_raw >= caps.reconcile_raw);
}

// ============================================================================
// BufferCapacities Tests
// ============================================================================

#[test]
fn test_buffer_capacities_default_for_testing() {
    let caps = BufferCapacities::default_for_testing();

    assert!(caps.sink_enriched > 0);
    assert!(caps.tail_raw > 0);
    assert!(caps.reconcile_raw > 0);
    assert!(caps.backfill_raw > 0);
    assert!(caps.heal_raw > 0);
    assert!(caps.resync_per_sample > 0);
}

#[test]
fn test_buffer_capacities_equality() {
    let caps1 = BufferCapacities::default_for_testing();
    let caps2 = BufferCapacities::default_for_testing();
    assert_eq!(caps1, caps2);
}

#[test]
fn test_buffer_capacities_clone() {
    let caps = BufferCapacities::default_for_testing();
    let cloned = caps;
    assert_eq!(caps, cloned);
}

// ============================================================================
// total_buffer_memory Tests
// ============================================================================

#[test]
fn test_total_buffer_memory() {
    let caps = BufferCapacities {
        sink_enriched: 1000,
        tail_raw: 1000,
        reconcile_raw: 1000,
        backfill_raw: 1000,
        heal_raw: 1000,
        resync_per_sample: 100,
    };

    let memory = total_buffer_memory(&caps);
    // (1000 + 1000 + 1000 + 1000 + 1000 + 100) * 1024 = 5100 * 1024
    assert_eq!(memory, 5100 * 1024);
}

#[test]
fn test_total_buffer_memory_default() {
    let caps = BufferCapacities::default_for_testing();
    let memory = total_buffer_memory(&caps);
    assert!(memory > 0);
}

// ============================================================================
// validate_capacities Tests
// ============================================================================

#[test]
fn test_validate_capacities_valid() {
    let caps = BufferCapacities::default_for_testing();
    assert!(validate_capacities(&caps).is_ok());
}

#[test]
fn test_validate_capacities_zero_sink_enriched() {
    let caps = BufferCapacities {
        sink_enriched: 0,
        ..BufferCapacities::default_for_testing()
    };
    assert!(validate_capacities(&caps).is_err());
}

#[test]
fn test_validate_capacities_zero_tail_raw() {
    let caps = BufferCapacities {
        tail_raw: 0,
        ..BufferCapacities::default_for_testing()
    };
    assert!(validate_capacities(&caps).is_err());
}

#[test]
fn test_validate_capacities_excessive_sink() {
    let caps = BufferCapacities {
        sink_enriched: 100_000_000,
        ..BufferCapacities::default_for_testing()
    };
    assert!(validate_capacities(&caps).is_err());
}

#[test]
fn test_validate_capacities_excessive_backfill() {
    let caps = BufferCapacities {
        backfill_raw: 100_000_000,
        ..BufferCapacities::default_for_testing()
    };
    assert!(validate_capacities(&caps).is_err());
}
