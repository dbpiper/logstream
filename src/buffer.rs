//! Buffer capacity calculations for log processing pipelines.
//! Derives optimal buffer sizes based on batch size and inflight limits.

use crate::config::Config;

// ============================================================================
// Constants
// ============================================================================

/// Multiplier for sink enriched buffer relative to batch size.
pub const SINK_ENRICHED_MULTIPLIER: usize = 4;

/// Multiplier for raw tail buffer relative to inflight.
pub const RAW_TAIL_INFLIGHT_MULTIPLIER: usize = 5;

/// Minimum floor for raw tail buffer.
pub const RAW_TAIL_FLOOR: usize = 1024;

/// Multiplier for reconcile buffer relative to inflight.
pub const RECONCILE_INFLIGHT_MULTIPLIER: usize = 4;

/// Minimum floor for reconcile buffer.
pub const RECONCILE_FLOOR: usize = 2048;

/// Multiplier for heal buffer relative to inflight.
pub const HEAL_INFLIGHT_MULTIPLIER: usize = 4;

/// Minimum floor for heal buffer.
pub const HEAL_FLOOR: usize = 2048;

/// Multiplier for backfill buffer relative to inflight.
pub const BACKFILL_INFLIGHT_MULTIPLIER: usize = 10;

/// Minimum floor for backfill buffer.
pub const BACKFILL_FLOOR: usize = 4096;

/// Multiplier for resync per-sample buffer.
pub const RESYNC_PER_SAMPLE_MULTIPLIER: usize = 8;

/// Minimum floor for resync per-sample buffer.
pub const RESYNC_PER_SAMPLE_FLOOR: usize = 16;

// ============================================================================
// Types
// ============================================================================

/// Calculated buffer capacities for all processing pipelines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferCapacities {
    /// Capacity for enriched events going to sink.
    pub sink_enriched: usize,
    /// Capacity for raw events from tail.
    pub tail_raw: usize,
    /// Capacity for raw events in reconcile.
    pub reconcile_raw: usize,
    /// Capacity for raw events in backfill.
    pub backfill_raw: usize,
    /// Capacity for raw events in heal.
    pub heal_raw: usize,
    /// Capacity for resync per-sample buffer.
    pub resync_per_sample: usize,
}

impl BufferCapacities {
    /// Create default capacities (for testing).
    pub fn default_for_testing() -> Self {
        Self {
            sink_enriched: 4000,
            tail_raw: 5120,
            reconcile_raw: 4096,
            backfill_raw: 10240,
            heal_raw: 4096,
            resync_per_sample: 128,
        }
    }
}

// ============================================================================
// Functions
// ============================================================================

/// Derive buffer capacities from configuration.
/// Uses batch size and max inflight to calculate appropriate buffer sizes.
pub fn derive_buffer_capacities(cfg: &Config) -> BufferCapacities {
    let base_batch = cfg.batch_size.max(1);
    let inflight = base_batch.saturating_mul(cfg.max_in_flight.max(1));
    let sink_enriched = base_batch.saturating_mul(SINK_ENRICHED_MULTIPLIER);

    let tail_raw = scaled_capacity(
        inflight,
        RAW_TAIL_INFLIGHT_MULTIPLIER,
        RAW_TAIL_FLOOR,
        sink_enriched,
    );
    let reconcile_raw = scaled_capacity(
        inflight,
        RECONCILE_INFLIGHT_MULTIPLIER,
        RECONCILE_FLOOR,
        sink_enriched,
    );
    let heal_raw = scaled_capacity(
        inflight,
        HEAL_INFLIGHT_MULTIPLIER,
        HEAL_FLOOR,
        sink_enriched,
    );
    let backfill_raw = scaled_capacity(
        inflight,
        BACKFILL_INFLIGHT_MULTIPLIER,
        BACKFILL_FLOOR,
        reconcile_raw,
    );
    let resync_per_sample = cfg
        .max_in_flight
        .max(1)
        .saturating_mul(RESYNC_PER_SAMPLE_MULTIPLIER)
        .max(RESYNC_PER_SAMPLE_FLOOR);

    BufferCapacities {
        sink_enriched,
        tail_raw,
        reconcile_raw,
        backfill_raw,
        heal_raw,
        resync_per_sample,
    }
}

/// Calculate scaled capacity with floor and baseline constraints.
/// Returns: max(inflight * multiplier, floor, baseline)
pub fn scaled_capacity(inflight: usize, multiplier: usize, floor: usize, baseline: usize) -> usize {
    inflight.saturating_mul(multiplier).max(floor).max(baseline)
}

/// Calculate total buffer memory usage in bytes (assuming 1KB per event).
pub fn total_buffer_memory(caps: &BufferCapacities) -> usize {
    const EVENT_SIZE_BYTES: usize = 1024;
    (caps.sink_enriched
        + caps.tail_raw
        + caps.reconcile_raw
        + caps.backfill_raw
        + caps.heal_raw
        + caps.resync_per_sample)
        * EVENT_SIZE_BYTES
}

/// Validate buffer capacities are within reasonable bounds.
pub fn validate_capacities(caps: &BufferCapacities) -> Result<(), &'static str> {
    const MAX_BUFFER: usize = 10_000_000; // 10M events

    if caps.sink_enriched == 0 {
        return Err("sink_enriched must be > 0");
    }
    if caps.tail_raw == 0 {
        return Err("tail_raw must be > 0");
    }
    if caps.sink_enriched > MAX_BUFFER {
        return Err("sink_enriched exceeds maximum");
    }
    if caps.backfill_raw > MAX_BUFFER {
        return Err("backfill_raw exceeds maximum");
    }

    Ok(())
}
