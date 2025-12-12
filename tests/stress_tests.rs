//! Tests for unified stress tracking.

use logstream::stress::{CombinedStressChecker, StressConfig, StressLevel, StressTracker};
use std::sync::Arc;

// ============================================================================
// Basic Tracker Tests
// ============================================================================

#[test]
fn test_stress_tracker_new() {
    let tracker = StressTracker::with_config(StressConfig::CLOUDWATCH);
    assert_eq!(tracker.stress_level(), StressLevel::Normal);
    assert_eq!(tracker.failure_streak(), 0);
    assert_eq!(tracker.total_failures(), 0);
}

#[test]
fn test_stress_tracker_for_es() {
    let tracker = StressTracker::for_es();
    assert_eq!(tracker.stress_level(), StressLevel::Normal);
    assert_eq!(tracker.backoff_duration().as_secs(), 3); // ES min backoff
}

#[test]
fn test_stress_tracker_for_cloudwatch() {
    let tracker = StressTracker::for_cloudwatch();
    assert_eq!(tracker.stress_level(), StressLevel::Normal);
    assert_eq!(tracker.backoff_duration().as_secs(), 1); // CW min backoff
}

#[test]
fn test_stress_tracker_failure_increases_streak() {
    let tracker = StressTracker::for_cloudwatch();

    tracker.record_failure();
    assert_eq!(tracker.failure_streak(), 1);
    assert_eq!(tracker.total_failures(), 1);

    tracker.record_failure();
    assert_eq!(tracker.failure_streak(), 2);
    assert_eq!(tracker.total_failures(), 2);
}

#[test]
fn test_stress_tracker_stress_levels() {
    let tracker = StressTracker::for_cloudwatch();

    // 1-2 failures: Normal
    tracker.record_failure();
    tracker.record_failure();
    assert_eq!(tracker.stress_level(), StressLevel::Normal);

    // 3+ failures: Elevated
    tracker.record_failure();
    assert_eq!(tracker.stress_level(), StressLevel::Elevated);

    // 10+ failures: Critical
    for _ in 0..7 {
        tracker.record_failure();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Critical);
}

#[test]
fn test_stress_tracker_exponential_backoff_cw() {
    let tracker = StressTracker::for_cloudwatch();

    assert_eq!(tracker.backoff_duration().as_secs(), 1);

    tracker.record_failure();
    assert_eq!(tracker.backoff_duration().as_secs(), 2);

    tracker.record_failure();
    assert_eq!(tracker.backoff_duration().as_secs(), 4);

    tracker.record_failure();
    assert_eq!(tracker.backoff_duration().as_secs(), 8);
}

#[test]
fn test_stress_tracker_exponential_backoff_es() {
    let tracker = StressTracker::for_es();

    assert_eq!(tracker.backoff_duration().as_secs(), 3);

    tracker.record_failure();
    assert_eq!(tracker.backoff_duration().as_secs(), 6);

    tracker.record_failure();
    assert_eq!(tracker.backoff_duration().as_secs(), 12);
}

#[test]
fn test_stress_tracker_backoff_caps_at_max() {
    let tracker = StressTracker::for_cloudwatch();

    // Trigger many failures to hit the cap
    for _ in 0..20 {
        tracker.record_failure();
    }

    // Should cap at 30 seconds (CW max)
    assert_eq!(tracker.backoff_duration().as_secs(), 30);
}

#[test]
fn test_stress_tracker_es_backoff_caps_at_max() {
    let tracker = StressTracker::for_es();

    // Trigger many failures to hit the cap
    for _ in 0..20 {
        tracker.record_failure();
    }

    // Should cap at 60 seconds (ES max)
    assert_eq!(tracker.backoff_duration().as_secs(), 60);
}

#[test]
fn test_stress_tracker_success_decays() {
    let tracker = StressTracker::for_cloudwatch();

    // Build up stress
    for _ in 0..5 {
        tracker.record_failure();
    }
    assert_eq!(tracker.failure_streak(), 5);
    assert_eq!(tracker.stress_level(), StressLevel::Elevated);

    // Success reduces streak
    tracker.record_success();
    assert_eq!(tracker.failure_streak(), 4);

    tracker.record_success();
    assert_eq!(tracker.failure_streak(), 3);
}

#[test]
fn test_stress_tracker_success_decays_backoff() {
    let tracker = StressTracker::for_cloudwatch();

    // Build up backoff
    for _ in 0..4 {
        tracker.record_failure();
    }
    assert_eq!(tracker.backoff_duration().as_secs(), 16);

    // Success halves backoff
    tracker.record_success();
    assert_eq!(tracker.backoff_duration().as_secs(), 8);

    tracker.record_success();
    assert_eq!(tracker.backoff_duration().as_secs(), 4);
}

#[test]
fn test_stress_tracker_success_floors_at_min() {
    let tracker = StressTracker::for_cloudwatch();

    // Many successes should not go below 1 second (CW min)
    for _ in 0..20 {
        tracker.record_success();
    }

    assert_eq!(tracker.backoff_duration().as_secs(), 1);
}

#[test]
fn test_stress_tracker_check_value_increased() {
    let tracker = StressTracker::for_es();

    // First check
    assert!(!tracker.check_value_increased(0));

    // Value increased
    assert!(tracker.check_value_increased(5));

    // Value same
    assert!(!tracker.check_value_increased(5));

    // Value increased again
    assert!(tracker.check_value_increased(10));
}

// ============================================================================
// Priority-Aware Pause Tests
// ============================================================================

#[test]
fn test_priority_pause_critical_never_pauses() {
    let tracker = StressTracker::for_cloudwatch();

    // Build up to critical stress
    for _ in 0..15 {
        tracker.record_failure();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Critical);

    // CRITICAL priority never pauses
    assert!(tracker.should_pause_for_priority(255).is_none());
    assert!(tracker.should_pause_for_priority(250).is_none());
}

#[test]
fn test_priority_pause_high_only_critical_stress() {
    let tracker = StressTracker::for_cloudwatch();

    // Elevated stress (3-9 failures)
    for _ in 0..5 {
        tracker.record_failure();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Elevated);

    // HIGH priority (192) doesn't pause under elevated
    assert!(tracker.should_pause_for_priority(192).is_none());
    assert!(tracker.should_pause_for_priority(180).is_none());

    // Build to critical
    for _ in 0..5 {
        tracker.record_failure();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Critical);

    // HIGH priority DOES pause under critical
    assert!(tracker.should_pause_for_priority(192).is_some());
    assert!(tracker.should_pause_for_priority(180).is_some());
}

#[test]
fn test_priority_pause_normal_elevated_stress() {
    let tracker = StressTracker::for_cloudwatch();

    // Normal stress (1-2 failures)
    tracker.record_failure();
    tracker.record_failure();
    assert_eq!(tracker.stress_level(), StressLevel::Normal);

    // NORMAL priority (128) doesn't pause under normal stress
    assert!(tracker.should_pause_for_priority(128).is_none());

    // Elevated stress
    tracker.record_failure();
    assert_eq!(tracker.stress_level(), StressLevel::Elevated);

    // NORMAL priority DOES pause under elevated
    assert!(tracker.should_pause_for_priority(128).is_some());
    assert!(tracker.should_pause_for_priority(100).is_some());
}

#[test]
fn test_priority_pause_low_any_stress() {
    let tracker = StressTracker::for_cloudwatch();

    // No failures - no pause
    assert!(tracker.should_pause_for_priority(64).is_none());

    // Single failure - LOW pauses
    tracker.record_failure();
    assert!(tracker.should_pause_for_priority(64).is_some());
    assert!(tracker.should_pause_for_priority(50).is_some());
}

#[test]
fn test_priority_pause_idle_most_aggressive() {
    let tracker = StressTracker::for_cloudwatch();

    // No failures - no pause
    assert!(tracker.should_pause_for_priority(0).is_none());
    assert!(tracker.should_pause_for_priority(49).is_none());

    // Single failure - IDLE pauses with 2x backoff
    tracker.record_failure();
    let pause = tracker.should_pause_for_priority(0);
    assert!(pause.is_some());

    // IDLE gets 2x the backoff
    let low_pause = tracker.should_pause_for_priority(64);
    assert!(pause.unwrap() > low_pause.unwrap());
}

#[test]
fn test_priority_pause_ordering() {
    let tracker = StressTracker::for_cloudwatch();

    // Create elevated stress
    for _ in 0..5 {
        tracker.record_failure();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Elevated);

    // Check ordering: lower priority pauses first
    assert!(tracker.should_pause_for_priority(255).is_none()); // CRITICAL - no pause
    assert!(tracker.should_pause_for_priority(192).is_none()); // HIGH - no pause (only critical)
    assert!(tracker.should_pause_for_priority(128).is_some()); // NORMAL - pauses
    assert!(tracker.should_pause_for_priority(64).is_some()); // LOW - pauses
    assert!(tracker.should_pause_for_priority(0).is_some()); // IDLE - pauses with 2x
}

// ============================================================================
// Real-World Scenario Tests
// ============================================================================

#[test]
fn test_stress_tracker_real_world_throttle_burst() {
    let tracker = StressTracker::for_cloudwatch();

    // Simulate a burst of throttling (CloudWatch rate limit hit)
    for _ in 0..10 {
        tracker.record_failure();
    }

    // Should be in critical state
    assert_eq!(tracker.stress_level(), StressLevel::Critical);

    // Real-time tail (255) continues
    assert!(tracker.should_pause_for_priority(255).is_none());

    // Backfill (low priority ~64) pauses
    assert!(tracker.should_pause_for_priority(64).is_some());

    // After many successful calls, stress should decay
    for _ in 0..10 {
        tracker.record_success();
    }
    assert_eq!(tracker.stress_level(), StressLevel::Normal);

    // Backfill can now resume
    assert!(tracker.should_pause_for_priority(64).is_none());
}

#[test]
fn test_stress_tracker_intermittent_failures() {
    let tracker = StressTracker::for_cloudwatch();

    // Simulate intermittent failures
    tracker.record_failure();
    tracker.record_success();
    tracker.record_success();

    // Streak should be 0 after 2 successes
    assert_eq!(tracker.failure_streak(), 0);
    assert_eq!(tracker.stress_level(), StressLevel::Normal);

    // All priorities can work
    assert!(tracker.should_pause_for_priority(0).is_none());
}

#[test]
fn test_stress_tracker_total_failures_never_decreases() {
    let tracker = StressTracker::for_cloudwatch();

    tracker.record_failure();
    tracker.record_failure();
    tracker.record_failure();
    assert_eq!(tracker.total_failures(), 3);

    // Success doesn't decrease total
    tracker.record_success();
    tracker.record_success();
    assert_eq!(tracker.total_failures(), 3);

    // More failures add to total
    tracker.record_failure();
    assert_eq!(tracker.total_failures(), 4);
}

// ============================================================================
// Combined Stress Checker Tests
// ============================================================================

#[test]
fn test_combined_stress_checker_empty() {
    let checker = CombinedStressChecker::new();

    // Empty checker never pauses
    assert!(checker.should_pause_for_priority(0).is_none());
    assert_eq!(checker.max_stress_level(), StressLevel::Normal);
}

#[test]
fn test_combined_stress_checker_single_tracker() {
    let tracker = Arc::new(StressTracker::for_cloudwatch());

    // Build stress
    for _ in 0..5 {
        tracker.record_failure();
    }

    let checker = CombinedStressChecker::from_trackers([tracker]);

    assert_eq!(checker.max_stress_level(), StressLevel::Elevated);
    assert!(checker.should_pause_for_priority(128).is_some());
}

#[test]
fn test_combined_stress_checker_multiple_trackers() {
    let cw_tracker = Arc::new(StressTracker::for_cloudwatch());
    let es_tracker = Arc::new(StressTracker::for_es());

    // CW is stressed, ES is not
    for _ in 0..5 {
        cw_tracker.record_failure();
    }

    let checker = CombinedStressChecker::from_trackers([cw_tracker, es_tracker.clone()]);

    // Max stress is from CW
    assert_eq!(checker.max_stress_level(), StressLevel::Elevated);
    assert!(checker.should_pause_for_priority(128).is_some());

    // Now stress ES to critical
    for _ in 0..12 {
        es_tracker.record_failure();
    }

    // Max stress is now from ES
    assert_eq!(checker.max_stress_level(), StressLevel::Critical);
}

#[test]
fn test_combined_stress_checker_takes_longest_pause() {
    let cw_tracker = Arc::new(StressTracker::for_cloudwatch());
    let es_tracker = Arc::new(StressTracker::for_es());

    // Both stressed to same level but different backoffs
    for _ in 0..5 {
        cw_tracker.record_failure();
        es_tracker.record_failure();
    }

    let checker = CombinedStressChecker::from_trackers([cw_tracker.clone(), es_tracker.clone()]);

    // ES has longer backoff (starts at 3s, CW at 1s)
    let pause = checker.should_pause_for_priority(64).unwrap();
    assert!(pause >= es_tracker.backoff_duration());
}

#[test]
fn test_combined_stress_checker_pauses_if_any_stressed() {
    let cw_tracker = Arc::new(StressTracker::for_cloudwatch());
    let es_tracker = Arc::new(StressTracker::for_es());

    // Only CW is stressed
    for _ in 0..5 {
        cw_tracker.record_failure();
    }

    let checker = CombinedStressChecker::from_trackers([cw_tracker, es_tracker]);

    // Should pause even though ES is fine
    assert!(checker.should_pause_for_priority(128).is_some());
}

// ============================================================================
// StressConfig Tests
// ============================================================================

#[test]
fn test_stress_config_es_values() {
    let config = StressConfig::ES;
    assert_eq!(config.min_backoff_secs, 3);
    assert_eq!(config.max_backoff_secs, 60);
    assert_eq!(config.backoff_multiplier, 2);
    assert_eq!(config.elevated_threshold, 3);
    assert_eq!(config.critical_threshold, 10);
}

#[test]
fn test_stress_config_cloudwatch_values() {
    let config = StressConfig::CLOUDWATCH;
    assert_eq!(config.min_backoff_secs, 1);
    assert_eq!(config.max_backoff_secs, 30);
    assert_eq!(config.backoff_multiplier, 2);
    assert_eq!(config.elevated_threshold, 3);
    assert_eq!(config.critical_threshold, 10);
}

#[test]
fn test_stress_level_from_u8() {
    assert_eq!(StressLevel::from(0), StressLevel::Normal);
    assert_eq!(StressLevel::from(1), StressLevel::Elevated);
    assert_eq!(StressLevel::from(2), StressLevel::Critical);
    assert_eq!(StressLevel::from(99), StressLevel::Normal); // Unknown defaults to Normal
}
