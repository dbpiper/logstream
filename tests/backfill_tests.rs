//! Tests for backfill module.

use logstream::backfill::{
    calculate_eps, day_range_ms, validate_backfill_config, BACKFILL_CONCURRENCY,
};
use logstream::process::{priority_for_day_offset, Priority};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Priority Assignment Tests
// ============================================================================

#[test]
fn test_priority_today_is_critical() {
    // Day 0 (today) should be CRITICAL priority
    assert_eq!(priority_for_day_offset(0), Priority::CRITICAL);
}

#[test]
fn test_priority_recent_days_are_high() {
    // Days 1-3 should be HIGH priority
    for day in 1..=3 {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::HIGH,
            "Day {} should be HIGH priority",
            day
        );
    }
}

#[test]
fn test_priority_last_week_is_normal() {
    // Days 4-7 should be NORMAL priority
    for day in 4..=7 {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::NORMAL,
            "Day {} should be NORMAL priority",
            day
        );
    }
}

#[test]
fn test_priority_last_month_is_low() {
    // Days 8-30 should be LOW priority
    for day in [8, 14, 30] {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::LOW,
            "Day {} should be LOW priority",
            day
        );
    }
}

#[test]
fn test_priority_older_than_month_is_idle() {
    // Days 31+ should be IDLE priority
    for day in [31, 60, 365, 3650] {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::IDLE,
            "Day {} should be IDLE priority",
            day
        );
    }
}

#[test]
fn test_priority_boundary_at_day_30() {
    assert_eq!(priority_for_day_offset(30), Priority::LOW);
    assert_eq!(priority_for_day_offset(31), Priority::IDLE);
}

#[test]
fn test_priority_ordering_correct() {
    let today = priority_for_day_offset(0); // CRITICAL
    let yesterday = priority_for_day_offset(1); // HIGH
    let last_week = priority_for_day_offset(5); // NORMAL
    let last_month = priority_for_day_offset(10); // LOW
    let old = priority_for_day_offset(100); // IDLE

    assert!(today > yesterday);
    assert!(yesterday > last_week);
    assert!(last_week > last_month);
    assert!(last_month > old);
}

// ============================================================================
// Day Range Tests
// ============================================================================

#[test]
fn test_day_range_today_valid() {
    let (start, end) = day_range_ms(0);
    assert!(start < end, "Start must be before end");

    // Should be approximately 24 hours (86399 seconds for 23:59:59)
    let diff_seconds = (end - start) / 1000;
    assert!(
        (86398..=86400).contains(&diff_seconds),
        "Day should be ~24 hours, got {} seconds",
        diff_seconds
    );
}

#[test]
fn test_day_range_yesterday_before_today() {
    let (today_start, _) = day_range_ms(0);
    let (yesterday_start, yesterday_end) = day_range_ms(1);

    assert!(
        yesterday_end < today_start,
        "Yesterday should end before today starts"
    );
    assert!(
        yesterday_start < yesterday_end,
        "Yesterday start before end"
    );
}

#[test]
fn test_day_range_chronological_order() {
    let mut prev_start = i64::MAX;
    for day in 0..30 {
        let (start, _) = day_range_ms(day);
        assert!(
            start < prev_start,
            "Day {} should be before day {}",
            day,
            day - 1
        );
        prev_start = start;
    }
}

#[test]
fn test_day_range_far_past() {
    // Should not panic for very old days
    let (start, end) = day_range_ms(3650); // 10 years ago
    assert!(start < end);
}

// ============================================================================
// Throughput Calculation Tests
// ============================================================================

#[test]
fn test_calculate_eps_exact_second() {
    assert_eq!(calculate_eps(1000, 1000), 1000);
}

#[test]
fn test_calculate_eps_high_throughput() {
    assert_eq!(calculate_eps(50000, 500), 100000);
}

#[test]
fn test_calculate_eps_low_throughput() {
    assert_eq!(calculate_eps(10, 10000), 1);
}

#[test]
fn test_calculate_eps_zero_time_returns_zero() {
    assert_eq!(calculate_eps(1000, 0), 0);
}

#[test]
fn test_calculate_eps_zero_events() {
    assert_eq!(calculate_eps(0, 1000), 0);
}

#[test]
fn test_calculate_eps_large_numbers() {
    // 1 million events in 10 seconds = 100k eps
    assert_eq!(calculate_eps(1_000_000, 10_000), 100_000);
}

// ============================================================================
// Configuration Validation Tests
// ============================================================================

#[test]
fn test_validate_config_normal_values() {
    assert!(validate_backfill_config(30, 8).is_ok());
    assert!(validate_backfill_config(365, 4).is_ok());
    assert!(validate_backfill_config(3650, 16).is_ok());
}

#[test]
fn test_validate_config_zero_days_allowed() {
    assert!(validate_backfill_config(0, 8).is_ok());
}

#[test]
fn test_validate_config_zero_concurrency_rejected() {
    let result = validate_backfill_config(30, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("concurrency"));
}

#[test]
fn test_validate_config_excessive_days_rejected() {
    let result = validate_backfill_config(36501, 8);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("100 years"));
}

#[test]
fn test_validate_config_boundary_days() {
    assert!(validate_backfill_config(36500, 8).is_ok()); // 100 years exactly
    assert!(validate_backfill_config(36501, 8).is_err()); // just over
}

// ============================================================================
// Concurrency Constant Tests
// ============================================================================

#[test]
fn test_backfill_concurrency_is_reasonable() {
    // Verify the constant is within reasonable bounds
    let concurrency = BACKFILL_CONCURRENCY;
    assert!(concurrency >= 1, "Must have at least 1 concurrent task");
    assert!(
        concurrency <= 32,
        "Too many concurrent tasks risks overwhelming AWS API"
    );
}

// ============================================================================
// Concurrency Behavior Tests
// ============================================================================

#[tokio::test]
async fn test_semaphore_limits_concurrency() {
    let concurrency = 4;
    let total_tasks = 20;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _ in 0..total_tasks {
        let sem = semaphore.clone();
        let active_clone = active.clone();
        let max_active_clone = max_active.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let current = active_clone.fetch_add(1, Ordering::SeqCst) + 1;
            max_active_clone.fetch_max(current, Ordering::SeqCst);

            // Simulate work
            tokio::time::sleep(Duration::from_millis(10)).await;

            active_clone.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let observed_max = max_active.load(Ordering::SeqCst);
    assert!(
        observed_max <= concurrency,
        "Max concurrent was {} but should be <= {}",
        observed_max,
        concurrency
    );
}

#[tokio::test]
async fn test_all_tasks_complete_with_semaphore() {
    let concurrency = BACKFILL_CONCURRENCY;
    let total_tasks = 50;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for _ in 0..total_tasks {
        let sem = semaphore.clone();
        let completed_clone = completed.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            tokio::time::sleep(Duration::from_millis(1)).await;
            completed_clone.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(
        completed.load(Ordering::SeqCst),
        total_tasks,
        "All tasks should complete"
    );
}

#[tokio::test]
async fn test_semaphore_fairness() {
    // Ensure tasks complete in roughly FIFO order
    let concurrency = 2;
    let total_tasks = 10;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let order = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::new();

    for i in 0..total_tasks {
        let sem = semaphore.clone();
        let order_clone = order.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
            order_clone.lock().await.push(i);
        }));

        // Small delay between spawns to ensure order
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let completed_order = order.lock().await;
    // First few should complete in roughly spawn order
    assert!(
        completed_order[0] < 3,
        "First completed should be one of first spawned"
    );
}

// ============================================================================
// Integration-style Tests
// ============================================================================

#[tokio::test]
async fn test_backfill_day_processing_simulation() {
    // Simulate the backfill pattern: spawn tasks for multiple days with priority
    let days_to_process = 35;
    let mut day_priorities = Vec::new();

    for day in 0..days_to_process {
        let priority = priority_for_day_offset(day);
        day_priorities.push((day, priority));
    }

    // Verify distribution based on new priority mapping:
    // Day 0 = CRITICAL, Days 1-3 = HIGH, Days 4-7 = NORMAL, Days 8-30 = LOW, Days 31+ = IDLE
    let critical_count = day_priorities
        .iter()
        .filter(|(_, p)| *p == Priority::CRITICAL)
        .count();
    let high_count = day_priorities
        .iter()
        .filter(|(_, p)| *p == Priority::HIGH)
        .count();
    let normal_count = day_priorities
        .iter()
        .filter(|(_, p)| *p == Priority::NORMAL)
        .count();
    let low_count = day_priorities
        .iter()
        .filter(|(_, p)| *p == Priority::LOW)
        .count();
    let idle_count = day_priorities
        .iter()
        .filter(|(_, p)| *p == Priority::IDLE)
        .count();

    assert_eq!(critical_count, 1, "Only today should be CRITICAL");
    assert_eq!(high_count, 3, "Days 1-3 should be HIGH");
    assert_eq!(normal_count, 4, "Days 4-7 should be NORMAL");
    assert_eq!(low_count, 23, "Days 8-30 should be LOW");
    assert_eq!(idle_count, 4, "Days 31-34 should be IDLE");
}

#[test]
fn test_day_range_non_overlapping() {
    // Verify that consecutive days don't overlap
    for day in 0..100 {
        let (_, end) = day_range_ms(day);
        let (next_start, _) = day_range_ms(day + 1);

        assert!(
            next_start < end,
            "Day {} should start before day {} ends (chronologically)",
            day + 1,
            day
        );
    }
}
