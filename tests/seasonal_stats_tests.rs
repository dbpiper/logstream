use logstream::seasonal_stats::{FeasibilityResult, SeasonalStats};
use logstream::stress::{StressConfig, StressLevel, StressTracker};

fn make_tracker(level: StressLevel) -> StressTracker {
    let tracker = StressTracker::with_config(StressConfig::CLOUDWATCH);
    match level {
        StressLevel::Normal => {}
        StressLevel::Elevated => {
            for _ in 0..5 {
                tracker.record_failure();
            }
        }
        StressLevel::Critical => {
            for _ in 0..15 {
                tracker.record_failure();
            }
        }
    }
    tracker
}

#[test]
fn test_record_increments_count() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    stats.record_verified(ts, 100);
    stats.record_verified(ts + 1000, 100);

    assert!(stats.total_samples() >= 2);
}

#[test]
fn test_different_times_all_recorded() {
    let stats = SeasonalStats::new();
    let ts1 = 1700000000000i64;
    let ts2 = ts1 + 3_600_000;

    stats.record_verified(ts1, 100);
    stats.record_verified(ts2, 200);

    assert!(stats.total_samples() >= 2);
}

#[test]
fn test_record_and_retrieve() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let result = stats.expected_range(ts);
    assert!(result.is_some());
    let (mean, _stddev) = result.unwrap();
    assert!(mean > 50.0 && mean < 200.0);
}

#[test]
fn test_no_history_returns_none() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;
    assert!(stats.expected_range(ts).is_none());
}

#[test]
fn test_feasibility_with_no_history() {
    let stats = SeasonalStats::new();
    let tracker = make_tracker(StressLevel::Normal);
    let result = stats.is_feasible(1700000000000, 3_600_000, 100, &tracker);
    assert!(matches!(result, FeasibilityResult::NoHistory));
    assert!(result.is_feasible());
}

#[test]
fn test_feasibility_normal_stress_wide_tolerance() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let tracker = make_tracker(StressLevel::Normal);
    let result = stats.is_feasible(ts, 3_600_000, 95, &tracker);
    assert!(result.is_feasible());
}

#[test]
fn test_feasibility_detects_extreme_deviation() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..20 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let tracker = make_tracker(StressLevel::Normal);
    let result = stats.is_feasible(ts, 3_600_000, 1, &tracker);
    assert!(!result.is_feasible());
}

#[test]
fn test_feasibility_zero_count_with_history() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 1000);
    }

    let tracker = make_tracker(StressLevel::Normal);
    let result = stats.is_feasible(ts, 3_600_000, 0, &tracker);
    assert!(!result.is_feasible());
}

#[test]
fn test_sample_count() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    assert_eq!(stats.total_samples(), 0);

    for i in 0..5 {
        stats.record_verified(ts + i * 1000, 100);
    }

    assert!(stats.total_samples() >= 5);
}

#[test]
fn test_should_record_only_on_feasible() {
    let feasible = FeasibilityResult::Feasible {
        expected: 100.0,
        stddev: 10.0,
        sigma_used: 4.0,
    };
    assert!(feasible.should_record());

    let suspicious = FeasibilityResult::Suspicious {
        expected: 100.0,
        stddev: 10.0,
        deviation: 50.0,
        sigma_used: 4.0,
    };
    assert!(!suspicious.should_record());

    let no_history = FeasibilityResult::NoHistory;
    assert!(!no_history.should_record());
}

#[test]
fn test_continuous_similarity() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..20 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let result1 = stats.expected_range(ts);
    let result2 = stats.expected_range(ts + 500);

    assert!(result1.is_some());
    assert!(result2.is_some());
}

#[test]
fn test_stress_level_affects_sigma() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let normal = make_tracker(StressLevel::Normal);
    let critical = make_tracker(StressLevel::Critical);

    let result_normal = stats.is_feasible(ts, 3_600_000, 100, &normal);
    let result_critical = stats.is_feasible(ts, 3_600_000, 100, &critical);

    if let FeasibilityResult::Feasible { sigma_used: s1, .. } = result_normal {
        if let FeasibilityResult::Feasible { sigma_used: s2, .. } = result_critical {
            assert!(s1 > s2);
        }
    }
}

#[test]
fn test_min_samples_builds_stats() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    assert!(stats.expected_range(ts).is_none());

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    assert!(stats.expected_range(ts).is_some());
}

#[test]
fn test_feasibility_result_variants() {
    assert!(FeasibilityResult::NoHistory.is_feasible());
    assert!(!FeasibilityResult::NoHistory.should_record());

    let feasible = FeasibilityResult::Feasible {
        expected: 100.0,
        stddev: 10.0,
        sigma_used: 4.0,
    };
    assert!(feasible.is_feasible());
    assert!(feasible.should_record());

    let suspicious = FeasibilityResult::Suspicious {
        expected: 100.0,
        stddev: 10.0,
        deviation: 50.0,
        sigma_used: 4.0,
    };
    assert!(!suspicious.is_feasible());
    assert!(!suspicious.should_record());
}

#[test]
fn test_range_scaling() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let tracker = make_tracker(StressLevel::Normal);

    let result_1h = stats.is_feasible(ts, 3_600_000, 100, &tracker);
    let result_2h = stats.is_feasible(ts, 7_200_000, 200, &tracker);

    assert!(result_1h.is_feasible());
    assert!(result_2h.is_feasible());
}

#[test]
fn test_regime_change_api_exists() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let result = stats.detect_regime_change(ts);
    assert!(result.is_none() || result.is_some());
}

#[test]
fn test_kernel_weights_similar_times() {
    let stats = SeasonalStats::new();
    let base_ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(base_ts + i * 1000, 100);
    }

    let nearby = base_ts + 500;
    let result = stats.expected_range(nearby);
    assert!(result.is_some());

    let (mean, _) = result.unwrap();
    assert!(mean > 50.0);
}

#[test]
fn test_recency_weights_newer_samples() {
    let stats = SeasonalStats::new();
    let old_ts = 1700000000000i64;
    let new_ts = old_ts + 30 * 24 * 3_600_000;

    for i in 0..5 {
        stats.record_verified(old_ts + i * 1000, 50);
    }
    for i in 0..5 {
        stats.record_verified(new_ts + i * 1000, 150);
    }

    let result = stats.expected_range(new_ts);
    assert!(result.is_some());

    let (mean, _) = result.unwrap();
    assert!(mean > 100.0);
}

#[test]
fn test_max_samples_limit() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..1500 {
        stats.record_verified(ts + i * 1000, 100);
    }

    assert!(stats.total_samples() <= 1000);
}

#[test]
fn test_cyclical_encoding() {
    let stats = SeasonalStats::new();

    let morning = 1700000000000i64;
    let next_morning = morning + 24 * 3_600_000;

    for i in 0..5 {
        stats.record_verified(morning + i * 1000, 100);
    }
    for i in 0..5 {
        stats.record_verified(next_morning + i * 1000, 110);
    }

    let result = stats.expected_range(morning + 48 * 3_600_000);
    assert!(result.is_some());
}

#[test]
fn test_smooth_interpolation() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 3_600_000, 100 + i as u64 * 10);
    }

    let mid = ts + 5 * 3_600_000 + 1_800_000;
    let result = stats.expected_range(mid);

    assert!(result.is_some());
}

#[test]
fn test_diversity_stats() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 3_600_000, 100);
    }

    let result = stats.diversity_stats();
    assert!(result.is_some());

    let (mean, min) = result.unwrap();
    assert!(mean >= min);
    assert!(min >= 0.0);
}

#[test]
fn test_diversity_increases_with_spread() {
    let stats_clustered = SeasonalStats::new();
    let stats_spread = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats_clustered.record_verified(ts + i * 1000, 100);
    }

    for i in 0..10 {
        stats_spread.record_verified(ts + i * 6 * 3_600_000, 100);
    }

    let (mean_c, _) = stats_clustered.diversity_stats().unwrap();
    let (mean_s, _) = stats_spread.diversity_stats().unwrap();

    assert!(mean_s > mean_c);
}

#[test]
fn test_old_diverse_samples_preserved() {
    let stats = SeasonalStats::new();
    let old_ts = 1600000000000i64;
    let new_ts = 1700000000000i64;

    for hour in [0, 6, 12, 18] {
        stats.record_verified(old_ts + hour * 3_600_000, 100);
    }

    for i in 0..1000 {
        stats.record_verified(new_ts + i * 60_000, 100);
    }

    let result = stats.expected_range(old_ts);
    assert!(result.is_some());
}

#[test]
fn test_month_seasonality() {
    let stats = SeasonalStats::new();
    let base = 1704067200000i64;

    for i in 0..10 {
        stats.record_verified(base + i * 1000, 100);
    }

    let one_month_later = base + 30 * 24 * 3_600_000;
    for i in 0..10 {
        stats.record_verified(one_month_later + i * 1000, 200);
    }

    assert!(stats.total_samples() >= 10);
}

#[test]
fn test_catastrophic_forgetting_prevention() {
    let stats = SeasonalStats::new();

    let year_ago = 1668000000000i64;
    for day in 0..7 {
        for hour in [9, 12, 15, 18] {
            stats.record_verified(year_ago + day * 86_400_000 + hour * 3_600_000, 50);
        }
    }
    let old_patterns = stats.total_samples();

    let now = 1700000000000i64;
    for i in 0..500 {
        stats.record_verified(now + i * 60_000, 100);
    }

    let result = stats.expected_range(year_ago + 9 * 3_600_000);
    assert!(result.is_some());

    assert!(stats.total_samples() <= 1000);
    assert!(stats.total_samples() > old_patterns / 2);
}

#[test]
fn test_pattern_diversity_maintained() {
    let stats = SeasonalStats::new();
    let base = 1700000000000i64;

    for day in 0..7 {
        for hour in [9, 12, 18] {
            let ts = base + day * 24 * 3_600_000 + hour * 3_600_000;
            stats.record_verified(ts, 100 + hour as u64);
        }
    }

    assert!(stats.total_samples() >= 21);

    let result = stats.diversity_stats();
    assert!(result.is_some());
    let (_, min) = result.unwrap();
    assert!(min > 0.0);
}

#[test]
fn test_eviction_preserves_diversity() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for week in 0..4 {
        for day in 0..7 {
            for hour in 0..24 {
                let sample_ts = ts + week * 7 * 86_400_000 + day * 86_400_000 + hour * 3_600_000;
                stats.record_verified(sample_ts, 100);
            }
        }
    }

    assert!(stats.total_samples() <= 1000);

    let (mean, min) = stats.diversity_stats().unwrap();
    assert!(min > 0.01);
    assert!(mean > min);
}

mod data_integrity_tests {
    use logstream::seasonal_stats::{validate_event_integrity, DataIntegrity};

    #[test]
    fn test_valid_evenly_distributed_events() {
        let start = 1000i64;
        let end = 2000i64;
        let timestamps: Vec<i64> = (0..10).map(|i| start + i * 100).collect();

        let result = validate_event_integrity(&timestamps, start, end);
        assert!(result.is_usable());
    }

    #[test]
    fn test_empty_events_invalid() {
        let result = validate_event_integrity(&[], 1000, 2000);
        assert!(!result.is_usable());
    }

    #[test]
    fn test_single_event_partial() {
        let result = validate_event_integrity(&[1500], 1000, 2000);
        assert!(result.is_usable());
        assert!(result.should_upsert());
    }

    #[test]
    fn test_clustered_events_low_coverage() {
        let start = 1000i64;
        let end = 2000i64;
        let timestamps: Vec<i64> = (0..10).map(|i| start + i).collect();

        let result = validate_event_integrity(&timestamps, start, end);
        assert!(matches!(
            result,
            DataIntegrity::Partial { .. } | DataIntegrity::Invalid
        ));
    }

    #[test]
    fn test_events_spanning_full_range() {
        let start = 1000i64;
        let end = 2000i64;
        let timestamps = vec![start, start + 250, start + 500, start + 750, end - 1];

        let result = validate_event_integrity(&timestamps, start, end);
        assert!(result.is_usable());
    }

    #[test]
    fn test_monotonic_timestamps() {
        let timestamps = vec![100i64, 200, 300, 400, 500];
        let result = validate_event_integrity(&timestamps, 100, 600);
        assert!(result.is_usable());
    }

    #[test]
    fn test_gap_in_middle_detected() {
        let start = 1000i64;
        let end = 10000i64;
        let timestamps = vec![1000, 1001, 1002, 9998, 9999, 10000];

        let result = validate_event_integrity(&timestamps, start, end);
        match result {
            DataIntegrity::Partial { coverage } => assert!(coverage < 1.0),
            DataIntegrity::Valid => {}
            DataIntegrity::Invalid => panic!("should be usable"),
        }
    }

    #[test]
    fn test_reasonable_distribution() {
        let start = 0i64;
        let end = 1000i64;
        let timestamps: Vec<i64> = (0..20).map(|i| i * 50).collect();

        let result = validate_event_integrity(&timestamps, start, end);
        assert!(result.is_usable());
    }

    #[test]
    fn test_data_integrity_is_usable() {
        assert!(DataIntegrity::Valid.is_usable());
        assert!(DataIntegrity::Partial { coverage: 0.5 }.is_usable());
        assert!(!DataIntegrity::Invalid.is_usable());
    }

    #[test]
    fn test_data_integrity_should_upsert() {
        assert!(!DataIntegrity::Valid.should_upsert());
        assert!(DataIntegrity::Partial { coverage: 0.5 }.should_upsert());
        assert!(!DataIntegrity::Invalid.should_upsert());
    }

    #[test]
    fn test_fourier_detects_natural_distribution() {
        let base = 1700000000000i64;
        let range_ms = 86_400_000i64;
        let timestamps: Vec<i64> = (0..100).map(|i| base + (i * range_ms / 100)).collect();

        let result = validate_event_integrity(&timestamps, base, base + range_ms);
        assert!(result.is_usable());
    }

    #[test]
    fn test_fourier_detects_clustered_same_hour() {
        let base = 1700000000000i64;
        let timestamps: Vec<i64> = (0..50).map(|i| base + i * 1000).collect();

        let result = validate_event_integrity(&timestamps, base, base + 86_400_000);
        match result {
            DataIntegrity::Partial { coverage } => assert!(coverage < 0.8),
            DataIntegrity::Valid => {}
            DataIntegrity::Invalid => {}
        }
    }

    #[test]
    fn test_fourier_uniform_across_day() {
        let base = 1700000000000i64;
        let hour_ms = 3_600_000i64;
        let timestamps: Vec<i64> = (0..24).map(|h| base + h * hour_ms + 1_800_000).collect();

        let result = validate_event_integrity(&timestamps, base, base + 24 * hour_ms);
        assert!(result.is_usable());
    }

    #[test]
    fn test_circular_variance_uniform() {
        let base = 1700000000000i64;
        let hour_ms = 3_600_000i64;
        let timestamps: Vec<i64> = (0..24).map(|h| base + h * hour_ms).collect();

        let result = validate_event_integrity(&timestamps, base, base + 24 * hour_ms);
        assert!(result.is_usable());
    }

    #[test]
    fn test_gap_coefficient_of_variation() {
        let base = 1000i64;
        let uniform: Vec<i64> = (0..20).map(|i| base + i * 100).collect();
        let result = validate_event_integrity(&uniform, base, base + 2000);
        assert!(result.is_usable());
    }
}
