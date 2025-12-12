//! Tests for adaptive rate controller.

use logstream::adaptive::{create_controller, AdaptiveConfig, AdaptiveController};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_initial_values() {
    let ctrl = AdaptiveController::new(AdaptiveConfig::default());
    assert_eq!(ctrl.batch_size(), 5000);
    assert_eq!(ctrl.max_in_flight(), 4);
    assert_eq!(ctrl.delay(), Duration::ZERO);
}

#[tokio::test]
async fn test_emergency_backoff() {
    let ctrl = AdaptiveController::new(AdaptiveConfig::default());

    ctrl.record_latency(15000, true).await;

    assert!(ctrl.batch_size() < 5000);
    assert!(ctrl.max_in_flight() < 4);
    assert!(ctrl.delay() > Duration::ZERO);
}

#[tokio::test]
async fn test_gradual_backoff() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..5 {
        ctrl.record_latency(3000, true).await;
    }

    assert!(ctrl.batch_size() < 5000);
}

#[tokio::test]
async fn test_speedup_on_fast() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        fast_threshold: 3,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..5 {
        ctrl.record_latency(100, true).await;
    }

    assert!(ctrl.batch_size() > 5000);
}

#[tokio::test]
async fn test_respects_limits() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        min_batch_size: 100,
        max_batch_size: 10000,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..20 {
        ctrl.record_latency(20000, true).await;
    }

    assert!(ctrl.batch_size() >= 100);
    assert!(ctrl.max_in_flight() >= 1);
}

#[tokio::test]
async fn test_failure_triggers_backoff() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 2,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);
    let initial_batch = ctrl.batch_size();

    for _ in 0..5 {
        ctrl.record_latency(500, false).await;
    }

    assert!(ctrl.batch_size() < initial_batch);
}

#[tokio::test]
async fn test_mixed_latencies_stabilize() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 5,
        fast_threshold: 5,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for i in 0..20 {
        let latency = if i % 2 == 0 { 100 } else { 1500 };
        ctrl.record_latency(latency, true).await;
    }

    let batch = ctrl.batch_size();
    assert!((2000..=10000).contains(&batch));
}

#[tokio::test]
async fn test_recovery_after_backoff() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 2,
        fast_threshold: 3,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..5 {
        ctrl.record_latency(15000, true).await;
    }
    let backed_off_batch = ctrl.batch_size();
    assert!(backed_off_batch < 5000);

    for _ in 0..20 {
        ctrl.record_latency(100, true).await;
    }

    assert!(ctrl.batch_size() > backed_off_batch);
}

#[tokio::test]
async fn test_delay_increases_on_critical() {
    let ctrl = AdaptiveController::new(AdaptiveConfig::default());
    assert_eq!(ctrl.delay(), Duration::ZERO);

    ctrl.record_latency(15000, true).await;

    assert!(ctrl.delay() > Duration::ZERO);
}

#[tokio::test]
async fn test_delay_decreases_on_fast() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        fast_threshold: 3,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    ctrl.record_latency(15000, true).await;
    let initial_delay = ctrl.delay();
    assert!(initial_delay > Duration::ZERO);

    for _ in 0..30 {
        ctrl.record_latency(100, true).await;
    }

    assert!(ctrl.delay() <= initial_delay);
}

#[tokio::test]
async fn test_in_flight_adjusts() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 2,
        fast_threshold: 3,
        initial_max_in_flight: 8,
        min_in_flight: 1,
        max_in_flight: 16,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);
    assert_eq!(ctrl.max_in_flight(), 8);

    for _ in 0..5 {
        ctrl.record_latency(3000, true).await;
    }
    let reduced = ctrl.max_in_flight();
    assert!(reduced < 8);

    for _ in 0..30 {
        ctrl.record_latency(100, true).await;
    }
    assert!(ctrl.max_in_flight() >= reduced);
}

#[tokio::test]
async fn test_config_defaults_reasonable() {
    let config = AdaptiveConfig::default();

    assert!(config.initial_batch_size > 0);
    assert!(config.min_batch_size < config.max_batch_size);
    assert!(config.min_in_flight < config.max_in_flight);
    assert!(config.target_latency_ms < config.slow_latency_ms);
    assert!(config.slow_latency_ms < config.critical_latency_ms);
    assert!(config.slow_threshold > 0);
    assert!(config.fast_threshold > 0);
}

#[tokio::test]
async fn test_latency_tracking() {
    let config = AdaptiveConfig {
        sample_window: 5,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for i in 0..10 {
        ctrl.record_latency((i * 100) as u64, true).await;
    }

    let latencies = ctrl.latencies().await;
    assert_eq!(latencies.len(), 5);
}

#[tokio::test]
async fn test_concurrent_access() {
    let ctrl = Arc::new(AdaptiveController::new(AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        ..AdaptiveConfig::default()
    }));

    let mut handles = vec![];

    for i in 0..10 {
        let ctrl_clone = ctrl.clone();
        handles.push(tokio::spawn(async move {
            for j in 0..10 {
                let latency = ((i * 10 + j) * 50) as u64;
                ctrl_clone.record_latency(latency, true).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    assert!(ctrl.batch_size() > 0);
    assert!(ctrl.max_in_flight() > 0);
}

#[tokio::test]
async fn test_create_controller() {
    let ctrl = create_controller();
    assert_eq!(ctrl.batch_size(), 5000);
    assert_eq!(ctrl.max_in_flight(), 4);
}

#[tokio::test]
async fn test_never_goes_below_minimum() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        min_batch_size: 500,
        min_in_flight: 2,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..50 {
        ctrl.record_latency(50000, true).await;
    }

    assert!(ctrl.batch_size() >= 500);
    assert!(ctrl.max_in_flight() >= 2);
}

#[tokio::test]
async fn test_never_exceeds_maximum() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        max_batch_size: 8000,
        max_in_flight: 6,
        fast_threshold: 2,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..50 {
        ctrl.record_latency(10, true).await;
    }

    assert!(ctrl.batch_size() <= 8000);
    assert!(ctrl.max_in_flight() <= 6);
}

#[tokio::test]
async fn test_slow_then_critical_compounds() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 2,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);
    let initial = ctrl.batch_size();

    for _ in 0..3 {
        ctrl.record_latency(3000, true).await;
    }
    let after_slow = ctrl.batch_size();
    assert!(after_slow < initial);

    ctrl.record_latency(15000, true).await;
    let after_critical = ctrl.batch_size();
    assert!(after_critical < after_slow);
}

// ===== OPTIMALITY TESTS =====

fn simulate_es_latency(batch_size: usize, in_flight: usize, optimal_throughput: usize) -> u64 {
    let current_throughput = batch_size * in_flight;
    let load_ratio = current_throughput as f64 / optimal_throughput as f64;

    if load_ratio <= 0.5 {
        100
    } else if load_ratio <= 0.9 {
        300
    } else if load_ratio <= 1.0 {
        600
    } else if load_ratio <= 1.2 {
        2500
    } else if load_ratio <= 1.5 {
        8000
    } else {
        15000
    }
}

#[tokio::test]
async fn test_converges_to_optimal_throughput() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        initial_batch_size: 1000,
        initial_max_in_flight: 2,
        min_batch_size: 100,
        max_batch_size: 20000,
        min_in_flight: 1,
        max_in_flight: 16,
        slow_threshold: 3,
        fast_threshold: 5,
        target_latency_ms: 500,
        slow_latency_ms: 2000,
        critical_latency_ms: 10000,
        sample_window: 10,
    };
    let ctrl = AdaptiveController::new(config);

    let optimal_throughput = 40000;

    let mut throughputs = Vec::new();
    for _ in 0..200 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let throughput = batch * in_flight;

        let latency = simulate_es_latency(batch, in_flight, optimal_throughput);
        ctrl.record_latency(latency, latency < 10000).await;

        throughputs.push(throughput);
    }

    let final_throughputs: Vec<_> = throughputs.iter().skip(150).copied().collect();
    let avg_final = final_throughputs.iter().sum::<usize>() / final_throughputs.len();

    let efficiency = avg_final as f64 / optimal_throughput as f64;
    assert!(
        efficiency > 0.4 && efficiency < 1.3,
        "efficiency {} not in optimal range (0.4-1.3), avg_throughput={}, optimal={}",
        efficiency,
        avg_final,
        optimal_throughput
    );
}

#[tokio::test]
async fn test_adapts_to_changing_capacity() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 2,
        fast_threshold: 3,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    for _ in 0..50 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let latency = simulate_es_latency(batch, in_flight, 100000);
        ctrl.record_latency(latency, true).await;
    }
    let phase1_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    for _ in 0..50 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let latency = simulate_es_latency(batch, in_flight, 20000);
        ctrl.record_latency(latency, latency < 10000).await;
    }
    let phase2_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    for _ in 0..50 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let latency = simulate_es_latency(batch, in_flight, 80000);
        ctrl.record_latency(latency, true).await;
    }
    let phase3_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    assert!(
        phase2_throughput < phase1_throughput,
        "should back off when capacity drops: phase1={} phase2={}",
        phase1_throughput,
        phase2_throughput
    );

    assert!(
        phase3_throughput > phase2_throughput,
        "should recover when capacity returns: phase2={} phase3={}",
        phase2_throughput,
        phase3_throughput
    );
}

#[tokio::test]
async fn test_finds_sweet_spot_not_extremes() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        initial_batch_size: 500,
        initial_max_in_flight: 1,
        slow_threshold: 2,
        fast_threshold: 4,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    let optimal = 30000;

    for _ in 0..100 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let latency = simulate_es_latency(batch, in_flight, optimal);
        ctrl.record_latency(latency, latency < 10000).await;
    }

    let final_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    assert!(
        final_throughput > 1000,
        "throughput {} too low (over-conservative)",
        final_throughput
    );

    assert!(
        final_throughput < 200000,
        "throughput {} too high (over-aggressive)",
        final_throughput
    );
}

#[tokio::test]
async fn test_stability_at_optimal() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        slow_threshold: 3,
        fast_threshold: 5,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    let optimal = 25000;

    for _ in 0..100 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        let latency = simulate_es_latency(batch, in_flight, optimal);
        ctrl.record_latency(latency, true).await;
    }

    let mut throughputs = Vec::new();
    for _ in 0..50 {
        let batch = ctrl.batch_size();
        let in_flight = ctrl.max_in_flight();
        throughputs.push(batch * in_flight);

        let latency = simulate_es_latency(batch, in_flight, optimal);
        ctrl.record_latency(latency, true).await;
    }

    let mean = throughputs.iter().sum::<usize>() as f64 / throughputs.len() as f64;
    let variance: f64 = throughputs
        .iter()
        .map(|&t| (t as f64 - mean).powi(2))
        .sum::<f64>()
        / throughputs.len() as f64;
    let std_dev = variance.sqrt();
    let cv = std_dev / mean;

    assert!(
        cv < 0.5,
        "throughput too unstable: cv={:.2}, mean={:.0}, stddev={:.0}",
        cv,
        mean,
        std_dev
    );
}

#[tokio::test]
async fn test_quick_backoff_on_overload() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        initial_batch_size: 10000,
        initial_max_in_flight: 8,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    let initial_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    for i in 0..10 {
        ctrl.record_latency(12000, i < 5).await;
    }

    let after_overload = ctrl.batch_size() * ctrl.max_in_flight();

    let reduction = 1.0 - (after_overload as f64 / initial_throughput as f64);
    assert!(
        reduction > 0.3,
        "should back off quickly: initial={} after={} reduction={:.1}%",
        initial_throughput,
        after_overload,
        reduction * 100.0
    );
}

#[tokio::test]
async fn test_gradual_speedup_on_underload() {
    let config = AdaptiveConfig {
        adjust_interval: Duration::ZERO,
        initial_batch_size: 1000,
        initial_max_in_flight: 2,
        fast_threshold: 3,
        ..AdaptiveConfig::default()
    };
    let ctrl = AdaptiveController::new(config);

    let initial_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    for _ in 0..30 {
        ctrl.record_latency(50, true).await;
    }

    let final_throughput = ctrl.batch_size() * ctrl.max_in_flight();

    assert!(
        final_throughput > initial_throughput,
        "should speed up on fast responses: initial={} final={}",
        initial_throughput,
        final_throughput
    );
}
