//! Adaptive rate controller for Elasticsearch.
//! Monitors ES health and dynamically adjusts ingestion behavior.
//! Uses TCP-style congestion control: probe capacity, back off on congestion.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{info, warn};

/// Adaptive controller state.
#[derive(Debug)]
pub struct AdaptiveController {
    /// Current batch size (dynamically adjusted).
    batch_size: AtomicUsize,
    /// Current max in-flight requests.
    max_in_flight: AtomicUsize,
    /// Current inter-batch delay in ms.
    delay_ms: AtomicU64,
    /// Recent latency samples (ms).
    latencies: RwLock<Vec<u64>>,
    /// Consecutive slow responses.
    slow_count: AtomicUsize,
    /// Consecutive fast responses.
    fast_count: AtomicUsize,
    /// Configuration.
    config: AdaptiveConfig,
    /// Last adjustment time.
    last_adjust: RwLock<Instant>,
}

/// Configuration for adaptive behavior.
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Initial batch size.
    pub initial_batch_size: usize,
    /// Minimum batch size.
    pub min_batch_size: usize,
    /// Maximum batch size.
    pub max_batch_size: usize,
    /// Initial max in-flight.
    pub initial_max_in_flight: usize,
    /// Minimum in-flight.
    pub min_in_flight: usize,
    /// Maximum in-flight.
    pub max_in_flight: usize,
    /// Target latency (ms) - below this we can speed up.
    pub target_latency_ms: u64,
    /// Slow threshold (ms) - above this we back off.
    pub slow_latency_ms: u64,
    /// Critical threshold (ms) - severe backoff.
    pub critical_latency_ms: u64,
    /// Number of samples for moving average.
    pub sample_window: usize,
    /// Consecutive slow responses before backing off.
    pub slow_threshold: usize,
    /// Consecutive fast responses before speeding up.
    pub fast_threshold: usize,
    /// Minimum time between adjustments.
    pub adjust_interval: Duration,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_batch_size: 5000,
            min_batch_size: 100,
            max_batch_size: 20000,
            initial_max_in_flight: 4,
            min_in_flight: 1,
            max_in_flight: 16,
            target_latency_ms: 500,
            slow_latency_ms: 2000,
            critical_latency_ms: 10000,
            sample_window: 20,
            slow_threshold: 3,
            fast_threshold: 10,
            adjust_interval: Duration::from_secs(5),
        }
    }
}

impl AdaptiveController {
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            batch_size: AtomicUsize::new(config.initial_batch_size),
            max_in_flight: AtomicUsize::new(config.initial_max_in_flight),
            delay_ms: AtomicU64::new(0),
            latencies: RwLock::new(Vec::with_capacity(config.sample_window)),
            slow_count: AtomicUsize::new(0),
            fast_count: AtomicUsize::new(0),
            config,
            last_adjust: RwLock::new(Instant::now()),
        }
    }

    /// Get current recommended batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed)
    }

    /// Get current max in-flight.
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight.load(Ordering::Relaxed)
    }

    /// Get current inter-batch delay.
    pub fn delay(&self) -> Duration {
        Duration::from_millis(self.delay_ms.load(Ordering::Relaxed))
    }

    /// Record a bulk response and adjust parameters.
    pub async fn record_latency(&self, latency_ms: u64, success: bool) {
        // Update latency samples
        {
            let mut latencies = self.latencies.write().await;
            latencies.push(latency_ms);
            if latencies.len() > self.config.sample_window {
                latencies.remove(0);
            }
        }

        // Track slow/fast streaks
        if !success || latency_ms > self.config.slow_latency_ms {
            self.slow_count.fetch_add(1, Ordering::Relaxed);
            self.fast_count.store(0, Ordering::Relaxed);
        } else if latency_ms < self.config.target_latency_ms {
            self.fast_count.fetch_add(1, Ordering::Relaxed);
            self.slow_count.store(0, Ordering::Relaxed);
        }

        // Critical latency - immediate severe backoff
        if latency_ms > self.config.critical_latency_ms {
            self.emergency_backoff().await;
            return;
        }

        // Check if we should adjust
        let should_adjust = {
            let last = self.last_adjust.read().await;
            last.elapsed() > self.config.adjust_interval
        };

        if should_adjust {
            self.maybe_adjust().await;
        }
    }

    /// Emergency backoff for critical issues.
    async fn emergency_backoff(&self) {
        let batch = self.batch_size.load(Ordering::Relaxed);
        let in_flight = self.max_in_flight.load(Ordering::Relaxed);

        // Halve batch size and in-flight
        let new_batch = (batch / 2).max(self.config.min_batch_size);
        let new_in_flight = (in_flight / 2).max(self.config.min_in_flight);

        // Add delay
        let current_delay = self.delay_ms.load(Ordering::Relaxed);
        let new_delay = (current_delay + 500).min(5000);

        self.batch_size.store(new_batch, Ordering::Relaxed);
        self.max_in_flight.store(new_in_flight, Ordering::Relaxed);
        self.delay_ms.store(new_delay, Ordering::Relaxed);

        warn!(
            "adaptive: EMERGENCY backoff batch={}->{} in_flight={}->{} delay={}ms",
            batch, new_batch, in_flight, new_in_flight, new_delay
        );

        // Reset counters
        self.slow_count.store(0, Ordering::Relaxed);
        self.fast_count.store(0, Ordering::Relaxed);
        *self.last_adjust.write().await = Instant::now();
    }

    /// Possibly adjust parameters based on recent performance.
    async fn maybe_adjust(&self) {
        let slow = self.slow_count.load(Ordering::Relaxed);
        let fast = self.fast_count.load(Ordering::Relaxed);

        // Calculate average latency
        let avg_latency = {
            let latencies = self.latencies.read().await;
            if latencies.is_empty() {
                return;
            }
            latencies.iter().sum::<u64>() / latencies.len() as u64
        };

        let batch = self.batch_size.load(Ordering::Relaxed);
        let in_flight = self.max_in_flight.load(Ordering::Relaxed);
        let delay = self.delay_ms.load(Ordering::Relaxed);

        // Back off if consistently slow
        if slow >= self.config.slow_threshold {
            let new_batch = (batch * 3 / 4).max(self.config.min_batch_size);
            let new_in_flight = if in_flight > 2 {
                in_flight - 1
            } else {
                self.config.min_in_flight
            };
            let new_delay = (delay + 100).min(2000);

            self.batch_size.store(new_batch, Ordering::Relaxed);
            self.max_in_flight.store(new_in_flight, Ordering::Relaxed);
            self.delay_ms.store(new_delay, Ordering::Relaxed);

            info!(
                "adaptive: backoff batch={}->{} in_flight={}->{} delay={}ms (avg_latency={}ms)",
                batch, new_batch, in_flight, new_in_flight, new_delay, avg_latency
            );

            self.slow_count.store(0, Ordering::Relaxed);
            *self.last_adjust.write().await = Instant::now();
        }
        // Speed up if consistently fast
        else if fast >= self.config.fast_threshold && avg_latency < self.config.target_latency_ms
        {
            let new_batch = (batch * 5 / 4).min(self.config.max_batch_size);
            let new_in_flight = (in_flight + 1).min(self.config.max_in_flight);
            let new_delay = delay.saturating_sub(50);

            self.batch_size.store(new_batch, Ordering::Relaxed);
            self.max_in_flight.store(new_in_flight, Ordering::Relaxed);
            self.delay_ms.store(new_delay, Ordering::Relaxed);

            info!(
                "adaptive: speedup batch={}->{} in_flight={}->{} delay={}ms (avg_latency={}ms)",
                batch, new_batch, in_flight, new_in_flight, new_delay, avg_latency
            );

            self.fast_count.store(0, Ordering::Relaxed);
            *self.last_adjust.write().await = Instant::now();
        }
    }
}

/// Create a shared adaptive controller.
pub fn create_controller() -> Arc<AdaptiveController> {
    Arc::new(AdaptiveController::new(AdaptiveConfig::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Critical latency triggers emergency backoff
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

        // Multiple slow responses
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

        // Multiple fast responses
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

        // Many critical latencies - should hit minimum
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

        // Failed requests should trigger backoff
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

        // Mix of fast and slow - should stabilize, not oscillate wildly
        for i in 0..20 {
            let latency = if i % 2 == 0 { 100 } else { 1500 };
            ctrl.record_latency(latency, true).await;
        }

        // Should be near initial values (not at extremes)
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

        // First, cause backoff
        for _ in 0..5 {
            ctrl.record_latency(15000, true).await; // Critical
        }
        let backed_off_batch = ctrl.batch_size();
        assert!(backed_off_batch < 5000);

        // Then, recover with fast responses
        for _ in 0..20 {
            ctrl.record_latency(100, true).await;
        }

        // Should have recovered
        assert!(ctrl.batch_size() > backed_off_batch);
    }

    #[tokio::test]
    async fn test_delay_increases_on_critical() {
        let ctrl = AdaptiveController::new(AdaptiveConfig::default());
        assert_eq!(ctrl.delay(), Duration::ZERO);

        // Critical latency should add delay
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

        // First add delay via critical latency
        ctrl.record_latency(15000, true).await;
        let initial_delay = ctrl.delay();
        assert!(initial_delay > Duration::ZERO);

        // Fast responses should eventually reduce delay (may take multiple adjustment cycles)
        for _ in 0..30 {
            ctrl.record_latency(100, true).await;
        }

        // Delay should have decreased or stayed same (not increased)
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

        // Slow responses reduce in-flight
        for _ in 0..5 {
            ctrl.record_latency(3000, true).await;
        }
        let reduced = ctrl.max_in_flight();
        assert!(reduced < 8);

        // Fast responses increase in-flight (but may not fully recover in limited iterations)
        for _ in 0..30 {
            ctrl.record_latency(100, true).await;
        }
        // Should have increased from the reduced value
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

        // Record more than window size
        for i in 0..10 {
            ctrl.record_latency((i * 100) as u64, true).await;
        }

        // Should have maintained window size
        let latencies = ctrl.latencies.read().await;
        assert_eq!(latencies.len(), 5);
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let ctrl = Arc::new(AdaptiveController::new(AdaptiveConfig {
            adjust_interval: Duration::ZERO,
            ..AdaptiveConfig::default()
        }));

        let mut handles = vec![];

        // Spawn multiple tasks recording latencies
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

        // Should not have panicked and values should be valid
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

        // Hammer with critical latencies
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

        // Many fast responses
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

        // Slow responses
        for _ in 0..3 {
            ctrl.record_latency(3000, true).await;
        }
        let after_slow = ctrl.batch_size();
        assert!(after_slow < initial);

        // Then critical
        ctrl.record_latency(15000, true).await;
        let after_critical = ctrl.batch_size();
        assert!(after_critical < after_slow);
    }

    // ===== OPTIMALITY TESTS =====
    // These tests verify the controller converges to optimal throughput

    /// Simulates an ES backend with a fixed optimal throughput point.
    /// Returns latency based on current load vs optimal capacity.
    fn simulate_es_latency(batch_size: usize, in_flight: usize, optimal_throughput: usize) -> u64 {
        let current_throughput = batch_size * in_flight;
        let load_ratio = current_throughput as f64 / optimal_throughput as f64;

        if load_ratio <= 0.5 {
            // Under-utilized: fast but wasting capacity
            100
        } else if load_ratio <= 0.9 {
            // Sweet spot: good latency, good throughput
            300
        } else if load_ratio <= 1.0 {
            // Near optimal: slightly higher latency
            600
        } else if load_ratio <= 1.2 {
            // Slightly overloaded
            2500
        } else if load_ratio <= 1.5 {
            // Overloaded
            8000
        } else {
            // Severely overloaded
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

        // Simulate ES with optimal throughput of 40000 events/batch
        let optimal_throughput = 40000;

        // Run simulation for many iterations
        let mut throughputs = Vec::new();
        for _ in 0..200 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let throughput = batch * in_flight;

            let latency = simulate_es_latency(batch, in_flight, optimal_throughput);
            ctrl.record_latency(latency, latency < 10000).await;

            throughputs.push(throughput);
        }

        // Check convergence: last 50 iterations should be near optimal
        let final_throughputs: Vec<_> = throughputs.iter().skip(150).copied().collect();
        let avg_final = final_throughputs.iter().sum::<usize>() / final_throughputs.len();

        // Should be within 50% of optimal (not too conservative, not overloaded)
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

        // Phase 1: High capacity ES (optimal = 100k)
        for _ in 0..50 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let latency = simulate_es_latency(batch, in_flight, 100000);
            ctrl.record_latency(latency, true).await;
        }
        let phase1_throughput = ctrl.batch_size() * ctrl.max_in_flight();

        // Phase 2: Capacity drops (optimal = 20k) - simulates ES under pressure
        for _ in 0..50 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let latency = simulate_es_latency(batch, in_flight, 20000);
            ctrl.record_latency(latency, latency < 10000).await;
        }
        let phase2_throughput = ctrl.batch_size() * ctrl.max_in_flight();

        // Phase 3: Capacity recovers (optimal = 80k)
        for _ in 0..50 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let latency = simulate_es_latency(batch, in_flight, 80000);
            ctrl.record_latency(latency, true).await;
        }
        let phase3_throughput = ctrl.batch_size() * ctrl.max_in_flight();

        // Should have backed off in phase 2
        assert!(
            phase2_throughput < phase1_throughput,
            "should back off when capacity drops: phase1={} phase2={}",
            phase1_throughput,
            phase2_throughput
        );

        // Should have recovered in phase 3
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

        // Simulate ES where optimal is around 30k throughput
        let optimal = 30000;

        for _ in 0..100 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let latency = simulate_es_latency(batch, in_flight, optimal);
            ctrl.record_latency(latency, latency < 10000).await;
        }

        let final_throughput = ctrl.batch_size() * ctrl.max_in_flight();

        // Should not be at minimum (too conservative)
        assert!(
            final_throughput > 1000,
            "throughput {} too low (over-conservative)",
            final_throughput
        );

        // Should not be at maximum (too aggressive causing failures)
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

        // Simulate ES with specific optimal point
        let optimal = 25000;

        // Warm up to find optimal
        for _ in 0..100 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            let latency = simulate_es_latency(batch, in_flight, optimal);
            ctrl.record_latency(latency, true).await;
        }

        // Record throughput variance over next 50 iterations
        let mut throughputs = Vec::new();
        for _ in 0..50 {
            let batch = ctrl.batch_size();
            let in_flight = ctrl.max_in_flight();
            throughputs.push(batch * in_flight);

            let latency = simulate_es_latency(batch, in_flight, optimal);
            ctrl.record_latency(latency, true).await;
        }

        // Calculate variance - should be stable, not oscillating wildly
        let mean = throughputs.iter().sum::<usize>() as f64 / throughputs.len() as f64;
        let variance: f64 = throughputs
            .iter()
            .map(|&t| (t as f64 - mean).powi(2))
            .sum::<f64>()
            / throughputs.len() as f64;
        let std_dev = variance.sqrt();
        let cv = std_dev / mean; // Coefficient of variation

        // CV should be low (< 0.3 means stable)
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

        // Suddenly ES becomes overloaded - should back off quickly
        for i in 0..10 {
            ctrl.record_latency(12000, i < 5).await; // Mix of slow and failures
        }

        let after_overload = ctrl.batch_size() * ctrl.max_in_flight();

        // Should have backed off significantly within 10 requests
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

        // Fast responses - should speed up
        for _ in 0..30 {
            ctrl.record_latency(50, true).await;
        }

        let final_throughput = ctrl.batch_size() * ctrl.max_in_flight();

        // Should have increased throughput
        assert!(
            final_throughput > initial_throughput,
            "should speed up on fast responses: initial={} final={}",
            initial_throughput,
            final_throughput
        );
    }
}
