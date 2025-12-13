//! Adaptive rate controller for Elasticsearch.
//! Monitors ES health and dynamically adjusts ingestion behavior.
//! Uses TCP-style congestion control: probe capacity, back off on congestion.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{info, warn};

/// Heap usage threshold (85%) - start backing off
const HEAP_PRESSURE_THRESHOLD: f64 = 0.85;
/// Heap critical threshold (95%) - emergency backoff
const HEAP_CRITICAL_THRESHOLD: f64 = 0.95;
/// CPU usage threshold (80%) - start backing off
const CPU_PRESSURE_THRESHOLD: f64 = 0.80;
/// CPU critical threshold (95%) - emergency backoff
const CPU_CRITICAL_THRESHOLD: f64 = 0.95;

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
    /// External pressure flag (set by health monitor).
    external_pressure: AtomicBool,
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
            external_pressure: AtomicBool::new(false),
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

    pub async fn latencies(&self) -> Vec<u64> {
        self.latencies.read().await.clone()
    }

    /// Check if under external pressure (e.g., ES heap/CPU pressure).
    pub fn is_under_pressure(&self) -> bool {
        self.external_pressure.load(Ordering::Relaxed)
    }

    /// Set external pressure flag based on heap usage and trigger backoff.
    pub async fn set_heap_pressure(&self, heap_percent: f64) {
        if heap_percent >= HEAP_CRITICAL_THRESHOLD {
            self.external_pressure.store(true, Ordering::Relaxed);
            warn!(
                "adaptive: ES heap CRITICAL at {:.1}% - emergency backoff",
                heap_percent * 100.0
            );
            self.emergency_backoff().await;
        } else if heap_percent >= HEAP_PRESSURE_THRESHOLD {
            self.external_pressure.store(true, Ordering::Relaxed);
            info!(
                "adaptive: ES heap pressure at {:.1}% - backing off",
                heap_percent * 100.0
            );
            self.pressure_backoff().await;
        } else {
            // Don't clear pressure here - let set_es_pressure handle it
        }
    }

    /// Set external pressure flag based on CPU usage and trigger backoff.
    pub async fn set_cpu_pressure(&self, cpu_percent: f64) {
        if cpu_percent >= CPU_CRITICAL_THRESHOLD {
            self.external_pressure.store(true, Ordering::Relaxed);
            warn!(
                "adaptive: ES CPU CRITICAL at {:.1}% - emergency backoff",
                cpu_percent * 100.0
            );
            self.emergency_backoff().await;
        } else if cpu_percent >= CPU_PRESSURE_THRESHOLD {
            self.external_pressure.store(true, Ordering::Relaxed);
            info!(
                "adaptive: ES CPU pressure at {:.1}% - backing off",
                cpu_percent * 100.0
            );
            self.pressure_backoff().await;
        }
        // Don't clear pressure here - let set_es_pressure handle it
    }

    /// Set ES pressure based on both heap and CPU metrics.
    /// Only clears pressure when BOTH are below thresholds.
    pub async fn set_es_pressure(&self, heap_percent: f64, cpu_percent: f64) {
        let heap_pressure = heap_percent >= HEAP_PRESSURE_THRESHOLD;
        let cpu_pressure = cpu_percent >= CPU_PRESSURE_THRESHOLD;
        let heap_critical = heap_percent >= HEAP_CRITICAL_THRESHOLD;
        let cpu_critical = cpu_percent >= CPU_CRITICAL_THRESHOLD;

        if heap_critical || cpu_critical {
            self.external_pressure.store(true, Ordering::Relaxed);
            warn!(
                "adaptive: ES CRITICAL heap={:.1}% cpu={:.1}% - emergency backoff",
                heap_percent * 100.0,
                cpu_percent * 100.0
            );
            self.emergency_backoff().await;
        } else if heap_pressure || cpu_pressure {
            self.external_pressure.store(true, Ordering::Relaxed);
            info!(
                "adaptive: ES pressure heap={:.1}% cpu={:.1}% - backing off",
                heap_percent * 100.0,
                cpu_percent * 100.0
            );
            self.pressure_backoff().await;
        } else {
            // Clear pressure only if both metrics are healthy
            if self.external_pressure.swap(false, Ordering::Relaxed) {
                info!(
                    "adaptive: ES recovered heap={:.1}% cpu={:.1}% - pressure cleared",
                    heap_percent * 100.0,
                    cpu_percent * 100.0
                );
            }
        }
    }

    /// Moderate backoff for heap pressure (less aggressive than emergency).
    async fn pressure_backoff(&self) {
        let batch = self.batch_size.load(Ordering::Relaxed);
        let in_flight = self.max_in_flight.load(Ordering::Relaxed);

        // Reduce by 25%
        let new_batch = (batch * 3 / 4).max(self.config.min_batch_size);
        let new_in_flight = (in_flight * 3 / 4).max(self.config.min_in_flight);

        // Add moderate delay
        let current_delay = self.delay_ms.load(Ordering::Relaxed);
        let new_delay = (current_delay + 200).min(2000);

        self.batch_size.store(new_batch, Ordering::Relaxed);
        self.max_in_flight.store(new_in_flight, Ordering::Relaxed);
        self.delay_ms.store(new_delay, Ordering::Relaxed);

        // Reset fast counter to prevent speedup while under pressure
        self.fast_count.store(0, Ordering::Relaxed);
        *self.last_adjust.write().await = Instant::now();
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
        // Speed up if consistently fast AND not under external pressure
        else if fast >= self.config.fast_threshold
            && avg_latency < self.config.target_latency_ms
            && !self.external_pressure.load(Ordering::Relaxed)
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
