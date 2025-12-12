use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StressLevel {
    Normal = 0,
    Elevated = 1,
    Critical = 2,
}

impl From<u8> for StressLevel {
    fn from(v: u8) -> Self {
        match v {
            2 => StressLevel::Critical,
            1 => StressLevel::Elevated,
            _ => StressLevel::Normal,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StressConfig {
    pub min_backoff_secs: u64,
    pub max_backoff_secs: u64,
    pub backoff_multiplier: u64,
    pub elevated_threshold: u64,
    pub critical_threshold: u64,
}

impl StressConfig {
    pub const ES: Self = Self {
        min_backoff_secs: 3,
        max_backoff_secs: 60,
        backoff_multiplier: 2,
        elevated_threshold: 3,
        critical_threshold: 10,
    };

    pub const CLOUDWATCH: Self = Self {
        min_backoff_secs: 1,
        max_backoff_secs: 30,
        backoff_multiplier: 2,
        elevated_threshold: 3,
        critical_threshold: 10,
    };
}

impl Default for StressConfig {
    fn default() -> Self {
        Self::ES
    }
}

pub struct StressTracker {
    config: StressConfig,
    total_failures: AtomicU64,
    failure_streak: AtomicU64,
    current_backoff_secs: AtomicU64,
    stress_level: AtomicU8,
    last_known_value: AtomicU64,
}

impl Default for StressTracker {
    fn default() -> Self {
        Self::with_config(StressConfig::default())
    }
}

impl StressTracker {
    pub fn with_config(config: StressConfig) -> Self {
        Self {
            current_backoff_secs: AtomicU64::new(config.min_backoff_secs),
            config,
            total_failures: AtomicU64::new(0),
            failure_streak: AtomicU64::new(0),
            stress_level: AtomicU8::new(0),
            last_known_value: AtomicU64::new(0),
        }
    }

    pub fn for_es() -> Self {
        Self::with_config(StressConfig::ES)
    }

    pub fn for_cloudwatch() -> Self {
        Self::with_config(StressConfig::CLOUDWATCH)
    }

    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::SeqCst);
        let streak = self.failure_streak.fetch_add(1, Ordering::SeqCst) + 1;

        let current = self.current_backoff_secs.load(Ordering::SeqCst);
        let new_backoff =
            (current * self.config.backoff_multiplier).min(self.config.max_backoff_secs);
        self.current_backoff_secs
            .store(new_backoff, Ordering::SeqCst);

        self.update_stress_level(streak);
    }

    pub fn record_success(&self) {
        let current = self.current_backoff_secs.load(Ordering::SeqCst);
        let new_backoff = (current / 2).max(self.config.min_backoff_secs);
        self.current_backoff_secs
            .store(new_backoff, Ordering::SeqCst);

        let streak = self.failure_streak.load(Ordering::SeqCst);
        let new_streak = streak.saturating_sub(1);
        self.failure_streak.store(new_streak, Ordering::SeqCst);

        self.update_stress_level(new_streak);
    }

    pub fn check_value_increased(&self, current_value: u64) -> bool {
        let last = self.last_known_value.swap(current_value, Ordering::SeqCst);
        current_value > last
    }

    pub fn backoff_duration(&self) -> Duration {
        Duration::from_secs(self.current_backoff_secs.load(Ordering::SeqCst))
    }

    pub fn stress_level(&self) -> StressLevel {
        StressLevel::from(self.stress_level.load(Ordering::SeqCst))
    }

    pub fn is_stressed(&self) -> bool {
        self.stress_level() != StressLevel::Normal
    }

    pub fn failure_streak(&self) -> u64 {
        self.failure_streak.load(Ordering::SeqCst)
    }

    pub fn total_failures(&self) -> u64 {
        self.total_failures.load(Ordering::SeqCst)
    }

    pub fn should_pause_for_priority(&self, priority: u8) -> Option<Duration> {
        let streak = self.failure_streak.load(Ordering::SeqCst);
        let stress = self.stress_level();

        if priority >= 250 {
            return None;
        }

        if priority >= 180 {
            return match stress {
                StressLevel::Critical => Some(self.backoff_duration()),
                _ => None,
            };
        }

        if priority >= 100 {
            return match stress {
                StressLevel::Elevated | StressLevel::Critical => Some(self.backoff_duration()),
                StressLevel::Normal => None,
            };
        }

        if priority >= 50 {
            return if streak >= 1 {
                Some(self.backoff_duration())
            } else {
                None
            };
        }

        if streak >= 1 {
            Some(self.backoff_duration() * 2)
        } else {
            None
        }
    }

    fn update_stress_level(&self, streak: u64) {
        let level = if streak >= self.config.critical_threshold {
            StressLevel::Critical
        } else if streak >= self.config.elevated_threshold {
            StressLevel::Elevated
        } else {
            StressLevel::Normal
        };
        self.stress_level.store(level as u8, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
pub struct CombinedStressChecker {
    trackers: Vec<Arc<StressTracker>>,
}

impl CombinedStressChecker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, tracker: Arc<StressTracker>) {
        self.trackers.push(tracker);
    }

    pub fn from_trackers(trackers: impl IntoIterator<Item = Arc<StressTracker>>) -> Self {
        Self {
            trackers: trackers.into_iter().collect(),
        }
    }

    pub fn should_pause_for_priority(&self, priority: u8) -> Option<Duration> {
        self.trackers
            .iter()
            .filter_map(|t| t.should_pause_for_priority(priority))
            .max()
    }

    pub fn max_stress_level(&self) -> StressLevel {
        self.trackers
            .iter()
            .map(|t| t.stress_level())
            .max_by_key(|l| *l as u8)
            .unwrap_or(StressLevel::Normal)
    }
}
