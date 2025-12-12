use std::f64::consts::PI;
use std::sync::RwLock;

use crate::stress::{StressLevel, StressTracker};

const MAX_SAMPLES: usize = 1000;
const MIN_SAMPLES_FOR_STATS: usize = 5;

const HOUR_BANDWIDTH: f64 = 2.0;
const DAY_BANDWIDTH: f64 = 1.0;
const WEEK_BANDWIDTH: f64 = 0.5;
const MONTH_BANDWIDTH: f64 = 0.3;

const RECENCY_HALFLIFE_DAYS: f64 = 30.0;
const DIVERSITY_RECENCY_HALFLIFE: f64 = 90.0;

const REGIME_DETECTION_THRESHOLD: f64 = 2.0;
const RECENT_WINDOW_SIZE: usize = 10;

pub struct SeasonalStats {
    samples: RwLock<SampleBuffer>,
}

#[derive(Clone, Copy)]
struct Sample {
    timestamp_ms: i64,
    value: f64,
    hour_sin: f64,
    hour_cos: f64,
    day_sin: f64,
    day_cos: f64,
    week_sin: f64,
    week_cos: f64,
    month_sin: f64,
    month_cos: f64,
}

struct SampleBuffer {
    data: Vec<Sample>,
    recent_window: RecentWindow,
}

struct RecentWindow {
    values: [f64; RECENT_WINDOW_SIZE],
    index: usize,
    count: usize,
}

impl RecentWindow {
    fn new() -> Self {
        Self {
            values: [0.0; RECENT_WINDOW_SIZE],
            index: 0,
            count: 0,
        }
    }

    fn push(&mut self, value: f64) {
        self.values[self.index] = value;
        self.index = (self.index + 1) % RECENT_WINDOW_SIZE;
        if self.count < RECENT_WINDOW_SIZE {
            self.count += 1;
        }
    }

    fn mean(&self) -> Option<f64> {
        if self.count < 3 {
            return None;
        }
        let sum: f64 = self.values[..self.count].iter().sum();
        Some(sum / self.count as f64)
    }
}

impl Sample {
    fn new(timestamp_ms: i64, value: u64) -> Self {
        let secs = (timestamp_ms / 1000) as f64;

        let hour_of_day = (secs % 86400.0) / 86400.0;
        let day_of_week = ((secs / 86400.0) % 7.0) / 7.0;
        let week_of_month = ((secs / 86400.0) % 28.0) / 28.0;
        let month_of_year = ((secs / 86400.0) % 365.25) / 365.25;

        Self {
            timestamp_ms,
            value: value as f64,
            hour_sin: (2.0 * PI * hour_of_day).sin(),
            hour_cos: (2.0 * PI * hour_of_day).cos(),
            day_sin: (2.0 * PI * day_of_week).sin(),
            day_cos: (2.0 * PI * day_of_week).cos(),
            week_sin: (2.0 * PI * week_of_month).sin(),
            week_cos: (2.0 * PI * week_of_month).cos(),
            month_sin: (2.0 * PI * month_of_year).sin(),
            month_cos: (2.0 * PI * month_of_year).cos(),
        }
    }

    fn fourier_distance_sq(&self, other: &Sample) -> f64 {
        let hour =
            (self.hour_sin - other.hour_sin).powi(2) + (self.hour_cos - other.hour_cos).powi(2);
        let day = (self.day_sin - other.day_sin).powi(2) + (self.day_cos - other.day_cos).powi(2);
        let week =
            (self.week_sin - other.week_sin).powi(2) + (self.week_cos - other.week_cos).powi(2);
        let month =
            (self.month_sin - other.month_sin).powi(2) + (self.month_cos - other.month_cos).powi(2);

        hour / (HOUR_BANDWIDTH * HOUR_BANDWIDTH)
            + day / (DAY_BANDWIDTH * DAY_BANDWIDTH)
            + week / (WEEK_BANDWIDTH * WEEK_BANDWIDTH)
            + month / (MONTH_BANDWIDTH * MONTH_BANDWIDTH)
    }

    fn prediction_similarity(&self, other: &Sample, reference_ms: i64) -> f64 {
        let dist_sq = self.fourier_distance_sq(other);
        let pattern_weight = (-dist_sq / 2.0).exp();

        let age_days = (reference_ms - other.timestamp_ms).max(0) as f64 / 86_400_000.0;
        let recency_weight = (-age_days * (2.0_f64.ln()) / RECENCY_HALFLIFE_DAYS).exp();

        pattern_weight * recency_weight
    }

    fn diversity_value(&self, others: &[Sample], reference_ms: i64) -> f64 {
        if others.is_empty() {
            return f64::MAX;
        }

        let age_days = (reference_ms - self.timestamp_ms).max(0) as f64 / 86_400_000.0;
        let recency_bonus = (-age_days * (2.0_f64.ln()) / DIVERSITY_RECENCY_HALFLIFE).exp();

        let mut min_dist = f64::MAX;
        for other in others {
            if std::ptr::eq(self, other) {
                continue;
            }
            let dist = self.fourier_distance_sq(other);
            min_dist = min_dist.min(dist);
        }

        min_dist.sqrt() * (0.5 + 0.5 * recency_bonus)
    }
}

impl SampleBuffer {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(MAX_SAMPLES),
            recent_window: RecentWindow::new(),
        }
    }

    fn add(&mut self, sample: Sample) {
        self.recent_window.push(sample.value);

        if self.data.len() >= MAX_SAMPLES {
            self.evict_least_diverse(sample.timestamp_ms);
        }

        self.data.push(sample);
    }

    fn evict_least_diverse(&mut self, now_ms: i64) {
        if self.data.len() < 2 {
            return;
        }

        let mut min_diversity = f64::MAX;
        let mut evict_idx = 0;

        for (i, sample) in self.data.iter().enumerate() {
            let diversity = sample.diversity_value(&self.data, now_ms);
            if diversity < min_diversity {
                min_diversity = diversity;
                evict_idx = i;
            }
        }

        self.data.swap_remove(evict_idx);
    }

    fn reference_time(&self) -> i64 {
        self.data.iter().map(|s| s.timestamp_ms).max().unwrap_or(0)
    }
}

impl Default for SeasonalStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SeasonalStats {
    pub fn new() -> Self {
        Self {
            samples: RwLock::new(SampleBuffer::new()),
        }
    }

    pub fn record_verified(&self, timestamp_ms: i64, count: u64) {
        let sample = Sample::new(timestamp_ms, count);
        let mut buffer = self.samples.write().unwrap();
        buffer.add(sample);
    }

    pub fn expected_range(&self, timestamp_ms: i64) -> Option<(f64, f64)> {
        let buffer = self.samples.read().unwrap();

        if buffer.data.len() < MIN_SAMPLES_FOR_STATS {
            return None;
        }

        let query = Sample::new(timestamp_ms, 0);
        let reference_ms = buffer.reference_time().max(timestamp_ms);

        let mut total_weight = 0.0;
        let mut weighted_sum = 0.0;
        let mut weighted_sq_sum = 0.0;

        for sample in &buffer.data {
            let weight = query.prediction_similarity(sample, reference_ms);
            if weight > 1e-10 {
                total_weight += weight;
                weighted_sum += weight * sample.value;
                weighted_sq_sum += weight * sample.value * sample.value;
            }
        }

        if total_weight < 1e-10 {
            return None;
        }

        let mean = weighted_sum / total_weight;
        let variance = (weighted_sq_sum / total_weight) - (mean * mean);
        let stddev = variance.max(0.0).sqrt();

        let regime_factor = self.regime_adjustment_internal(&buffer, mean);

        Some((mean * regime_factor, stddev))
    }

    fn regime_adjustment_internal(&self, buffer: &SampleBuffer, expected_mean: f64) -> f64 {
        let recent_mean = match buffer.recent_window.mean() {
            Some(m) => m,
            None => return 1.0,
        };

        if expected_mean < 1.0 {
            return 1.0;
        }

        let ratio = recent_mean / expected_mean;
        let lower = 1.0 / REGIME_DETECTION_THRESHOLD;

        if !(lower..=REGIME_DETECTION_THRESHOLD).contains(&ratio) {
            ratio.sqrt()
        } else {
            1.0
        }
    }

    pub fn is_feasible(
        &self,
        timestamp_ms: i64,
        range_ms: i64,
        cw_count: u64,
        stress: &StressTracker,
    ) -> FeasibilityResult {
        let stress_level = stress.stress_level();
        let sigma_multiplier = match stress_level {
            StressLevel::Normal => 4.0,
            StressLevel::Elevated => 2.5,
            StressLevel::Critical => 1.5,
        };

        let range_hours = (range_ms as f64) / 3_600_000.0;
        let mid_timestamp = timestamp_ms + range_ms / 2;

        let (expected_mean, stddev) = match self.expected_range(mid_timestamp) {
            Some((m, s)) => (m * range_hours, s * range_hours.sqrt()),
            None => return FeasibilityResult::NoHistory,
        };

        let cw_f = cw_count as f64;
        let deviation = (cw_f - expected_mean).abs();
        let threshold = stddev * sigma_multiplier;

        let min_threshold = expected_mean * 0.1;
        let effective_threshold = threshold.max(min_threshold).max(10.0);

        if deviation <= effective_threshold {
            FeasibilityResult::Feasible {
                expected: expected_mean,
                stddev,
                sigma_used: sigma_multiplier,
            }
        } else {
            FeasibilityResult::Suspicious {
                expected: expected_mean,
                stddev,
                deviation,
                sigma_used: sigma_multiplier,
            }
        }
    }

    pub fn sample_count(&self, _timestamp_ms: i64) -> u64 {
        let buffer = self.samples.read().unwrap();
        buffer.data.len() as u64
    }

    pub fn detect_regime_change(&self, timestamp_ms: i64) -> Option<f64> {
        let (expected_mean, _) = self.expected_range(timestamp_ms)?;

        let buffer = self.samples.read().unwrap();
        let recent_mean = buffer.recent_window.mean()?;

        if expected_mean < 1.0 {
            return None;
        }

        let ratio = recent_mean / expected_mean;
        let lower = 1.0 / REGIME_DETECTION_THRESHOLD;

        if !(lower..=REGIME_DETECTION_THRESHOLD).contains(&ratio) {
            Some(ratio)
        } else {
            None
        }
    }

    pub fn total_samples(&self) -> usize {
        let buffer = self.samples.read().unwrap();
        buffer.data.len()
    }

    pub fn diversity_stats(&self) -> Option<(f64, f64)> {
        let buffer = self.samples.read().unwrap();
        if buffer.data.len() < 2 {
            return None;
        }

        let now = buffer.reference_time();
        let diversities: Vec<f64> = buffer
            .data
            .iter()
            .map(|s| s.diversity_value(&buffer.data, now))
            .filter(|d| d.is_finite())
            .collect();

        if diversities.is_empty() {
            return None;
        }

        let mean = diversities.iter().sum::<f64>() / diversities.len() as f64;
        let min = diversities.iter().cloned().fold(f64::MAX, f64::min);

        Some((mean, min))
    }
}

#[derive(Debug, Clone)]
pub enum FeasibilityResult {
    NoHistory,
    Feasible {
        expected: f64,
        stddev: f64,
        sigma_used: f64,
    },
    Suspicious {
        expected: f64,
        stddev: f64,
        deviation: f64,
        sigma_used: f64,
    },
}

impl FeasibilityResult {
    pub fn is_feasible(&self) -> bool {
        matches!(
            self,
            FeasibilityResult::Feasible { .. } | FeasibilityResult::NoHistory
        )
    }

    pub fn should_record(&self) -> bool {
        matches!(self, FeasibilityResult::Feasible { .. })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DataIntegrity {
    Valid,
    Partial { coverage: f64 },
    Invalid,
}

impl DataIntegrity {
    pub fn is_usable(&self) -> bool {
        !matches!(self, DataIntegrity::Invalid)
    }

    pub fn should_upsert(&self) -> bool {
        matches!(self, DataIntegrity::Partial { .. })
    }
}

pub fn validate_event_integrity(
    timestamps: &[i64],
    range_start: i64,
    range_end: i64,
) -> DataIntegrity {
    if timestamps.is_empty() {
        return DataIntegrity::Invalid;
    }

    if timestamps.len() < 3 {
        return DataIntegrity::Partial { coverage: 0.1 };
    }

    let range_ms = (range_end - range_start).max(1) as f64;
    let mut sorted = timestamps.to_vec();
    sorted.sort_unstable();

    let features: Vec<FourierPoint> = sorted.iter().map(|&ts| FourierPoint::new(ts)).collect();

    let temporal_score = temporal_coverage_score(&sorted, range_start, range_end);
    let fourier_diversity = fourier_diversity_score(&features);
    let fourier_uniformity = fourier_uniformity_score(&features, range_ms);
    let gap_score = gap_distribution_score(&sorted, range_ms);

    let weighted_score = temporal_score * 0.25
        + fourier_diversity * 0.30
        + fourier_uniformity * 0.25
        + gap_score * 0.20;

    if weighted_score > 0.7 {
        DataIntegrity::Valid
    } else if weighted_score > 0.3 {
        DataIntegrity::Partial {
            coverage: weighted_score,
        }
    } else {
        DataIntegrity::Invalid
    }
}

#[derive(Clone, Copy)]
struct FourierPoint {
    hour_sin: f64,
    hour_cos: f64,
    progress_sin: f64,
    progress_cos: f64,
}

impl FourierPoint {
    fn new(timestamp_ms: i64) -> Self {
        let secs = (timestamp_ms / 1000) as f64;
        let hour_of_day = (secs % 86400.0) / 86400.0;

        Self {
            hour_sin: (2.0 * PI * hour_of_day).sin(),
            hour_cos: (2.0 * PI * hour_of_day).cos(),
            progress_sin: 0.0,
            progress_cos: 1.0,
        }
    }

    fn distance_sq(&self, other: &FourierPoint) -> f64 {
        let hour =
            (self.hour_sin - other.hour_sin).powi(2) + (self.hour_cos - other.hour_cos).powi(2);
        let progress = (self.progress_sin - other.progress_sin).powi(2)
            + (self.progress_cos - other.progress_cos).powi(2);
        hour + progress
    }
}

fn temporal_coverage_score(sorted: &[i64], start: i64, end: i64) -> f64 {
    if sorted.len() < 2 {
        return 0.5;
    }

    let range = (end - start).max(1) as f64;
    let first = sorted.first().copied().unwrap_or(start);
    let last = sorted.last().copied().unwrap_or(end);
    let data_range = (last - first).max(0) as f64;

    let coverage = data_range / range;
    let start_dist = ((first - start).abs() as f64 / range).min(1.0);
    let end_dist = ((end - last).abs() as f64 / range).min(1.0);

    let boundary_penalty = 1.0 - (start_dist + end_dist) / 4.0;

    (coverage * boundary_penalty).min(1.0)
}

fn fourier_diversity_score(features: &[FourierPoint]) -> f64 {
    if features.len() < 3 {
        return 0.5;
    }

    let n = features.len();
    let sample_size = n.min(50);
    let step = n / sample_size;

    let mut total_min_dist = 0.0;
    let mut count = 0;

    for i in (0..n).step_by(step.max(1)) {
        let mut min_dist = f64::MAX;
        for (j, other) in features.iter().enumerate() {
            if i != j {
                let dist = features[i].distance_sq(other);
                min_dist = min_dist.min(dist);
            }
        }
        if min_dist < f64::MAX {
            total_min_dist += min_dist.sqrt();
            count += 1;
        }
    }

    if count == 0 {
        return 0.5;
    }

    let avg_min_dist = total_min_dist / count as f64;
    let max_possible = 4.0_f64.sqrt();

    (avg_min_dist / max_possible * 4.0).min(1.0)
}

fn fourier_uniformity_score(features: &[FourierPoint], range_ms: f64) -> f64 {
    if features.len() < 4 {
        return 0.5;
    }

    let hour_variance =
        compute_circular_variance(features.iter().map(|f| (f.hour_sin, f.hour_cos)));
    let progress_variance =
        compute_circular_variance(features.iter().map(|f| (f.progress_sin, f.progress_cos)));

    let uniformity = (hour_variance + progress_variance) / 2.0;

    let range_hours = range_ms / 3_600_000.0;
    let expected_hour_variance = if range_hours >= 24.0 {
        0.8
    } else {
        range_hours / 30.0
    };

    if expected_hour_variance > 0.0 {
        (uniformity / expected_hour_variance).min(1.0)
    } else {
        uniformity
    }
}

fn compute_circular_variance(points: impl Iterator<Item = (f64, f64)>) -> f64 {
    let mut sum_sin = 0.0;
    let mut sum_cos = 0.0;
    let mut count = 0;

    for (sin_val, cos_val) in points {
        sum_sin += sin_val;
        sum_cos += cos_val;
        count += 1;
    }

    if count == 0 {
        return 0.0;
    }

    let mean_sin = sum_sin / count as f64;
    let mean_cos = sum_cos / count as f64;
    let r = (mean_sin.powi(2) + mean_cos.powi(2)).sqrt();

    1.0 - r
}

fn gap_distribution_score(sorted: &[i64], range_ms: f64) -> f64 {
    if sorted.len() < 3 {
        return 0.5;
    }

    let gaps: Vec<f64> = sorted
        .windows(2)
        .map(|w| (w[1] - w[0]).max(0) as f64)
        .collect();

    if gaps.is_empty() {
        return 0.5;
    }

    let n = gaps.len();
    let mean_gap = gaps.iter().sum::<f64>() / n as f64;
    let variance = gaps.iter().map(|g| (g - mean_gap).powi(2)).sum::<f64>() / n as f64;
    let stddev = variance.sqrt();

    let expected_gap = range_ms / (sorted.len() as f64);
    let cv = if mean_gap > 0.0 {
        stddev / mean_gap
    } else {
        1.0
    };

    let mean_score = if expected_gap > 0.0 {
        1.0 - ((mean_gap - expected_gap).abs() / expected_gap).min(1.0)
    } else {
        0.5
    };

    let cv_score = (-cv / 2.0).exp();

    let max_gap = gaps.iter().cloned().fold(0.0, f64::max);
    let max_gap_ratio = max_gap / range_ms;
    let max_gap_score = if max_gap_ratio > 0.5 {
        0.3
    } else {
        1.0 - max_gap_ratio
    };

    mean_score * 0.3 + cv_score * 0.4 + max_gap_score * 0.3
}
