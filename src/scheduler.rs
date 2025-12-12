//! Multi-level priority scheduler (OS-style).
//! Uses SEPARATE channels per priority to prevent backfill from blocking real-time.
//! Real-time (CRITICAL) is ALWAYS drained completely before any lower priority.

use tokio::sync::mpsc;

use crate::types::EnrichedEvent;

/// Priority levels (higher number = higher priority).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(pub u8);

impl Priority {
    /// Critical: real-time tail events - never delayed
    pub const CRITICAL: Priority = Priority(255);
    /// High: recent reconciliation
    pub const HIGH: Priority = Priority(192);
    /// Normal: standard backfill for today
    pub const NORMAL: Priority = Priority(128);
    /// Low: historical backfill (last week)
    pub const LOW: Priority = Priority(64);
    /// Idle: old backfill, healing - only when nothing else
    pub const IDLE: Priority = Priority(0);
}

/// Channel capacity per priority level.
const CRITICAL_CAPACITY: usize = 10000;
const HIGH_CAPACITY: usize = 50000;
const NORMAL_CAPACITY: usize = 100000;
const LOW_CAPACITY: usize = 100000;
const IDLE_CAPACITY: usize = 100000;

/// Sender for a specific priority level.
#[derive(Clone)]
pub struct PrioritySender {
    tx: mpsc::Sender<EnrichedEvent>,
}

impl PrioritySender {
    pub async fn send(
        &self,
        event: EnrichedEvent,
    ) -> Result<(), mpsc::error::SendError<EnrichedEvent>> {
        self.tx.send(event).await
    }
}

/// Multi-level priority scheduler.
/// ALWAYS drains higher priority completely before touching lower.
pub struct Scheduler {
    critical_rx: mpsc::Receiver<EnrichedEvent>,
    high_rx: mpsc::Receiver<EnrichedEvent>,
    normal_rx: mpsc::Receiver<EnrichedEvent>,
    low_rx: mpsc::Receiver<EnrichedEvent>,
    idle_rx: mpsc::Receiver<EnrichedEvent>,
}

impl Scheduler {
    /// Receive next event, strictly prioritized.
    /// CRITICAL is always fully drained before HIGH, HIGH before NORMAL, etc.
    pub async fn recv(&mut self) -> Option<EnrichedEvent> {
        // Try CRITICAL first (non-blocking)
        if let Ok(event) = self.critical_rx.try_recv() {
            return Some(event);
        }

        // Try HIGH
        if let Ok(event) = self.high_rx.try_recv() {
            return Some(event);
        }

        // Try NORMAL
        if let Ok(event) = self.normal_rx.try_recv() {
            return Some(event);
        }

        // Try LOW
        if let Ok(event) = self.low_rx.try_recv() {
            return Some(event);
        }

        // Try IDLE
        if let Ok(event) = self.idle_rx.try_recv() {
            return Some(event);
        }

        // Nothing pending - wait for any event, but prefer higher priority
        tokio::select! {
            biased; // Check in order

            result = self.critical_rx.recv() => result,
            result = self.high_rx.recv() => result,
            result = self.normal_rx.recv() => result,
            result = self.low_rx.recv() => result,
            result = self.idle_rx.recv() => result,
        }
    }
}

/// Factory for creating priority senders.
#[derive(Clone)]
pub struct SenderFactory {
    critical_tx: mpsc::Sender<EnrichedEvent>,
    high_tx: mpsc::Sender<EnrichedEvent>,
    normal_tx: mpsc::Sender<EnrichedEvent>,
    low_tx: mpsc::Sender<EnrichedEvent>,
    idle_tx: mpsc::Sender<EnrichedEvent>,
}

impl SenderFactory {
    /// Get a sender for the specified priority level.
    pub fn at(&self, priority: Priority) -> PrioritySender {
        let tx = match priority.0 {
            255 => self.critical_tx.clone(),
            192..=254 => self.high_tx.clone(),
            128..=191 => self.normal_tx.clone(),
            64..=127 => self.low_tx.clone(),
            _ => self.idle_tx.clone(),
        };
        PrioritySender { tx }
    }
}

/// Builder for the scheduler.
pub struct SchedulerBuilder;

impl SchedulerBuilder {
    pub fn new(_capacity: usize) -> Self {
        Self
    }

    pub fn build(self) -> (Scheduler, SenderFactory) {
        let (critical_tx, critical_rx) = mpsc::channel(CRITICAL_CAPACITY);
        let (high_tx, high_rx) = mpsc::channel(HIGH_CAPACITY);
        let (normal_tx, normal_rx) = mpsc::channel(NORMAL_CAPACITY);
        let (low_tx, low_rx) = mpsc::channel(LOW_CAPACITY);
        let (idle_tx, idle_rx) = mpsc::channel(IDLE_CAPACITY);

        let scheduler = Scheduler {
            critical_rx,
            high_rx,
            normal_rx,
            low_rx,
            idle_rx,
        };

        let factory = SenderFactory {
            critical_tx,
            high_tx,
            normal_tx,
            low_tx,
            idle_tx,
        };

        (scheduler, factory)
    }
}
