//! Priority-based event routing for EnrichedEvents.
//! Routes events through separate channels per priority level to ensure
//! real-time events are never blocked by historical backfill.

use tokio::sync::mpsc;

use crate::process::Priority;
use crate::types::EnrichedEvent;

/// Channel capacity per priority level.
const CRITICAL_CAPACITY: usize = 10000;
const HIGH_CAPACITY: usize = 50000;
const NORMAL_CAPACITY: usize = 100000;
const LOW_CAPACITY: usize = 100000;
const IDLE_CAPACITY: usize = 100000;

/// Sender for a specific priority level.
#[derive(Clone)]
pub struct EventSender {
    tx: mpsc::Sender<EnrichedEvent>,
}

impl EventSender {
    pub async fn send(
        &self,
        event: EnrichedEvent,
    ) -> Result<(), mpsc::error::SendError<EnrichedEvent>> {
        self.tx.send(event).await
    }
}

/// Priority-based event router.
/// Routes events to the ES bulk sink in strict priority order.
/// CRITICAL is always fully drained before HIGH, HIGH before NORMAL, etc.
pub struct EventRouter {
    critical_rx: mpsc::Receiver<EnrichedEvent>,
    high_rx: mpsc::Receiver<EnrichedEvent>,
    normal_rx: mpsc::Receiver<EnrichedEvent>,
    low_rx: mpsc::Receiver<EnrichedEvent>,
    idle_rx: mpsc::Receiver<EnrichedEvent>,
}

impl EventRouter {
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

/// Factory for creating event senders at different priority levels.
#[derive(Clone)]
pub struct EventSenderFactory {
    critical_tx: mpsc::Sender<EnrichedEvent>,
    high_tx: mpsc::Sender<EnrichedEvent>,
    normal_tx: mpsc::Sender<EnrichedEvent>,
    low_tx: mpsc::Sender<EnrichedEvent>,
    idle_tx: mpsc::Sender<EnrichedEvent>,
}

impl EventSenderFactory {
    /// Get a sender for the specified priority level.
    pub fn at(&self, priority: Priority) -> EventSender {
        let tx = match priority.0 {
            255 => self.critical_tx.clone(),
            192..=254 => self.high_tx.clone(),
            128..=191 => self.normal_tx.clone(),
            64..=127 => self.low_tx.clone(),
            _ => self.idle_tx.clone(),
        };
        EventSender { tx }
    }
}

/// Build an event router with its sender factory.
pub fn build_event_router() -> (EventRouter, EventSenderFactory) {
    let (critical_tx, critical_rx) = mpsc::channel(CRITICAL_CAPACITY);
    let (high_tx, high_rx) = mpsc::channel(HIGH_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(NORMAL_CAPACITY);
    let (low_tx, low_rx) = mpsc::channel(LOW_CAPACITY);
    let (idle_tx, idle_rx) = mpsc::channel(IDLE_CAPACITY);

    let router = EventRouter {
        critical_rx,
        high_rx,
        normal_rx,
        low_rx,
        idle_rx,
    };

    let factory = EventSenderFactory {
        critical_tx,
        high_tx,
        normal_tx,
        low_tx,
        idle_tx,
    };

    (router, factory)
}
