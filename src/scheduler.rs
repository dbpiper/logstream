//! Multi-level priority scheduler (OS-style).
//! Higher priority tasks always preempt lower priority ones.
//! Supports arbitrary priority levels with fair scheduling within each level.

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::types::EnrichedEvent;

/// Priority levels (higher number = higher priority, like Unix nice inverse).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(pub u8);

impl Priority {
    /// Critical: real-time tail events - never delayed
    pub const CRITICAL: Priority = Priority(255);
    /// High: recent reconciliation
    pub const HIGH: Priority = Priority(192);
    /// Normal: standard backfill for today
    pub const NORMAL: Priority = Priority(128);
    /// Low: historical backfill
    pub const LOW: Priority = Priority(64);
    /// Idle: background healing, only when nothing else to do
    pub const IDLE: Priority = Priority(0);
}

/// A scheduled event with priority and sequence number for FIFO within priority.
struct ScheduledEvent {
    priority: Priority,
    sequence: u64,
    event: EnrichedEvent,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first, then lower sequence (FIFO within priority)
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => other.sequence.cmp(&self.sequence), // Lower seq = earlier
            ord => ord,
        }
    }
}

/// Sender handle for submitting events at a specific priority.
#[derive(Clone)]
pub struct PrioritySender {
    tx: mpsc::Sender<ScheduledEvent>,
    priority: Priority,
    sequence: Arc<AtomicU64>,
}

impl PrioritySender {
    /// Submit an event at this sender's priority level.
    pub async fn send(
        &self,
        event: EnrichedEvent,
    ) -> Result<(), mpsc::error::SendError<EnrichedEvent>> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let scheduled = ScheduledEvent {
            priority: self.priority,
            sequence: seq,
            event,
        };
        self.tx
            .send(scheduled)
            .await
            .map_err(|e| mpsc::error::SendError(e.0.event))
    }
}

/// Priority-based receiver that always returns highest priority events first.
pub struct Scheduler {
    rx: mpsc::Receiver<ScheduledEvent>,
    heap: BinaryHeap<ScheduledEvent>,
}

impl Scheduler {
    /// Receive the next highest-priority event.
    /// Higher priority events always returned before lower priority.
    /// Within same priority, FIFO order is maintained.
    pub async fn recv(&mut self) -> Option<EnrichedEvent> {
        // First, drain any pending events from channel into heap
        while let Ok(event) = self.rx.try_recv() {
            self.heap.push(event);
        }

        // If heap has events, return highest priority
        if let Some(scheduled) = self.heap.pop() {
            return Some(scheduled.event);
        }

        // Otherwise wait for next event
        self.rx.recv().await.map(|e| e.event)
    }
}

/// Create a scheduler with senders for each priority level.
pub struct SchedulerBuilder {
    capacity: usize,
}

impl SchedulerBuilder {
    pub fn new(capacity: usize) -> Self {
        Self { capacity }
    }

    /// Build the scheduler and return (scheduler, sender_factory).
    /// The sender_factory can create senders at any priority level.
    pub fn build(self) -> (Scheduler, SenderFactory) {
        let (tx, rx) = mpsc::channel(self.capacity);
        let sequence = Arc::new(AtomicU64::new(0));

        let scheduler = Scheduler {
            rx,
            heap: BinaryHeap::new(),
        };

        let factory = SenderFactory { tx, sequence };

        (scheduler, factory)
    }
}

/// Factory for creating priority senders.
#[derive(Clone)]
pub struct SenderFactory {
    tx: mpsc::Sender<ScheduledEvent>,
    sequence: Arc<AtomicU64>,
}

impl SenderFactory {
    /// Get a sender at the specified priority level.
    pub fn at(&self, priority: Priority) -> PrioritySender {
        PrioritySender {
            tx: self.tx.clone(),
            priority,
            sequence: self.sequence.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EventMeta;

    fn test_event(id: &str) -> EnrichedEvent {
        EnrichedEvent {
            timestamp: "2025-12-12T00:00:00Z".to_string(),
            event: EventMeta { id: id.to_string() },
            message: serde_json::Value::String("test".to_string()),
            parsed: None,
            target_index: None,
            tags: vec![],
        }
    }

    #[tokio::test]
    async fn test_critical_preempts_all() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let idle = factory.at(Priority::IDLE);
        let low = factory.at(Priority::LOW);
        let critical = factory.at(Priority::CRITICAL);

        // Send in reverse priority order
        idle.send(test_event("idle-1")).await.unwrap();
        low.send(test_event("low-1")).await.unwrap();
        idle.send(test_event("idle-2")).await.unwrap();
        critical.send(test_event("critical-1")).await.unwrap();
        low.send(test_event("low-2")).await.unwrap();

        // Should receive critical first
        assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-1");
        // Then low (in FIFO order)
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low-1");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low-2");
        // Then idle
        assert_eq!(scheduler.recv().await.unwrap().event.id, "idle-1");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "idle-2");
    }

    #[tokio::test]
    async fn test_fifo_within_priority() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let normal = factory.at(Priority::NORMAL);

        normal.send(test_event("first")).await.unwrap();
        normal.send(test_event("second")).await.unwrap();
        normal.send(test_event("third")).await.unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "first");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "second");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "third");
    }

    #[tokio::test]
    async fn test_all_priority_levels() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        factory
            .at(Priority::IDLE)
            .send(test_event("idle"))
            .await
            .unwrap();
        factory
            .at(Priority::LOW)
            .send(test_event("low"))
            .await
            .unwrap();
        factory
            .at(Priority::NORMAL)
            .send(test_event("normal"))
            .await
            .unwrap();
        factory
            .at(Priority::HIGH)
            .send(test_event("high"))
            .await
            .unwrap();
        factory
            .at(Priority::CRITICAL)
            .send(test_event("critical"))
            .await
            .unwrap();

        // Should come out in priority order
        assert_eq!(scheduler.recv().await.unwrap().event.id, "critical");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "high");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "normal");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "idle");
    }

    #[tokio::test]
    async fn test_custom_priority() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        // Custom priorities between standard levels
        let p100 = factory.at(Priority(100));
        let p150 = factory.at(Priority(150));
        let p200 = factory.at(Priority(200));

        p100.send(test_event("p100")).await.unwrap();
        p200.send(test_event("p200")).await.unwrap();
        p150.send(test_event("p150")).await.unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "p200");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "p150");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "p100");
    }

    #[tokio::test]
    async fn test_interleaved_send_recv() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let critical = factory.at(Priority::CRITICAL);
        let low = factory.at(Priority::LOW);

        low.send(test_event("low-1")).await.unwrap();
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low-1");

        critical.send(test_event("critical-1")).await.unwrap();
        low.send(test_event("low-2")).await.unwrap();

        // Critical should come first
        assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-1");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low-2");
    }

    #[tokio::test]
    async fn test_priority_boundary_values() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        // Test min and max priority values
        let min_priority = factory.at(Priority(0));
        let max_priority = factory.at(Priority(255));
        let mid_priority = factory.at(Priority(127));

        min_priority.send(test_event("min")).await.unwrap();
        mid_priority.send(test_event("mid")).await.unwrap();
        max_priority.send(test_event("max")).await.unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "max");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "mid");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "min");
    }

    #[tokio::test]
    async fn test_adjacent_priority_ordering() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        // Adjacent priorities should still maintain order
        let p63 = factory.at(Priority(63));
        let p64 = factory.at(Priority(64));
        let p65 = factory.at(Priority(65));

        p63.send(test_event("p63")).await.unwrap();
        p64.send(test_event("p64")).await.unwrap();
        p65.send(test_event("p65")).await.unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "p65");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "p64");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "p63");
    }

    #[tokio::test]
    async fn test_multiple_senders_same_priority() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        // Multiple senders at same priority should maintain FIFO
        let sender1 = factory.at(Priority::NORMAL);
        let sender2 = factory.at(Priority::NORMAL);
        let sender3 = factory.at(Priority::NORMAL);

        sender1.send(test_event("s1-first")).await.unwrap();
        sender2.send(test_event("s2-second")).await.unwrap();
        sender3.send(test_event("s3-third")).await.unwrap();
        sender1.send(test_event("s1-fourth")).await.unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "s1-first");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "s2-second");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "s3-third");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "s1-fourth");
    }

    #[tokio::test]
    async fn test_sender_clone() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let sender = factory.at(Priority::HIGH);
        let sender_clone = sender.clone();

        sender.send(test_event("original")).await.unwrap();
        sender_clone.send(test_event("clone")).await.unwrap();

        // Both should work and maintain FIFO
        assert_eq!(scheduler.recv().await.unwrap().event.id, "original");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "clone");
    }

    #[tokio::test]
    async fn test_factory_clone() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let factory_clone = factory.clone();

        factory
            .at(Priority::HIGH)
            .send(test_event("f1"))
            .await
            .unwrap();
        factory_clone
            .at(Priority::HIGH)
            .send(test_event("f2"))
            .await
            .unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "f1");
        assert_eq!(scheduler.recv().await.unwrap().event.id, "f2");
    }

    #[tokio::test]
    async fn test_high_volume_maintains_priority() {
        let (mut scheduler, factory) = SchedulerBuilder::new(50).build();

        let critical = factory.at(Priority::CRITICAL);
        let idle = factory.at(Priority::IDLE);

        // Send 10 idle, then 3 critical
        for i in 0..10 {
            idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
        }
        for i in 0..3 {
            critical
                .send(test_event(&format!("critical-{i}")))
                .await
                .unwrap();
        }

        // Critical should all come first
        for i in 0..3 {
            assert_eq!(
                scheduler.recv().await.unwrap().event.id,
                format!("critical-{i}")
            );
        }
        // Then all idle
        for i in 0..10 {
            assert_eq!(
                scheduler.recv().await.unwrap().event.id,
                format!("idle-{i}")
            );
        }
    }

    #[tokio::test]
    async fn test_late_high_priority_preempts() {
        let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

        let critical = factory.at(Priority::CRITICAL);
        let low = factory.at(Priority::LOW);

        // Send low priority events
        for i in 0..5 {
            low.send(test_event(&format!("low-{i}"))).await.unwrap();
        }

        // Receive one
        assert_eq!(scheduler.recv().await.unwrap().event.id, "low-0");

        // Now high priority arrives
        critical.send(test_event("critical-late")).await.unwrap();

        // More low priority
        low.send(test_event("low-5")).await.unwrap();

        // Critical should come next despite being late
        assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-late");

        // Then remaining low priority in order
        for i in 1..6 {
            let event = scheduler.recv().await.unwrap();
            assert_eq!(event.event.id, format!("low-{i}"));
        }
    }

    #[tokio::test]
    async fn test_mixed_priorities_stress() {
        let (mut scheduler, factory) = SchedulerBuilder::new(50).build();

        let priorities = [
            Priority::IDLE,
            Priority::LOW,
            Priority::NORMAL,
            Priority::HIGH,
            Priority::CRITICAL,
        ];

        // 3 rounds × 5 priorities = 15 events
        for round in 0..3 {
            for (i, &priority) in priorities.iter().enumerate() {
                factory
                    .at(priority)
                    .send(test_event(&format!("p{i}-r{round}")))
                    .await
                    .unwrap();
            }
        }

        // Should receive all CRITICAL first, then HIGH, etc.
        let expected_order = [4, 3, 2, 1, 0];
        for &priority_idx in &expected_order {
            for round in 0..3 {
                assert_eq!(
                    scheduler.recv().await.unwrap().event.id,
                    format!("p{priority_idx}-r{round}")
                );
            }
        }
    }

    #[test]
    fn test_priority_constants_values() {
        // Verify specific values
        assert_eq!(Priority::CRITICAL.0, 255);
        assert_eq!(Priority::HIGH.0, 192);
        assert_eq!(Priority::NORMAL.0, 128);
        assert_eq!(Priority::LOW.0, 64);
        assert_eq!(Priority::IDLE.0, 0);

        // Verify ordering via values
        let (critical, high, normal, low, idle) = (
            Priority::CRITICAL.0,
            Priority::HIGH.0,
            Priority::NORMAL.0,
            Priority::LOW.0,
            Priority::IDLE.0,
        );
        assert!(critical > high);
        assert!(high > normal);
        assert!(normal > low);
        assert!(low > idle);
    }

    #[test]
    fn test_priority_ord_trait() {
        // Verify Ord via runtime values
        let critical = Priority::CRITICAL;
        let high = Priority::HIGH;
        let normal = Priority::NORMAL;
        let low = Priority::LOW;
        let idle = Priority::IDLE;

        assert!(critical > high);
        assert!(high > normal);
        assert!(normal > low);
        assert!(low > idle);
        assert!(Priority(200) > Priority(100));
        assert_eq!(Priority(100), Priority(100));
    }

    #[tokio::test]
    async fn test_concurrent_producers() {
        let (mut scheduler, factory) = SchedulerBuilder::new(50).build();

        let critical = factory.at(Priority::CRITICAL);
        let low = factory.at(Priority::LOW);

        // Spawn concurrent producers (5 each)
        let critical_handle = tokio::spawn(async move {
            for i in 0..5 {
                critical.send(test_event(&format!("c{i}"))).await.unwrap();
            }
        });

        let low_handle = tokio::spawn(async move {
            for i in 0..5 {
                low.send(test_event(&format!("l{i}"))).await.unwrap();
            }
        });

        critical_handle.await.unwrap();
        low_handle.await.unwrap();

        // Collect all received events
        let mut critical_events = Vec::new();
        let mut low_events = Vec::new();

        for _ in 0..10 {
            let event = scheduler.recv().await.unwrap();
            if event.event.id.starts_with('c') {
                critical_events.push(event.event.id);
            } else {
                low_events.push(event.event.id);
            }
        }

        assert_eq!(critical_events.len(), 5);
        assert_eq!(low_events.len(), 5);

        // Verify FIFO within each priority
        for (i, id) in critical_events.iter().enumerate() {
            assert_eq!(id, &format!("c{i}"));
        }
        for (i, id) in low_events.iter().enumerate() {
            assert_eq!(id, &format!("l{i}"));
        }
    }

    #[tokio::test]
    async fn test_single_event() {
        let (mut scheduler, factory) = SchedulerBuilder::new(10).build();

        factory
            .at(Priority::NORMAL)
            .send(test_event("only-one"))
            .await
            .unwrap();

        assert_eq!(scheduler.recv().await.unwrap().event.id, "only-one");
    }

    #[tokio::test]
    async fn test_channel_closes_cleanly() {
        let (mut scheduler, factory) = SchedulerBuilder::new(10).build();

        factory
            .at(Priority::HIGH)
            .send(test_event("before-drop"))
            .await
            .unwrap();

        // Drop the factory (and all senders)
        drop(factory);

        // Should still receive pending event
        assert_eq!(scheduler.recv().await.unwrap().event.id, "before-drop");

        // Then None when channel is closed
        assert!(scheduler.recv().await.is_none());
    }

    #[tokio::test]
    async fn test_granular_priorities() {
        let (mut scheduler, factory) = SchedulerBuilder::new(20).build();

        // Sample 8 priority levels across the range
        let priorities: Vec<u8> = vec![0, 32, 64, 96, 128, 160, 192, 255];
        for &p in &priorities {
            factory
                .at(Priority(p))
                .send(test_event(&format!("p{p}")))
                .await
                .unwrap();
        }

        drop(factory);

        // Should receive in descending priority order
        let mut last_priority = 256u16;
        while let Some(event) = scheduler.recv().await {
            let p: u8 = event.event.id[1..].parse().unwrap();
            assert!((p as u16) < last_priority);
            last_priority = p as u16;
        }
    }

    #[tokio::test]
    async fn test_same_priority_fifo() {
        let (mut scheduler, factory) = SchedulerBuilder::new(50).build();

        let sender = factory.at(Priority::NORMAL);

        // Send 20 events at same priority
        for i in 0..20 {
            sender.send(test_event(&format!("{i}"))).await.unwrap();
        }

        drop(factory);

        // Should receive in exact FIFO order
        for i in 0..20 {
            assert_eq!(scheduler.recv().await.unwrap().event.id, format!("{i}"));
        }
    }
}
