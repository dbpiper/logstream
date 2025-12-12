//! Tests for the priority scheduler.

use logstream::scheduler::{Priority, SchedulerBuilder};
use logstream::types::{EnrichedEvent, EventMeta};

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
async fn test_critical_always_first() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    factory
        .at(Priority::IDLE)
        .send(test_event("idle-1"))
        .await
        .unwrap();
    factory
        .at(Priority::LOW)
        .send(test_event("low-1"))
        .await
        .unwrap();
    factory
        .at(Priority::IDLE)
        .send(test_event("idle-2"))
        .await
        .unwrap();
    factory
        .at(Priority::CRITICAL)
        .send(test_event("critical-1"))
        .await
        .unwrap();
    factory
        .at(Priority::LOW)
        .send(test_event("low-2"))
        .await
        .unwrap();

    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-1");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low-1");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low-2");
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

    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "high");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "normal");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "idle");
}

#[tokio::test]
async fn test_critical_preempts_during_processing() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let low = factory.at(Priority::LOW);

    low.send(test_event("low-1")).await.unwrap();
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low-1");

    low.send(test_event("low-2")).await.unwrap();
    critical.send(test_event("critical-1")).await.unwrap();
    low.send(test_event("low-3")).await.unwrap();

    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-1");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low-2");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "low-3");
}

#[tokio::test]
async fn test_priority_boundary_values() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    factory
        .at(Priority(0))
        .send(test_event("min"))
        .await
        .unwrap();
    factory
        .at(Priority(127))
        .send(test_event("mid"))
        .await
        .unwrap();
    factory
        .at(Priority(255))
        .send(test_event("max"))
        .await
        .unwrap();

    assert_eq!(scheduler.recv().await.unwrap().event.id, "max");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "mid");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "min");
}

#[tokio::test]
async fn test_sender_clone() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let sender = factory.at(Priority::HIGH);
    let sender_clone = sender.clone();

    sender.send(test_event("original")).await.unwrap();
    sender_clone.send(test_event("clone")).await.unwrap();

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
async fn test_high_volume_critical_priority() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let idle = factory.at(Priority::IDLE);

    for i in 0..10 {
        idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
    }

    for i in 0..3 {
        critical
            .send(test_event(&format!("critical-{i}")))
            .await
            .unwrap();
    }

    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-0");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-1");
    assert_eq!(scheduler.recv().await.unwrap().event.id, "critical-2");

    for i in 0..10 {
        assert_eq!(
            scheduler.recv().await.unwrap().event.id,
            format!("idle-{i}")
        );
    }
}

#[tokio::test]
async fn test_concurrent_producers() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let low = factory.at(Priority::LOW);

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
}

#[test]
fn test_priority_constants() {
    let critical = Priority::CRITICAL.0;
    let high = Priority::HIGH.0;
    let normal = Priority::NORMAL.0;
    let low = Priority::LOW.0;
    let idle = Priority::IDLE.0;

    assert_eq!(critical, 255);
    assert_eq!(high, 192);
    assert_eq!(normal, 128);
    assert_eq!(low, 64);
    assert_eq!(idle, 0);
    assert!(critical > high);
    assert!(high > normal);
    assert!(normal > low);
    assert!(low > idle);
}

#[test]
fn test_priority_ord() {
    let critical = Priority::CRITICAL;
    let high = Priority::HIGH;
    let normal = Priority::NORMAL;
    let low = Priority::LOW;
    let idle = Priority::IDLE;

    assert!(critical > high);
    assert!(high > normal);
    assert!(normal > low);
    assert!(low > idle);
    assert_eq!(Priority(100), Priority(100));
}

// ===== OPTIMALITY TESTS =====

#[tokio::test]
async fn test_realtime_never_starves_under_backfill_flood() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let idle = factory.at(Priority::IDLE);

    for i in 0..1000 {
        idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
    }

    for i in 0..10 {
        critical
            .send(test_event(&format!("critical-{i}")))
            .await
            .unwrap();
    }

    for i in 0..10 {
        let event = scheduler.recv().await.unwrap();
        assert_eq!(
            event.event.id,
            format!("critical-{i}"),
            "critical event {} should come before any idle events",
            i
        );
    }
}

#[tokio::test]
async fn test_interleaved_realtime_always_prioritized() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let idle = factory.at(Priority::IDLE);

    for i in 0..20 {
        idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
        if i % 5 == 0 {
            critical
                .send(test_event(&format!("critical-{}", i / 5)))
                .await
                .unwrap();
        }
    }

    let mut received_critical = 0;
    let mut received_idle = 0;
    let mut critical_finished = false;

    for _ in 0..24 {
        let event = scheduler.recv().await.unwrap();
        if event.event.id.starts_with("critical") {
            assert!(
                !critical_finished,
                "received critical after starting idle events"
            );
            received_critical += 1;
        } else {
            critical_finished = true;
            received_idle += 1;
        }
    }

    assert_eq!(received_critical, 4);
    assert_eq!(received_idle, 20);
}

#[tokio::test]
async fn test_throughput_not_degraded_by_empty_high_priority() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let idle = factory.at(Priority::IDLE);

    for i in 0..100 {
        idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
    }

    let start = std::time::Instant::now();
    for i in 0..100 {
        let event = scheduler.recv().await.unwrap();
        assert_eq!(event.event.id, format!("idle-{i}"));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 100,
        "throughput degraded: {}ms for 100 events",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_priority_ordering_maintained_under_concurrent_load() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let handles: Vec<_> = [
        (Priority::CRITICAL, "C", 50),
        (Priority::HIGH, "H", 100),
        (Priority::NORMAL, "N", 200),
        (Priority::LOW, "L", 300),
        (Priority::IDLE, "I", 500),
    ]
    .into_iter()
    .map(|(priority, prefix, count)| {
        let sender = factory.at(priority);
        tokio::spawn(async move {
            for i in 0..count {
                sender
                    .send(test_event(&format!("{prefix}-{i}")))
                    .await
                    .unwrap();
                if i % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    })
    .collect();

    for h in handles {
        h.await.unwrap();
    }

    let mut last_priority = 255u8;
    let mut counts = std::collections::HashMap::new();

    let total = 50 + 100 + 200 + 300 + 500;
    for _ in 0..total {
        let event = scheduler.recv().await.unwrap();
        let prefix = event.event.id.chars().next().unwrap();
        *counts.entry(prefix).or_insert(0) += 1;

        let priority = match prefix {
            'C' => 255,
            'H' => 192,
            'N' => 128,
            'L' => 64,
            'I' => 0,
            _ => panic!("unexpected prefix"),
        };

        assert!(
            priority <= last_priority,
            "priority went up: {} -> {}",
            last_priority,
            priority
        );
        last_priority = priority;
    }

    assert_eq!(counts.get(&'C'), Some(&50));
    assert_eq!(counts.get(&'H'), Some(&100));
    assert_eq!(counts.get(&'N'), Some(&200));
    assert_eq!(counts.get(&'L'), Some(&300));
    assert_eq!(counts.get(&'I'), Some(&500));
}

#[tokio::test]
async fn test_late_arriving_critical_preempts_immediately() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);
    let low = factory.at(Priority::LOW);

    for i in 0..50 {
        low.send(test_event(&format!("low-{i}"))).await.unwrap();
    }

    for i in 0..10 {
        let event = scheduler.recv().await.unwrap();
        assert_eq!(event.event.id, format!("low-{i}"));
    }

    critical.send(test_event("critical-late")).await.unwrap();

    let event = scheduler.recv().await.unwrap();
    assert_eq!(
        event.event.id, "critical-late",
        "late-arriving critical should preempt pending low events"
    );

    for i in 10..50 {
        let event = scheduler.recv().await.unwrap();
        assert_eq!(event.event.id, format!("low-{i}"));
    }
}

#[tokio::test]
async fn test_fairness_within_priority() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let sender1 = factory.at(Priority::NORMAL);
    let sender2 = factory.at(Priority::NORMAL);
    let sender3 = factory.at(Priority::NORMAL);

    for i in 0..10 {
        sender1.send(test_event(&format!("s1-{i}"))).await.unwrap();
        sender2.send(test_event(&format!("s2-{i}"))).await.unwrap();
        sender3.send(test_event(&format!("s3-{i}"))).await.unwrap();
    }

    let mut received = Vec::new();
    for _ in 0..30 {
        let event = scheduler.recv().await.unwrap();
        received.push(event.event.id);
    }

    assert_eq!(received[0], "s1-0");
    assert_eq!(received[1], "s2-0");
    assert_eq!(received[2], "s3-0");
    assert_eq!(received[3], "s1-1");
}

#[tokio::test]
async fn test_no_event_loss_under_load() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let total_per_priority = 100;
    let priorities = [
        Priority::CRITICAL,
        Priority::HIGH,
        Priority::NORMAL,
        Priority::LOW,
        Priority::IDLE,
    ];

    for priority in &priorities {
        let sender = factory.at(*priority);
        for i in 0..total_per_priority {
            sender
                .send(test_event(&format!("{:?}-{i}", priority.0)))
                .await
                .unwrap();
        }
    }

    let mut received = 0;
    for _ in 0..(priorities.len() * total_per_priority) {
        let _event = scheduler.recv().await.unwrap();
        received += 1;
    }

    assert_eq!(
        received,
        priorities.len() * total_per_priority,
        "some events were lost"
    );
}

#[tokio::test]
async fn test_channel_capacity_prevents_blocking_across_priorities() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let idle = factory.at(Priority::IDLE);
    // Fill idle channel with many events (less than capacity to avoid blocking)
    for i in 0..50000 {
        idle.send(test_event(&format!("idle-{i}"))).await.unwrap();
    }

    let critical = factory.at(Priority::CRITICAL);
    let start = std::time::Instant::now();
    for i in 0..10 {
        critical
            .send(test_event(&format!("critical-{i}")))
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 10,
        "critical send was delayed by full idle channel: {}ms",
        elapsed.as_millis()
    );

    for i in 0..10 {
        let event = scheduler.recv().await.unwrap();
        assert_eq!(event.event.id, format!("critical-{i}"));
    }
}

#[tokio::test]
async fn test_optimal_latency_for_critical() {
    let (mut scheduler, factory) = SchedulerBuilder::new(100).build();

    let critical = factory.at(Priority::CRITICAL);

    let mut latencies = Vec::new();

    for i in 0..100 {
        let start = std::time::Instant::now();
        critical.send(test_event(&format!("c-{i}"))).await.unwrap();
        let _event = scheduler.recv().await.unwrap();
        latencies.push(start.elapsed().as_micros());
    }

    let avg_latency = latencies.iter().sum::<u128>() / latencies.len() as u128;
    let max_latency = *latencies.iter().max().unwrap();

    assert!(
        avg_latency < 1000,
        "average critical latency too high: {}µs",
        avg_latency
    );
    assert!(
        max_latency < 10000,
        "max critical latency too high: {}µs",
        max_latency
    );
}
