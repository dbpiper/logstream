//! Tests for the priority-based event router.

use logstream::event_router::build_event_router;
use logstream::process::Priority;
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
    let (mut router, factory) = build_event_router();

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

    assert_eq!(router.recv().await.unwrap().event.id, "critical-1");
    assert_eq!(router.recv().await.unwrap().event.id, "low-1");
    assert_eq!(router.recv().await.unwrap().event.id, "low-2");
    assert_eq!(router.recv().await.unwrap().event.id, "idle-1");
    assert_eq!(router.recv().await.unwrap().event.id, "idle-2");
}

#[tokio::test]
async fn test_all_priority_levels() {
    let (mut router, factory) = build_event_router();

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

    assert_eq!(router.recv().await.unwrap().event.id, "critical");
    assert_eq!(router.recv().await.unwrap().event.id, "high");
    assert_eq!(router.recv().await.unwrap().event.id, "normal");
    assert_eq!(router.recv().await.unwrap().event.id, "low");
    assert_eq!(router.recv().await.unwrap().event.id, "idle");
}

#[tokio::test]
async fn test_priority_constants() {
    assert_eq!(Priority::CRITICAL.value(), 255);
    assert_eq!(Priority::HIGH.value(), 192);
    assert_eq!(Priority::NORMAL.value(), 128);
    assert_eq!(Priority::LOW.value(), 64);
    assert_eq!(Priority::IDLE.value(), 0);

    assert!(Priority::CRITICAL > Priority::HIGH);
    assert!(Priority::HIGH > Priority::NORMAL);
    assert!(Priority::NORMAL > Priority::LOW);
    assert!(Priority::LOW > Priority::IDLE);
}

#[tokio::test]
async fn test_priority_ord() {
    let mut priorities = vec![
        Priority::NORMAL,
        Priority::CRITICAL,
        Priority::IDLE,
        Priority::HIGH,
        Priority::LOW,
    ];
    priorities.sort();
    assert_eq!(
        priorities,
        vec![
            Priority::IDLE,
            Priority::LOW,
            Priority::NORMAL,
            Priority::HIGH,
            Priority::CRITICAL
        ]
    );
}

#[tokio::test]
async fn test_priority_boundary_values() {
    let (mut router, factory) = build_event_router();

    // Send at boundary values
    factory
        .at(Priority(0))
        .send(test_event("zero"))
        .await
        .unwrap();
    factory
        .at(Priority(255))
        .send(test_event("max"))
        .await
        .unwrap();

    // Max (CRITICAL) should come first
    assert_eq!(router.recv().await.unwrap().event.id, "max");
    assert_eq!(router.recv().await.unwrap().event.id, "zero");
}

#[tokio::test]
async fn test_sender_clone() {
    let (_router, factory) = build_event_router();
    let sender = factory.at(Priority::NORMAL);
    let _cloned = sender.clone();
}

#[tokio::test]
async fn test_factory_clone() {
    let (_router, factory) = build_event_router();
    let _cloned = factory.clone();
}

#[tokio::test]
async fn test_fifo_within_priority() {
    let (mut router, factory) = build_event_router();

    for i in 0..10 {
        factory
            .at(Priority::NORMAL)
            .send(test_event(&format!("normal-{}", i)))
            .await
            .unwrap();
    }

    for i in 0..10 {
        let event = router.recv().await.unwrap();
        assert_eq!(event.event.id, format!("normal-{}", i));
    }
}

#[tokio::test]
async fn test_concurrent_producers() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let (mut router, factory) = build_event_router();
    let barrier = Arc::new(Barrier::new(5));

    let mut handles = vec![];
    for priority in [
        Priority::CRITICAL,
        Priority::HIGH,
        Priority::NORMAL,
        Priority::LOW,
        Priority::IDLE,
    ] {
        let factory = factory.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for i in 0..10 {
                factory
                    .at(priority)
                    .send(test_event(&format!("{:?}-{}", priority, i)))
                    .await
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All 50 events should be receivable
    let mut received = 0;
    for _ in 0..50 {
        if router.recv().await.is_some() {
            received += 1;
        }
    }
    assert_eq!(received, 50);
}

#[tokio::test]
async fn test_high_volume_critical_priority() {
    let (mut router, factory) = build_event_router();

    // Send many events at different priorities
    for i in 0..100 {
        factory
            .at(Priority::IDLE)
            .send(test_event(&format!("idle-{}", i)))
            .await
            .unwrap();
    }
    for i in 0..10 {
        factory
            .at(Priority::CRITICAL)
            .send(test_event(&format!("critical-{}", i)))
            .await
            .unwrap();
    }

    // First 10 should all be CRITICAL
    for i in 0..10 {
        let event = router.recv().await.unwrap();
        assert_eq!(
            event.event.id,
            format!("critical-{}", i),
            "Event {} should be critical",
            i
        );
    }
}

#[tokio::test]
async fn test_interleaved_realtime_always_prioritized() {
    let (mut router, factory) = build_event_router();

    // Simulate interleaved production
    for i in 0..5 {
        factory
            .at(Priority::IDLE)
            .send(test_event(&format!("idle-{}", i)))
            .await
            .unwrap();
        factory
            .at(Priority::CRITICAL)
            .send(test_event(&format!("critical-{}", i)))
            .await
            .unwrap();
    }

    // All critical should come first
    for i in 0..5 {
        let event = router.recv().await.unwrap();
        assert_eq!(event.event.id, format!("critical-{}", i));
    }
}

#[tokio::test]
async fn test_critical_preempts_during_processing() {
    let (mut router, factory) = build_event_router();

    // Send low priority events
    for i in 0..5 {
        factory
            .at(Priority::LOW)
            .send(test_event(&format!("low-{}", i)))
            .await
            .unwrap();
    }

    // Receive first low priority
    let first = router.recv().await.unwrap();
    assert!(first.event.id.starts_with("low-"));

    // Now send critical
    factory
        .at(Priority::CRITICAL)
        .send(test_event("critical-urgent"))
        .await
        .unwrap();

    // Next should be critical (preemption)
    let next = router.recv().await.unwrap();
    assert_eq!(next.event.id, "critical-urgent");
}

#[tokio::test]
async fn test_throughput_not_degraded_by_empty_high_priority() {
    use std::time::Instant;

    let (mut router, factory) = build_event_router();

    // Only send low priority
    for i in 0..100 {
        factory
            .at(Priority::IDLE)
            .send(test_event(&format!("idle-{}", i)))
            .await
            .unwrap();
    }

    let start = Instant::now();
    for _ in 0..100 {
        router.recv().await.unwrap();
    }
    let elapsed = start.elapsed();

    // Should complete quickly even though we check higher priorities first
    assert!(elapsed.as_millis() < 1000, "Took too long: {:?}", elapsed);
}

#[tokio::test]
async fn test_no_event_loss_under_load() {
    use std::collections::HashSet;
    use std::sync::Arc;

    let (mut router, factory) = build_event_router();
    let factory = Arc::new(factory);

    // Spawn multiple producers
    let mut handles = vec![];
    for producer_id in 0..4 {
        let factory = factory.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                let priority = match i % 5 {
                    0 => Priority::CRITICAL,
                    1 => Priority::HIGH,
                    2 => Priority::NORMAL,
                    3 => Priority::LOW,
                    _ => Priority::IDLE,
                };
                factory
                    .at(priority)
                    .send(test_event(&format!("p{}-{}", producer_id, i)))
                    .await
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Should receive all 200 events
    let mut received = HashSet::new();
    for _ in 0..200 {
        let event = router.recv().await.unwrap();
        received.insert(event.event.id.clone());
    }
    assert_eq!(received.len(), 200);
}

#[tokio::test]
async fn test_late_arriving_critical_preempts_immediately() {
    let (mut router, factory) = build_event_router();

    // Fill up with low priority
    for i in 0..10 {
        factory
            .at(Priority::LOW)
            .send(test_event(&format!("low-{}", i)))
            .await
            .unwrap();
    }

    // Process a few
    for _ in 0..3 {
        router.recv().await.unwrap();
    }

    // Now critical arrives
    factory
        .at(Priority::CRITICAL)
        .send(test_event("critical-late"))
        .await
        .unwrap();

    // Next must be critical
    let next = router.recv().await.unwrap();
    assert_eq!(next.event.id, "critical-late");
}

#[tokio::test]
async fn test_fairness_within_priority() {
    let (mut router, factory) = build_event_router();

    for i in 0..100 {
        factory
            .at(Priority::NORMAL)
            .send(test_event(&format!("normal-{}", i)))
            .await
            .unwrap();
    }

    // Should receive in FIFO order within same priority
    for i in 0..100 {
        let event = router.recv().await.unwrap();
        assert_eq!(event.event.id, format!("normal-{}", i));
    }
}

#[tokio::test]
async fn test_realtime_never_starves_under_backfill_flood() {
    let (mut router, factory) = build_event_router();

    // Spawn backfill producer (lots of IDLE events)
    let backfill_factory = factory.clone();
    let backfill_handle = tokio::spawn(async move {
        for i in 0..1000 {
            let _ = backfill_factory
                .at(Priority::IDLE)
                .send(test_event(&format!("backfill-{}", i)))
                .await;
        }
    });

    // Small delay to ensure backfill starts
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Now send critical "real-time" event
    factory
        .at(Priority::CRITICAL)
        .send(test_event("realtime-urgent"))
        .await
        .unwrap();

    // First event we receive must be the realtime one
    let first = router.recv().await.unwrap();
    assert_eq!(first.event.id, "realtime-urgent");

    backfill_handle.await.unwrap();
}

#[tokio::test]
async fn test_channel_capacity_prevents_blocking_across_priorities() {
    let (_router, factory) = build_event_router();

    // Even with full IDLE queue, CRITICAL should work
    for i in 0..10000 {
        // Fill IDLE channel
        let _ = factory
            .at(Priority::IDLE)
            .send(test_event(&format!("idle-{}", i)))
            .await;
    }

    // CRITICAL should still accept
    let result = factory
        .at(Priority::CRITICAL)
        .send(test_event("critical-after-full"))
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_priority_ordering_maintained_under_concurrent_load() {
    use std::sync::Arc;

    let (mut router, factory) = build_event_router();
    let factory = Arc::new(factory);

    // Spawn producers at different priorities
    let handles: Vec<_> = [
        Priority::IDLE,
        Priority::LOW,
        Priority::NORMAL,
        Priority::HIGH,
        Priority::CRITICAL,
    ]
    .iter()
    .map(|&priority| {
        let factory = factory.clone();
        tokio::spawn(async move {
            for i in 0..10 {
                factory
                    .at(priority)
                    .send(test_event(&format!("{:?}-{}", priority, i)))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
    })
    .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    // Receive first 10 events and verify CRITICAL comes first
    let mut critical_count = 0;
    for _ in 0..10 {
        let event = router.recv().await.unwrap();
        if event.event.id.starts_with("Priority(255)") {
            critical_count += 1;
        }
    }

    // Most of the first 10 should be CRITICAL
    assert!(
        critical_count >= 5,
        "Expected at least 5 CRITICAL in first 10, got {}",
        critical_count
    );
}

#[tokio::test]
async fn test_optimal_latency_for_critical() {
    use std::time::Instant;

    let (mut router, factory) = build_event_router();

    // Pre-fill with low priority
    for i in 0..100 {
        factory
            .at(Priority::IDLE)
            .send(test_event(&format!("idle-{}", i)))
            .await
            .unwrap();
    }

    let start = Instant::now();
    factory
        .at(Priority::CRITICAL)
        .send(test_event("critical-timed"))
        .await
        .unwrap();

    let event = router.recv().await.unwrap();
    let latency = start.elapsed();

    assert_eq!(event.event.id, "critical-timed");
    // Should be nearly instant (under 50ms)
    assert!(
        latency.as_millis() < 50,
        "Critical latency too high: {:?}",
        latency
    );
}
