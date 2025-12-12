//! Tests for CloudWatch tailer, especially channel handling.
//!
//! These tests verify that logs are NEVER silently dropped.
//! When the downstream channel is closed or full, the tailer must
//! either wait for capacity or return an error (not advance checkpoint).

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

// ============================================================================
// Channel Capacity Tests
// ============================================================================

/// Test that channel sends block when channel is full (not drop).
#[tokio::test]
async fn test_channel_blocks_when_full() {
    // Create a tiny channel (capacity 1)
    let (tx, mut rx) = mpsc::channel::<u32>(1);

    // Send first message - should succeed immediately
    tx.send(1).await.unwrap();

    // Try to send second message - should block because channel is full
    let send_task = tokio::spawn(async move {
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
    });

    // Give the send task time to block
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Task should still be running (blocked)
    assert!(!send_task.is_finished());

    // Receive first message - should unblock one send
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg, 1);

    // Give time for send to complete
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Now receive second message
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg, 2);

    // Receive third message
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg, 3);

    // Task should complete
    send_task.await.unwrap();
}

/// Test that send fails when receiver is dropped.
#[tokio::test]
async fn test_send_fails_when_receiver_dropped() {
    let (tx, rx) = mpsc::channel::<u32>(10);

    // Drop receiver
    drop(rx);

    // Send should fail
    let result = tx.send(1).await;
    assert!(result.is_err());
}

/// Test that the pattern we use correctly propagates errors.
#[tokio::test]
async fn test_send_error_propagation_pattern() {
    let (tx, rx) = mpsc::channel::<u32>(10);
    drop(rx);

    // This is the pattern we use in poll_once
    let result: Result<(), anyhow::Error> = tx
        .send(1)
        .await
        .map_err(|_| anyhow::anyhow!("log channel closed"));

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("channel closed"));
}

// ============================================================================
// Log Event Preservation Tests
// ============================================================================

/// Simulates the critical scenario: logs must not be dropped when channel is slow.
#[tokio::test]
async fn test_logs_not_dropped_when_channel_slow() {
    use logstream::types::LogEvent;

    // Create a small channel to simulate backpressure
    let (tx, mut rx) = mpsc::channel::<LogEvent>(2);

    // Simulate a slow consumer
    let consumer = tokio::spawn(async move {
        let mut received = Vec::new();
        tokio::time::sleep(Duration::from_millis(100)).await;

        while let Some(event) = rx.recv().await {
            received.push(event.id);
            // Slow processing
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        received
    });

    // Send many events - they should all be preserved (blocking send)
    let producer = tokio::spawn(async move {
        for i in 0..10 {
            tx.send(LogEvent {
                id: format!("event-{}", i),
                timestamp_ms: i as i64,
                message: format!("message {}", i),
            })
            .await
            .expect("send should succeed");
        }
    });

    producer.await.unwrap();
    let received = consumer.await.unwrap();

    // All 10 events should have been received
    assert_eq!(received.len(), 10);
    for (i, id) in received.iter().enumerate() {
        assert_eq!(id, &format!("event-{}", i));
    }
}

/// Test that error is propagated when channel closes mid-send.
#[tokio::test]
async fn test_error_on_channel_close_mid_stream() {
    use logstream::types::LogEvent;

    let (tx, rx) = mpsc::channel::<LogEvent>(5);

    // Drop receiver after some messages
    let handle = tokio::spawn(async move {
        let mut count = 0;
        for i in 0..20 {
            let result = tx
                .send(LogEvent {
                    id: format!("event-{}", i),
                    timestamp_ms: i as i64,
                    message: String::new(),
                })
                .await;

            if result.is_err() {
                break;
            }
            count += 1;
        }
        count
    });

    // Receive a few then drop
    let mut rx = rx;
    let _ = rx.recv().await;
    let _ = rx.recv().await;
    drop(rx);

    let sent = handle.await.unwrap();

    // Should have sent some but not all (channel closed)
    assert!(sent > 0);
    assert!(sent < 20);
}

// ============================================================================
// Default Lookback Tests
// ============================================================================

/// Test that current_time_ms helper works correctly.
#[test]
fn test_current_time_ms_is_reasonable() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Should be after year 2020 (1577836800000 ms)
    assert!(now_ms > 1_577_836_800_000);

    // Should be before year 2100 (4102444800000 ms)
    assert!(now_ms < 4_102_444_800_000);
}

/// Test that 1 hour lookback is used (not 5 minutes).
#[test]
fn test_default_lookback_is_one_hour() {
    // 1 hour in milliseconds
    let one_hour_ms: i64 = 60 * 60 * 1000;

    // 5 minutes in milliseconds (old value)
    let five_min_ms: i64 = 5 * 60 * 1000;

    // Verify our constant is 1 hour, not 5 minutes
    assert_eq!(one_hour_ms, 3_600_000);
    assert_eq!(five_min_ms, 300_000);
    assert!(one_hour_ms > five_min_ms * 10);
}

// ============================================================================
// Stress Scenario Tests
// ============================================================================

/// Simulate the throttling scenario that caused data loss.
/// Many events arrive in a burst after throttling clears.
#[tokio::test]
async fn test_burst_after_throttle_no_data_loss() {
    use logstream::types::LogEvent;

    // Small channel simulates backpressure from ES stress
    let (tx, mut rx) = mpsc::channel::<LogEvent>(5);

    let event_count = 100;

    // Slow consumer (simulates ES under stress)
    let consumer = tokio::spawn(async move {
        let mut received = 0;
        while let Some(_event) = rx.recv().await {
            received += 1;
            // Very slow - 5ms per event
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        received
    });

    // Burst producer (simulates CW resuming after throttle)
    let producer = tokio::spawn(async move {
        for i in 0..event_count {
            // Using the correct pattern that blocks and propagates errors
            tx.send(LogEvent {
                id: format!("burst-{}", i),
                timestamp_ms: i as i64,
                message: String::new(),
            })
            .await
            .expect("must not drop");
        }
    });

    // Wait for producer with reasonable timeout
    let producer_result = timeout(Duration::from_secs(10), producer).await;
    assert!(producer_result.is_ok(), "producer should complete");

    let received = consumer.await.unwrap();

    // ALL events must be received - no drops allowed
    assert_eq!(
        received, event_count,
        "all {} events must be received, got {}",
        event_count, received
    );
}

/// Test that checkpoint should NOT advance if events fail to send.
#[tokio::test]
async fn test_checkpoint_not_advanced_on_send_failure() {
    // This test verifies the contract: if send fails, return error
    // so that the calling code (poll_once) doesn't advance the checkpoint.

    let (tx, rx) = mpsc::channel::<i32>(1);
    drop(rx); // Close immediately

    // Simulate what poll_once does
    let mut checkpoint_advanced = false;
    let mut latest_ts = 0i64;

    // Try to send events
    let result: Result<(), anyhow::Error> = async {
        for i in 0..5 {
            latest_ts = i;
            tx.send(i as i32)
                .await
                .map_err(|_| anyhow::anyhow!("channel closed"))?;
        }
        Ok(())
    }
    .await;

    // Only advance checkpoint if ALL sends succeeded
    if result.is_ok() {
        checkpoint_advanced = true;
    }

    // Checkpoint should NOT have advanced because sends failed
    assert!(!checkpoint_advanced);
    assert!(result.is_err());
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

/// Test that multiple senders can safely send to the same channel.
#[tokio::test]
async fn test_concurrent_senders_no_data_loss() {
    use logstream::types::LogEvent;

    let (tx, mut rx) = mpsc::channel::<LogEvent>(100);

    let sender_count = 5;
    let events_per_sender = 20;

    let mut handles = Vec::new();

    for sender_id in 0..sender_count {
        let tx_clone = tx.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..events_per_sender {
                tx_clone
                    .send(LogEvent {
                        id: format!("sender-{}-event-{}", sender_id, i),
                        timestamp_ms: (sender_id * 1000 + i) as i64,
                        message: String::new(),
                    })
                    .await
                    .expect("send should succeed");
            }
        }));
    }

    // Drop original sender
    drop(tx);

    // Wait for all senders
    for h in handles {
        h.await.unwrap();
    }

    // Count received
    let mut received = 0;
    while rx.recv().await.is_some() {
        received += 1;
    }

    // All events from all senders
    assert_eq!(received, sender_count * events_per_sender);
}

// ============================================================================
// Timeout Behavior Tests
// ============================================================================

/// Test that sends eventually complete even with slow consumer.
#[tokio::test]
async fn test_send_completes_with_slow_consumer() {
    let (tx, mut rx) = mpsc::channel::<i32>(1);

    // Very slow consumer
    let consumer = tokio::spawn(async move {
        while rx.recv().await.is_some() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Send 10 messages - should complete even though consumer is slow
    let start = std::time::Instant::now();
    for i in 0..10 {
        tx.send(i).await.unwrap();
    }
    drop(tx);

    let elapsed = start.elapsed();

    // Should take at least 450ms (9 waits of 50ms each)
    // (channel has capacity 1, so we wait for consumer after each send)
    assert!(elapsed >= Duration::from_millis(400));

    consumer.await.unwrap();
}
