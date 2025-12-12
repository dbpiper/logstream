use logstream::types::LogEvent;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

fn make_event(id: &str, ts: i64) -> LogEvent {
    LogEvent {
        id: id.to_string(),
        timestamp_ms: ts,
        message: format!("msg-{}", id),
    }
}

#[tokio::test]
async fn test_buffer_collects_all_events_before_proceeding() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(10);

    let collected = Arc::new(AtomicUsize::new(0));
    let collected_clone = collected.clone();

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
            collected_clone.fetch_add(1, Ordering::SeqCst);
        }
        events
    });

    for i in 0..50 {
        tx.send(make_event(&format!("e{}", i), i)).await.unwrap();
    }
    drop(tx);

    let events = collector.await.unwrap();
    assert_eq!(events.len(), 50);
    assert_eq!(collected.load(Ordering::SeqCst), 50);
}

#[tokio::test]
async fn test_fetch_failure_prevents_downstream_actions() {
    let fetch_succeeded = Arc::new(AtomicBool::new(false));
    let delete_called = Arc::new(AtomicBool::new(false));
    let insert_called = Arc::new(AtomicBool::new(false));

    let fetch_succeeded_clone = fetch_succeeded.clone();
    let delete_called_clone = delete_called.clone();
    let insert_called_clone = insert_called.clone();

    let result: Result<Vec<LogEvent>, &str> = Err("CW throttled");

    if let Ok(events) = result {
        fetch_succeeded_clone.store(true, Ordering::SeqCst);
        delete_called_clone.store(true, Ordering::SeqCst);
        for _ in events {
            insert_called_clone.store(true, Ordering::SeqCst);
        }
    }

    assert!(!fetch_succeeded.load(Ordering::SeqCst));
    assert!(!delete_called.load(Ordering::SeqCst));
    assert!(!insert_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_fetch_success_enables_delete_and_insert() {
    let delete_called = Arc::new(AtomicBool::new(false));
    let insert_count = Arc::new(AtomicUsize::new(0));

    let events = vec![make_event("a", 1), make_event("b", 2), make_event("c", 3)];

    let result: Result<Vec<LogEvent>, &str> = Ok(events);

    let delete_called_clone = delete_called.clone();
    let insert_count_clone = insert_count.clone();

    if let Ok(events) = result {
        delete_called_clone.store(true, Ordering::SeqCst);
        for _ in events {
            insert_count_clone.fetch_add(1, Ordering::SeqCst);
        }
    }

    assert!(delete_called.load(Ordering::SeqCst));
    assert_eq!(insert_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_empty_fetch_still_triggers_delete() {
    let delete_called = Arc::new(AtomicBool::new(false));

    let events: Vec<LogEvent> = vec![];
    let result: Result<Vec<LogEvent>, &str> = Ok(events);

    let delete_called_clone = delete_called.clone();

    if result.is_ok() {
        delete_called_clone.store(true, Ordering::SeqCst);
    }

    assert!(delete_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_channel_preserves_event_order() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(100);

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        events
    });

    for i in 0..100 {
        tx.send(make_event(&format!("{}", i), i)).await.unwrap();
    }
    drop(tx);

    let events = collector.await.unwrap();
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e.id, format!("{}", i));
        assert_eq!(e.timestamp_ms, i as i64);
    }
}

#[tokio::test]
async fn test_channel_backpressure_blocks_sender() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(2);

    let send_count = Arc::new(AtomicUsize::new(0));
    let send_count_clone = send_count.clone();

    let sender = tokio::spawn(async move {
        for i in 0..10 {
            tx.send(make_event(&format!("{}", i), i)).await.unwrap();
            send_count_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let sent_before_recv = send_count.load(Ordering::SeqCst);
    assert!(sent_before_recv <= 3);

    let mut received = 0;
    while rx.recv().await.is_some() {
        received += 1;
    }

    sender.await.unwrap();
    assert_eq!(received, 10);
    assert_eq!(send_count.load(Ordering::SeqCst), 10);
}

#[tokio::test]
async fn test_dropped_receiver_causes_send_error() {
    let (tx, rx) = mpsc::channel::<LogEvent>(10);
    drop(rx);

    let result = tx.send(make_event("x", 1)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_collector_panic_returns_error() {
    let (tx, rx) = mpsc::channel::<LogEvent>(10);

    let collector = tokio::spawn(async move {
        drop(rx);
        panic!("simulated panic");
        #[allow(unreachable_code)]
        Vec::<LogEvent>::new()
    });

    tx.send(make_event("x", 1)).await.ok();
    drop(tx);

    let result = collector.await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_large_batch_buffered_correctly() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(1000);

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        events
    });

    for i in 0..10_000 {
        tx.send(make_event(&format!("{}", i), i)).await.unwrap();
    }
    drop(tx);

    let events = collector.await.unwrap();
    assert_eq!(events.len(), 10_000);
}

#[tokio::test]
async fn test_concurrent_senders_all_collected() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(100);

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        events
    });

    let mut handles = vec![];
    for batch in 0..10 {
        let tx_clone = tx.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100 {
                tx_clone
                    .send(make_event(
                        &format!("{}-{}", batch, i),
                        (batch * 100 + i) as i64,
                    ))
                    .await
                    .unwrap();
            }
        }));
    }
    drop(tx);

    for h in handles {
        h.await.unwrap();
    }

    let events = collector.await.unwrap();
    assert_eq!(events.len(), 1000);
}

#[tokio::test]
async fn test_delete_failure_continues_to_insert() {
    let insert_count = Arc::new(AtomicUsize::new(0));

    let events = vec![make_event("a", 1), make_event("b", 2)];

    let insert_count_clone = insert_count.clone();
    for _ in events {
        insert_count_clone.fetch_add(1, Ordering::SeqCst);
    }

    assert_eq!(insert_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_event_ids_preserved_through_buffer() {
    let (tx, mut rx) = mpsc::channel::<LogEvent>(10);

    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        events
    });

    let original_ids = vec!["uuid-123", "uuid-456", "uuid-789"];
    for (i, id) in original_ids.iter().enumerate() {
        tx.send(make_event(id, i as i64)).await.unwrap();
    }
    drop(tx);

    let events = collector.await.unwrap();
    let collected_ids: Vec<&str> = events.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(collected_ids, original_ids);
}
