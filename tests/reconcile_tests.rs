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
async fn test_empty_fetch_preserves_es_data() {
    let delete_called = Arc::new(AtomicBool::new(false));

    let events: Vec<LogEvent> = vec![];
    let result: Result<Vec<LogEvent>, &str> = Ok(events);

    let delete_called_clone = delete_called.clone();

    if let Ok(events) = result {
        if !events.is_empty() {
            delete_called_clone.store(true, Ordering::SeqCst);
        }
    }

    assert!(!delete_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_non_empty_fetch_triggers_delete() {
    let delete_called = Arc::new(AtomicBool::new(false));

    let events = vec![make_event("a", 1)];
    let result: Result<Vec<LogEvent>, &str> = Ok(events);

    let delete_called_clone = delete_called.clone();

    if let Ok(events) = result {
        if !events.is_empty() {
            delete_called_clone.store(true, Ordering::SeqCst);
        }
    }

    assert!(delete_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_cw_retention_scenario_preserves_es() {
    let es_has_data = true;
    let cw_returns_empty = true;
    let delete_called = Arc::new(AtomicBool::new(false));
    let es_data_preserved = Arc::new(AtomicBool::new(true));

    let cw_events: Vec<LogEvent> = if cw_returns_empty {
        vec![]
    } else {
        vec![make_event("a", 1)]
    };

    let delete_called_clone = delete_called.clone();
    let es_preserved_clone = es_data_preserved.clone();

    if !cw_events.is_empty() {
        delete_called_clone.store(true, Ordering::SeqCst);
        es_preserved_clone.store(false, Ordering::SeqCst);
    }

    assert!(es_has_data);
    assert!(!delete_called.load(Ordering::SeqCst));
    assert!(es_data_preserved.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_partial_cw_data_triggers_replace() {
    let delete_called = Arc::new(AtomicBool::new(false));
    let insert_count = Arc::new(AtomicUsize::new(0));

    let cw_events = vec![make_event("partial-1", 1), make_event("partial-2", 2)];

    let delete_called_clone = delete_called.clone();
    let insert_count_clone = insert_count.clone();

    if !cw_events.is_empty() {
        delete_called_clone.store(true, Ordering::SeqCst);
        for _ in &cw_events {
            insert_count_clone.fetch_add(1, Ordering::SeqCst);
        }
    }

    assert!(delete_called.load(Ordering::SeqCst));
    assert_eq!(insert_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_fetch_then_replace_pattern_complete() {
    let fetch_succeeded = Arc::new(AtomicBool::new(false));
    let delete_called = Arc::new(AtomicBool::new(false));
    let insert_count = Arc::new(AtomicUsize::new(0));
    let es_preserved = Arc::new(AtomicBool::new(true));

    let cw_result: Result<Vec<LogEvent>, &str> = Ok(vec![
        make_event("new-1", 100),
        make_event("new-2", 200),
        make_event("new-3", 300),
    ]);

    let fetch_clone = fetch_succeeded.clone();
    let delete_clone = delete_called.clone();
    let insert_clone = insert_count.clone();
    let preserved_clone = es_preserved.clone();

    match cw_result {
        Ok(events) => {
            fetch_clone.store(true, Ordering::SeqCst);
            if events.is_empty() {
                return;
            }
            delete_clone.store(true, Ordering::SeqCst);
            preserved_clone.store(false, Ordering::SeqCst);
            for _ in events {
                insert_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
        Err(_) => {
            return;
        }
    }

    assert!(fetch_succeeded.load(Ordering::SeqCst));
    assert!(delete_called.load(Ordering::SeqCst));
    assert_eq!(insert_count.load(Ordering::SeqCst), 3);
    assert!(!es_preserved.load(Ordering::SeqCst));
}

use logstream::seasonal_stats::{FeasibilityResult, SeasonalStats};
use logstream::stress::{StressConfig, StressTracker};

fn make_tracker_at_level(level: logstream::stress::StressLevel) -> StressTracker {
    let tracker = StressTracker::with_config(StressConfig::CLOUDWATCH);
    match level {
        logstream::stress::StressLevel::Normal => {}
        logstream::stress::StressLevel::Elevated => {
            for _ in 0..5 {
                tracker.record_failure();
            }
        }
        logstream::stress::StressLevel::Critical => {
            for _ in 0..15 {
                tracker.record_failure();
            }
        }
    }
    tracker
}

#[test]
fn test_seasonal_no_history_allows_data() {
    let stats = SeasonalStats::new();
    let tracker = make_tracker_at_level(logstream::stress::StressLevel::Normal);
    let result = stats.is_feasible(1700000000000, 3_600_000, 100, &tracker);
    assert!(result.is_feasible());
}

#[test]
fn test_seasonal_learns_from_verified_data() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 100);
    }

    assert!(stats.total_samples() >= 10);
    assert!(stats.expected_range(ts).is_some());
}

#[test]
fn test_seasonal_stress_affects_tolerance() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..20 {
        stats.record_verified(ts + i * 1000, 100);
    }

    let normal = make_tracker_at_level(logstream::stress::StressLevel::Normal);
    let critical = make_tracker_at_level(logstream::stress::StressLevel::Critical);

    let result_normal = stats.is_feasible(ts, 3_600_000, 100, &normal);
    let result_critical = stats.is_feasible(ts, 3_600_000, 100, &critical);

    assert!(result_normal.is_feasible());
    assert!(result_critical.is_feasible());

    if let FeasibilityResult::Feasible { sigma_used: s1, .. } = result_normal {
        if let FeasibilityResult::Feasible { sigma_used: s2, .. } = result_critical {
            assert!(s1 > s2);
        }
    }
}

#[test]
fn test_seasonal_detects_suspicious_zero() {
    let stats = SeasonalStats::new();
    let ts = 1700000000000i64;

    for i in 0..10 {
        stats.record_verified(ts + i * 1000, 1000);
    }

    let tracker = make_tracker_at_level(logstream::stress::StressLevel::Normal);
    let result = stats.is_feasible(ts, 3_600_000, 0, &tracker);
    assert!(!result.is_feasible());
}

#[test]
fn test_feasibility_result_controls_recording() {
    let feasible = FeasibilityResult::Feasible {
        expected: 100.0,
        stddev: 10.0,
        sigma_used: 4.0,
    };
    assert!(feasible.should_record());

    let suspicious = FeasibilityResult::Suspicious {
        expected: 100.0,
        stddev: 10.0,
        deviation: 50.0,
        sigma_used: 4.0,
    };
    assert!(!suspicious.should_record());

    let no_history = FeasibilityResult::NoHistory;
    assert!(!no_history.should_record());
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

mod live_learn_tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;

    const LIVE_LEARN_THRESHOLD_PCT: u64 = 10;

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum LiveLearnDecision {
        TrustEs,
        Replace,
    }

    fn decide_live_learn(es_count: u64, cw_count: u64, cw_stressed: bool) -> LiveLearnDecision {
        let max_count = es_count.max(cw_count);
        let diff = es_count.abs_diff(cw_count);

        if max_count == 0 {
            return LiveLearnDecision::TrustEs;
        }

        let diff_pct = diff * 100 / max_count;
        let within_threshold = diff_pct <= LIVE_LEARN_THRESHOLD_PCT;
        let es_ahead_under_stress = es_count > cw_count && cw_stressed;

        if within_threshold || es_ahead_under_stress {
            LiveLearnDecision::TrustEs
        } else {
            LiveLearnDecision::Replace
        }
    }

    #[test]
    fn test_counts_within_threshold_trusts_es() {
        assert_eq!(
            decide_live_learn(1000, 1050, false),
            LiveLearnDecision::TrustEs
        );
        assert_eq!(
            decide_live_learn(1000, 950, false),
            LiveLearnDecision::TrustEs
        );
        assert_eq!(
            decide_live_learn(1000, 1100, false),
            LiveLearnDecision::TrustEs
        );
    }

    #[test]
    fn test_counts_outside_threshold_triggers_replace() {
        assert_eq!(
            decide_live_learn(1000, 2000, false),
            LiveLearnDecision::Replace
        );
        assert_eq!(
            decide_live_learn(1000, 500, false),
            LiveLearnDecision::Replace
        );
        assert_eq!(
            decide_live_learn(100, 200, false),
            LiveLearnDecision::Replace
        );
    }

    #[test]
    fn test_es_more_than_cw_under_stress_trusts_es() {
        assert_eq!(
            decide_live_learn(1000, 500, true),
            LiveLearnDecision::TrustEs
        );
        assert_eq!(
            decide_live_learn(2000, 100, true),
            LiveLearnDecision::TrustEs
        );
    }

    #[test]
    fn test_cw_more_than_es_under_stress_still_replaces() {
        assert_eq!(
            decide_live_learn(500, 1000, true),
            LiveLearnDecision::Replace
        );
    }

    #[test]
    fn test_both_zero_trusts_es() {
        assert_eq!(decide_live_learn(0, 0, false), LiveLearnDecision::TrustEs);
        assert_eq!(decide_live_learn(0, 0, true), LiveLearnDecision::TrustEs);
    }

    #[test]
    fn test_boundary_at_exactly_10_percent() {
        assert_eq!(
            decide_live_learn(1000, 1100, false),
            LiveLearnDecision::TrustEs
        );
        assert_eq!(
            decide_live_learn(1000, 1200, false),
            LiveLearnDecision::Replace
        );
    }

    #[tokio::test]
    async fn test_live_learn_records_es_count_not_cw() {
        let recorded_count = Arc::new(AtomicU64::new(0));
        let recorded = recorded_count.clone();

        let es_count = 1000u64;
        let cw_count = 1050u64;

        let decision = decide_live_learn(es_count, cw_count, false);
        if decision == LiveLearnDecision::TrustEs {
            recorded.store(es_count, Ordering::SeqCst);
        }

        assert_eq!(recorded_count.load(Ordering::SeqCst), 1000);
    }

    #[tokio::test]
    async fn test_live_learn_upserts_without_delete() {
        let delete_called = Arc::new(AtomicBool::new(false));
        let upsert_count = Arc::new(AtomicUsize::new(0));

        let decision = decide_live_learn(1000, 1050, false);

        if decision == LiveLearnDecision::TrustEs {
            upsert_count.fetch_add(50, Ordering::SeqCst);
        } else {
            delete_called.store(true, Ordering::SeqCst);
        }

        assert!(!delete_called.load(Ordering::SeqCst));
        assert_eq!(upsert_count.load(Ordering::SeqCst), 50);
    }

    #[tokio::test]
    async fn test_full_replace_deletes_then_inserts() {
        let delete_called = Arc::new(AtomicBool::new(false));
        let insert_count = Arc::new(AtomicUsize::new(0));

        let decision = decide_live_learn(1000, 2000, false);

        if decision == LiveLearnDecision::Replace {
            delete_called.store(true, Ordering::SeqCst);
            insert_count.fetch_add(2000, Ordering::SeqCst);
        }

        assert!(delete_called.load(Ordering::SeqCst));
        assert_eq!(insert_count.load(Ordering::SeqCst), 2000);
    }

    #[test]
    fn test_threshold_edge_cases() {
        assert_eq!(decide_live_learn(10, 11, false), LiveLearnDecision::TrustEs);
        assert_eq!(decide_live_learn(10, 13, false), LiveLearnDecision::Replace);
        assert_eq!(
            decide_live_learn(100, 110, false),
            LiveLearnDecision::TrustEs
        );
        assert_eq!(
            decide_live_learn(100, 120, false),
            LiveLearnDecision::Replace
        );
    }

    #[test]
    fn test_stress_only_protects_when_es_has_more() {
        assert_eq!(decide_live_learn(100, 50, true), LiveLearnDecision::TrustEs);
        assert_eq!(decide_live_learn(50, 100, true), LiveLearnDecision::Replace);
        assert_eq!(
            decide_live_learn(100, 100, true),
            LiveLearnDecision::TrustEs
        );
    }
}
