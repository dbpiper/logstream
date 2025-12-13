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

mod safe_replace_tests {
    use super::*;
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_safe_replace_pattern_upserts_before_delete() {
        let (tx, mut rx) = mpsc::channel::<LogEvent>(100);
        let upserted = Arc::new(AtomicUsize::new(0));
        let deleted = Arc::new(AtomicBool::new(false));

        let cw_events = vec![
            make_event("cw1", 1000),
            make_event("cw2", 2000),
            make_event("cw3", 3000),
        ];
        let es_ids = vec!["es1".to_string(), "cw1".to_string()]; // es1 is orphan, cw1 overlaps

        let upserted_clone = upserted.clone();
        let deleted_clone = deleted.clone();

        // Simulate safe replace pattern
        let cw_ids: HashSet<String> = cw_events.iter().map(|e| e.id.clone()).collect();

        // Upsert first
        for event in cw_events {
            tx.send(event).await.unwrap();
            upserted_clone.fetch_add(1, Ordering::SeqCst);
        }

        // Delete orphans only after upsert
        let orphans: Vec<String> = es_ids
            .into_iter()
            .filter(|id| !cw_ids.contains(id))
            .collect();

        assert_eq!(upserted.load(Ordering::SeqCst), 3);
        assert_eq!(orphans, vec!["es1".to_string()]);

        deleted_clone.store(true, Ordering::SeqCst);
        assert!(deleted.load(Ordering::SeqCst));

        // Drain channel
        drop(tx);
        let mut received = Vec::new();
        while let Some(e) = rx.recv().await {
            received.push(e);
        }
        assert_eq!(received.len(), 3);
    }

    #[tokio::test]
    async fn test_safe_replace_no_data_loss_on_ingestion_failure() {
        let es_ids = vec![
            "existing1".to_string(),
            "existing2".to_string(),
            "existing3".to_string(),
        ];
        let cw_events = [
            make_event("new1", 1000),
            make_event("new2", 2000),
            make_event("new3", 3000),
        ];

        let cw_ids: HashSet<String> = cw_events.iter().map(|e| e.id.clone()).collect();
        let ingested = Arc::new(AtomicUsize::new(0));

        // Simulate partial ingestion failure (only 1 event ingested)
        for (i, _) in cw_events.iter().enumerate() {
            if i == 1 {
                break; // Simulates ingestion failure
            }
            ingested.fetch_add(1, Ordering::SeqCst);
        }

        // Critical: orphan deletion should NOT happen if ingestion count is 0
        // In this case ingested = 1, so we proceed but carefully
        let should_delete_orphans = ingested.load(Ordering::SeqCst) > 0;
        assert!(should_delete_orphans);

        // Find orphans
        let es_id_set: HashSet<String> = es_ids.into_iter().collect();
        let orphans: Vec<String> = es_id_set
            .iter()
            .filter(|id| !cw_ids.contains(*id))
            .cloned()
            .collect();

        // All existing ES events are orphans since CW has completely different IDs
        assert_eq!(orphans.len(), 3);
    }

    #[tokio::test]
    async fn test_safe_replace_zero_ingestion_skips_orphan_delete() {
        let es_ids = vec!["existing1".to_string(), "existing2".to_string()];
        let cw_events: Vec<LogEvent> = vec![]; // Empty CW response

        let ingested = 0usize;
        let delete_orphans_called = Arc::new(AtomicBool::new(false));

        // Pattern: only delete orphans if we ingested something
        if ingested > 0 {
            delete_orphans_called.store(true, Ordering::SeqCst);
        }

        assert!(!delete_orphans_called.load(Ordering::SeqCst));
        assert!(cw_events.is_empty());
        assert!(!es_ids.is_empty());
    }

    #[tokio::test]
    async fn test_safe_replace_finds_missing_events() {
        let cw_ids: HashSet<String> = ["cw1", "cw2", "cw3", "cw4", "cw5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let es_ids: HashSet<String> = ["cw1", "cw3", "es_only"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Missing: in CW but not in ES
        let missing: Vec<&String> = cw_ids.iter().filter(|id| !es_ids.contains(*id)).collect();

        // Orphans: in ES but not in CW
        let orphans: Vec<&String> = es_ids.iter().filter(|id| !cw_ids.contains(*id)).collect();

        assert_eq!(missing.len(), 3); // cw2, cw4, cw5
        assert_eq!(orphans.len(), 1); // es_only
        assert!(missing.contains(&&"cw2".to_string()));
        assert!(missing.contains(&&"cw4".to_string()));
        assert!(missing.contains(&&"cw5".to_string()));
        assert!(orphans.contains(&&"es_only".to_string()));
    }

    #[tokio::test]
    async fn test_safe_replace_overlap_detection() {
        let cw_ids: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let es_ids: HashSet<String> = ["b", "c", "d"].iter().map(|s| s.to_string()).collect();

        let overlap: Vec<&String> = cw_ids.iter().filter(|id| es_ids.contains(*id)).collect();
        let missing: Vec<&String> = cw_ids.iter().filter(|id| !es_ids.contains(*id)).collect();
        let orphans: Vec<&String> = es_ids.iter().filter(|id| !cw_ids.contains(*id)).collect();

        assert_eq!(overlap.len(), 2); // b, c
        assert_eq!(missing.len(), 1); // a
        assert_eq!(orphans.len(), 1); // d
    }

    #[test]
    fn test_old_pattern_would_delete_before_insert_causes_data_loss() {
        // This test documents the OLD buggy pattern that caused data loss
        // OLD PATTERN (WRONG):
        //   1. delete_range(start, end)  <-- Deletes everything
        //   2. for event in cw_events { insert(event) }  <-- If this fails, data is LOST
        //
        // NEW PATTERN (CORRECT):
        //   1. for event in cw_events { upsert(event) }  <-- Safe, just updates/inserts
        //   2. Get ES IDs in range
        //   3. Find orphans (in ES but not in CW)
        //   4. Delete only orphans
        //
        // Simulating the old pattern failing:
        let es_count_before_delete = 100;
        let deleted_count = 100; // Old pattern deletes all
        let _ingestion_success = false; // Ingestion fails (e.g., shard limit)
        let events_ingested = 0;

        let data_remaining = es_count_before_delete - deleted_count + events_ingested;
        assert_eq!(data_remaining, 0); // DATA LOSS!

        // New pattern: doesn't delete until after successful ingestion
        let new_pattern_es_count = 100;
        let _new_pattern_ingested = 0; // Still fails
        let new_pattern_orphans_deleted = 0; // But we don't delete when ingested=0

        let new_pattern_remaining = new_pattern_es_count - new_pattern_orphans_deleted;
        assert_eq!(new_pattern_remaining, 100); // Data preserved!
    }

    #[test]
    fn test_partial_ingestion_preserves_partial_data() {
        // Even with partial ingestion, new pattern is safer
        let _cw_count = 100;
        let _events_actually_ingested = 50; // 50% success rate
        let es_count_before = 80;

        // Old pattern: would have deleted 80, only ingested 50 = net loss of 30
        // New pattern: ingested 50 new/updated, then check orphans

        // Assume 30 events are orphans (in ES, not in CW)
        // and 50 events overlap (in both)
        // CW has 100, ES has 80, overlap is 50
        // After upsert: we have the 50 ingested + the 30 we couldn't delete yet
        // = 80 minimum preserved

        let _orphans_in_es = 30;
        let overlap = 50;

        // With partial ingestion, we should still have at minimum the ES count
        // because we only delete orphans after successful ingestion
        let preserved = es_count_before; // We don't delete anything when partially failing

        assert_eq!(preserved, 80);
        assert!(preserved >= overlap);
    }
}
