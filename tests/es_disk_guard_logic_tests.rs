use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use logstream::es_disk_guard_logic::{compute_delete_candidates, DeleteCandidate, DeleteReason};

fn mk_index(stream: &str, date: &str, seq: &str) -> String {
    format!(".ds-{stream}-{date}-{seq}")
}

#[test]
fn compute_delete_candidates_excludes_write_index() {
    let stable_alias = Arc::<str>::from("cloudwatch-ecs-svc");
    let stream = "cloudwatch-ecs-svc-v1";
    let backing_indices = vec![
        mk_index(stream, "2025.12.25", "000001"),
        mk_index(stream, "2025.12.26", "000002"),
        mk_index(stream, "2025.12.27", "000003"), // write index (last)
    ];

    let max_ts_by_index = BTreeMap::from([(backing_indices[0].clone(), 0)]);
    let keep_cutoff_ms = i64::MAX; // everything is "old" according to max_ts

    let candidates = compute_delete_candidates(
        stable_alias,
        &backing_indices,
        keep_cutoff_ms,
        |index| max_ts_by_index.get(index).copied(),
        true,
    );

    let indices = candidates
        .iter()
        .map(|c| c.index.as_str())
        .collect::<Vec<_>>();
    assert!(!indices.contains(&backing_indices[2].as_str()));
}

#[test]
fn compute_delete_candidates_uses_max_timestamp_when_available() {
    let stable_alias = Arc::<str>::from("cloudwatch-ecs-svc");
    let stream = "cloudwatch-ecs-svc-v1";
    let backing_indices = vec![
        mk_index(stream, "2025.12.25", "000001"),
        mk_index(stream, "2025.12.26", "000002"),
        mk_index(stream, "2025.12.27", "000003"), // write index
    ];

    let max_ts_by_index = BTreeMap::from([
        (backing_indices[0].clone(), 900),
        (backing_indices[1].clone(), 1500),
    ]);
    let keep_cutoff_ms = 1000;

    let candidates = compute_delete_candidates(
        stable_alias,
        &backing_indices,
        keep_cutoff_ms,
        |index| max_ts_by_index.get(index).copied(),
        true,
    );

    assert_eq!(
        candidates,
        vec![DeleteCandidate {
            index: backing_indices[0].clone(),
            sort_key_ms: 900,
            watermark_ms: 900,
            stable_alias: Arc::<str>::from("cloudwatch-ecs-svc"),
            store_bytes: None,
            reason: DeleteReason::TimestampEligible,
        }]
    );
}

#[test]
fn compute_delete_candidates_emergency_deletes_oldest_when_none_eligible() {
    let stable_alias = Arc::<str>::from("cloudwatch-ecs-svc");
    let stream = "cloudwatch-ecs-svc-v1";
    let backing_indices = vec![
        mk_index(stream, "2025.12.25", "000001"),
        mk_index(stream, "2025.12.26", "000002"),
        mk_index(stream, "2025.12.27", "000003"), // write index
    ];

    let keep_cutoff_ms = 0; // date parsing likely yields > 0, so nothing is "old"

    let candidates = compute_delete_candidates(
        stable_alias,
        &backing_indices,
        keep_cutoff_ms,
        |_index| None,
        true,
    );

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].index, backing_indices[0]);
    assert_eq!(candidates[0].reason, DeleteReason::EmergencyOldest);
}

#[test]
fn compute_delete_candidates_is_linear_in_number_of_indices() {
    let stable_alias = Arc::<str>::from("cloudwatch-ecs-svc");
    let stream = "cloudwatch-ecs-svc-v1";

    let mut backing_indices = (0..10_000)
        .map(|n| mk_index(stream, "2025.12.25", &format!("{:0>6}", n)))
        .collect::<Vec<_>>();
    backing_indices.push(mk_index(stream, "2025.12.26", "999999")); // write index

    let lookups = AtomicUsize::new(0);
    let _ = compute_delete_candidates(
        stable_alias,
        &backing_indices,
        i64::MIN,
        |_index| {
            lookups.fetch_add(1, Ordering::Relaxed);
            None
        },
        false,
    );

    assert!(lookups.load(Ordering::Relaxed) <= backing_indices.len());
}

#[test]
fn compute_delete_candidates_emergency_still_progresses_when_date_unparseable() {
    let stable_alias = Arc::<str>::from("cloudwatch-ecs-svc");
    let backing_indices = vec![
        ".ds-cloudwatch-ecs-svc-v1-not-a-date-000001".to_string(),
        ".ds-cloudwatch-ecs-svc-v1-not-a-date-000002".to_string(),
    ];

    let candidates =
        compute_delete_candidates(stable_alias, &backing_indices, 0, |_index| None, true);

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].index, backing_indices[0]);
    assert_eq!(candidates[0].reason, DeleteReason::EmergencyOldest);
}
