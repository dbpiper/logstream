use std::sync::Arc;

use chrono::{NaiveDate, NaiveTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteReason {
    TimestampEligible,
    NameDateEligible,
    EmergencyOldest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteCandidate {
    pub index: String,
    pub sort_key_ms: i64,
    pub watermark_ms: i64,
    pub stable_alias: Arc<str>,
    pub store_bytes: Option<u64>,
    pub reason: DeleteReason,
}

pub fn compute_delete_candidates(
    stable_alias: Arc<str>,
    backing_indices: &[String],
    keep_cutoff_ms: i64,
    max_timestamp_ms: impl Fn(&str) -> Option<i64>,
    enable_emergency_delete: bool,
) -> Vec<DeleteCandidate> {
    compute_delete_candidates_with_store_bytes(
        stable_alias,
        backing_indices,
        keep_cutoff_ms,
        max_timestamp_ms,
        |_index| None,
        enable_emergency_delete,
    )
}

pub fn compute_delete_candidates_with_store_bytes(
    stable_alias: Arc<str>,
    backing_indices: &[String],
    keep_cutoff_ms: i64,
    max_timestamp_ms: impl Fn(&str) -> Option<i64>,
    store_bytes: impl Fn(&str) -> Option<u64>,
    enable_emergency_delete: bool,
) -> Vec<DeleteCandidate> {
    let Some((read_indices, _write_index)) = split_write_index(backing_indices) else {
        return Vec::new();
    };

    let candidates = read_indices
        .iter()
        .filter_map(|index| {
            let max_ts_ms = max_timestamp_ms(index.as_str()).filter(|v| *v > 0);
            let date_start_ms = parse_backing_index_date_start_ms(index.as_str());
            let eligible =
                is_eligible_for_deletion(keep_cutoff_ms, max_ts_ms, date_start_ms).is_some();
            eligible.then(|| {
                build_candidate(
                    stable_alias.clone(),
                    index,
                    keep_cutoff_ms,
                    max_ts_ms,
                    date_start_ms,
                    store_bytes(index.as_str()),
                )
            })
        })
        .collect::<Vec<_>>();

    if !candidates.is_empty() {
        return sort_by_delete_order(candidates);
    }

    if !enable_emergency_delete {
        return Vec::new();
    }

    read_indices
        .first()
        .map(|oldest| {
            let date_start_ms = parse_backing_index_date_start_ms(oldest.as_str());
            let sort_key_ms = date_start_ms.unwrap_or(i64::MIN);
            DeleteCandidate {
                index: oldest.clone(),
                sort_key_ms,
                watermark_ms: keep_cutoff_ms,
                stable_alias,
                store_bytes: store_bytes(oldest.as_str()),
                reason: DeleteReason::EmergencyOldest,
            }
        })
        .into_iter()
        .collect()
}

fn split_write_index(backing_indices: &[String]) -> Option<(&[String], &String)> {
    (backing_indices.len() > 1).then(|| {
        let (read, write) = backing_indices.split_at(backing_indices.len().saturating_sub(1));
        (read, write.first().expect("len>1"))
    })
}

fn is_eligible_for_deletion(
    keep_cutoff_ms: i64,
    max_ts_ms: Option<i64>,
    date_start_ms: Option<i64>,
) -> Option<DeleteReason> {
    match max_ts_ms {
        Some(ts) => (ts < keep_cutoff_ms).then_some(DeleteReason::TimestampEligible),
        None => date_start_ms
            .is_some_and(|d| d < keep_cutoff_ms)
            .then_some(DeleteReason::NameDateEligible),
    }
}

fn build_candidate(
    stable_alias: Arc<str>,
    index: &str,
    keep_cutoff_ms: i64,
    max_ts_ms: Option<i64>,
    date_start_ms: Option<i64>,
    store_bytes: Option<u64>,
) -> DeleteCandidate {
    let sort_key_ms = max_ts_ms.unwrap_or_else(|| date_start_ms.unwrap_or(i64::MAX));
    let watermark_ms = max_ts_ms.unwrap_or_else(|| {
        date_start_ms
            .unwrap_or(keep_cutoff_ms)
            .saturating_add(86_400_000)
            .saturating_sub(1)
    });
    let reason = is_eligible_for_deletion(keep_cutoff_ms, max_ts_ms, date_start_ms)
        .unwrap_or(DeleteReason::NameDateEligible);
    DeleteCandidate {
        index: index.to_string(),
        sort_key_ms,
        watermark_ms,
        stable_alias,
        store_bytes,
        reason,
    }
}

fn sort_by_delete_order(mut candidates: Vec<DeleteCandidate>) -> Vec<DeleteCandidate> {
    candidates.sort_by_key(|c| c.sort_key_ms);
    candidates
}

pub fn parse_backing_index_date_start_ms(index: &str) -> Option<i64> {
    let mut parts = index.rsplitn(3, '-');
    let _seq = parts.next()?;
    let date_str = parts.next()?;
    let _prefix = parts.next()?;

    let date = NaiveDate::parse_from_str(date_str, "%Y.%m.%d").ok()?;
    let start = date.and_time(NaiveTime::from_hms_opt(0, 0, 0)?).and_utc();
    Some(start.timestamp_millis())
}
