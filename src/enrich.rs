use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tracing::warn;

use crate::types::{EnrichedEvent, EventMeta};

/// Enrich raw log event with timestamp and optional JSON parsing.
pub fn enrich_event(
    raw: crate::types::LogEvent,
    target_index: Option<String>,
) -> Option<EnrichedEvent> {
    let ts_dt = DateTime::<Utc>::from_timestamp_millis(raw.timestamp_ms)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_millis(0).unwrap());
    let ts = ts_dt.to_rfc3339();
    let idx = target_index.unwrap_or_else(|| {
        let date = ts_dt.format("%Y.%m.%d").to_string();
        format!("logs-{}", date)
    });

    // Normalize message
    let mut message_str = raw.message.replace('\r', "");
    message_str = message_str.trim().to_string();
    if message_str.is_empty() {
        return None;
    }

    // Keep original as string for ES text mapping
    let message = Value::String(message_str.clone());

    // Best-effort parse and normalize into JSON; keep alongside original string
    let (parsed, mut tags) = match try_parse_and_normalize(&message_str) {
        Ok(Some(val)) => (Some(val), vec!["json_parsed".into()]),
        Ok(None) => (None, vec!["not_json_message".into()]),
        Err(err) => {
            warn!("json parse/normalize failure: {err}");
            (None, vec!["json_failure".into()])
        }
    };

    // Always include sync tags
    tags.push("sync".into());
    tags.push("synced".into());

    Some(EnrichedEvent {
        timestamp: ts,
        event: EventMeta { id: raw.id },
        message,
        parsed,
        target_index: Some(idx),
        tags,
    })
}

fn try_parse_and_normalize(s: &str) -> Result<Option<Value>> {
    if s.is_empty() {
        return Ok(None);
    }
    // Quick check for likely JSON object/array
    let trimmed = s.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Ok(None);
    }
    let val: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(e) => return Err(e.into()),
    };
    let mut val = val;
    normalize_value(&mut val, 0);
    Ok(Some(val))
}

fn normalize_value(v: &mut Value, depth: usize) {
    const MAX_DEPTH: usize = 50;
    const INT_MAX: i64 = 2_147_483_647;
    if depth > MAX_DEPTH {
        return;
    }
    match v {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                if let Some(mut child) = map.remove(&k) {
                    if child.is_null() {
                        continue;
                    }
                    let clean_key = sanitize_key(&k);
                    normalize_value(&mut child, depth + 1);
                    map.insert(clean_key, child);
                }
            }
        }
        Value::Array(arr) => {
            arr.retain(|x| !x.is_null());
            for child in arr.iter_mut() {
                normalize_value(child, depth + 1);
            }
        }
        Value::Number(num) => {
            if let Some(f) = num.as_f64() {
                *v = Value::String(f.to_string());
            } else if let Some(i) = num.as_i64() {
                if i.abs() > INT_MAX {
                    *v = Value::String(i.to_string());
                }
            }
        }
        _ => {}
    }
}

fn sanitize_key(k: &str) -> String {
    let mut out = String::with_capacity(k.len());
    for ch in k.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    out
}
