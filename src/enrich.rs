use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use tracing::warn;

use crate::types::{EnrichedEvent, EventMeta};

const MAX_FIELD_NAME_LEN: usize = 256;
const MAX_STRING_VALUE_LEN: usize = 32000;
const MAX_ARRAY_LEN: usize = 1000;

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

    let mut message_str = raw.message.replace('\r', "");
    message_str = message_str.trim().to_string();
    if message_str.is_empty() {
        return None;
    }

    let message = Value::String(message_str.clone());

    let (parsed, mut tags) = match try_parse_and_normalize(&message_str) {
        Ok(Some(val)) => (Some(val), vec!["json_parsed".into()]),
        Ok(None) => (None, vec!["not_json_message".into()]),
        Err(err) => {
            warn!("json parse/normalize failure: {err}");
            (None, vec!["json_failure".into()])
        }
    };

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

pub fn normalize_for_es(value: &mut Value) {
    normalize_value(value, 0);
}

fn try_parse_and_normalize(s: &str) -> Result<Option<Value>> {
    if s.is_empty() {
        return Ok(None);
    }
    let trimmed = s.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Ok(None);
    }
    let mut val: Value = serde_json::from_str(trimmed)?;
    normalize_value(&mut val, 0);
    Ok(Some(val))
}

fn normalize_value(v: &mut Value, depth: usize) {
    const MAX_DEPTH: usize = 10;
    const FLATTEN_DEPTH: usize = 6;
    const INT_MAX: i64 = 2_147_483_647;

    if depth > MAX_DEPTH {
        *v = flatten_to_string(v);
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
                    if clean_key.is_empty() || clean_key.len() > MAX_FIELD_NAME_LEN {
                        continue;
                    }

                    if is_reserved_field(&clean_key) {
                        let renamed = format!("_user_{}", clean_key);
                        normalize_value(&mut child, depth + 1);
                        map.insert(renamed, child);
                    } else if has_type_conflict(&child, &clean_key) {
                        // Field pattern suggests it should be a primitive
                        let stringified = flatten_to_string(&child);
                        map.insert(clean_key, stringified);
                    } else if depth >= FLATTEN_DEPTH && matches!(child, Value::Object(_)) {
                        // Deep nesting: flatten objects to prevent mapping conflicts
                        let stringified = flatten_to_string(&child);
                        map.insert(clean_key, stringified);
                    } else {
                        normalize_value(&mut child, depth + 1);
                        map.insert(clean_key, child);
                    }
                }
            }
        }
        Value::Array(arr) => {
            arr.retain(|x| !x.is_null());
            if arr.len() > MAX_ARRAY_LEN {
                arr.truncate(MAX_ARRAY_LEN);
            }
            if !is_homogeneous_array(arr) {
                *v = flatten_array_to_strings(arr);
            } else {
                for child in arr.iter_mut() {
                    normalize_value(child, depth + 1);
                }
            }
        }
        Value::Number(num) => {
            if let Some(f) = num.as_f64() {
                if f.is_nan() || f.is_infinite() {
                    *v = Value::Null;
                } else {
                    *v = Value::String(f.to_string());
                }
            } else if let Some(i) = num.as_i64() {
                if i.abs() > INT_MAX {
                    *v = Value::String(i.to_string());
                }
            }
        }
        Value::String(s) => {
            if s.len() > MAX_STRING_VALUE_LEN {
                s.truncate(MAX_STRING_VALUE_LEN);
                s.push_str("...[truncated]");
            }
        }
        _ => {}
    }
}

fn is_reserved_field(key: &str) -> bool {
    matches!(
        key,
        "_id"
            | "_index"
            | "_type"
            | "_source"
            | "_score"
            | "_routing"
            | "_version"
            | "_seq_no"
            | "_primary_term"
    )
}

fn has_type_conflict(value: &Value, key: &str) -> bool {
    let key_lower = key.to_lowercase();

    // Keys that are almost always primitives but sometimes come as objects/arrays
    let always_primitive_keys = ["id", "type", "status", "code", "version"];
    if always_primitive_keys.contains(&key_lower.as_str()) {
        return matches!(value, Value::Object(_) | Value::Array(_));
    }

    // Only flatten objects for the date pattern checks (not arrays)
    if !matches!(value, Value::Object(_)) {
        return false;
    }

    // Date/time fields ending with common suffixes - should be strings, not objects
    let date_suffixes = ["at", "date", "time", "timestamp", "datetime"];
    for suffix in &date_suffixes {
        if key_lower.ends_with(suffix) && key_lower.len() > suffix.len() {
            return true;
        }
    }

    false
}

/// Aggressively flatten ALL nested objects to strings.
/// Use this on retry after a mapping error - it's the nuclear option.
pub fn flatten_all_objects(value: &mut Value) {
    flatten_objects_recursive(value, 0);
}

fn flatten_objects_recursive(v: &mut Value, depth: usize) {
    match v {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                if let Some(mut child) = map.remove(&k) {
                    let clean_key = sanitize_key(&k);
                    if clean_key.is_empty() {
                        continue;
                    }

                    // At depth > 0, stringify any nested objects
                    if depth > 0 && matches!(child, Value::Object(_)) {
                        map.insert(clean_key, flatten_to_string(&child));
                    } else if matches!(child, Value::Array(_)) {
                        // Stringify arrays of objects
                        if let Value::Array(ref arr) = child {
                            if arr.iter().any(|x| matches!(x, Value::Object(_))) {
                                map.insert(clean_key, flatten_to_string(&child));
                                continue;
                            }
                        }
                        flatten_objects_recursive(&mut child, depth + 1);
                        map.insert(clean_key, child);
                    } else {
                        flatten_objects_recursive(&mut child, depth + 1);
                        map.insert(clean_key, child);
                    }
                }
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                flatten_objects_recursive(child, depth + 1);
            }
        }
        _ => {}
    }
}

fn is_homogeneous_array(arr: &[Value]) -> bool {
    if arr.is_empty() {
        return true;
    }
    let first_type = value_type(&arr[0]);
    arr.iter().all(|v| value_type(v) == first_type)
}

fn value_type(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn flatten_to_string(v: &Value) -> Value {
    match v {
        Value::String(s) => Value::String(s.clone()),
        Value::Null => Value::Null,
        _ => Value::String(v.to_string()),
    }
}

fn flatten_array_to_strings(arr: &[Value]) -> Value {
    let strings: Vec<Value> = arr.iter().map(flatten_to_string).collect();
    Value::Array(strings)
}

pub fn sanitize_key(k: &str) -> String {
    let mut out = String::with_capacity(k.len().min(MAX_FIELD_NAME_LEN));
    let mut last_was_underscore = false;

    for ch in k.chars().take(MAX_FIELD_NAME_LEN) {
        if ch.is_ascii_alphanumeric() {
            out.push(if ch.is_ascii_uppercase() {
                ch.to_ascii_lowercase()
            } else {
                ch
            });
            last_was_underscore = false;
        } else if !last_was_underscore && !out.is_empty() {
            out.push('_');
            last_was_underscore = true;
        }
    }

    while out.ends_with('_') {
        out.pop();
    }
    while out.starts_with('_') {
        out.remove(0);
    }

    if out.is_empty()
        || out
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
    {
        format!("field_{}", out)
    } else {
        out
    }
}
