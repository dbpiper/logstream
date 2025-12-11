use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    pub id: String,
    pub timestamp_ms: i64,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedEvent {
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    #[serde(rename = "event")]
    pub event: EventMeta,
    #[serde(rename = "message")]
    pub message: serde_json::Value,
    /// Parsed JSON form of the message when available (keeps original string in `message`)
    #[serde(rename = "parsed", skip_serializing_if = "Option::is_none")]
    pub parsed: Option<serde_json::Value>,
    #[serde(rename = "_target_index")]
    pub target_index: Option<String>,
    #[serde(rename = "tags")]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    pub id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_event_serialization() {
        let event = LogEvent {
            id: "test-123".to_string(),
            timestamp_ms: 1733900000000,
            message: "Hello world".to_string(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("test-123"));
        assert!(json.contains("1733900000000"));
        assert!(json.contains("Hello world"));

        let parsed: LogEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, event.id);
        assert_eq!(parsed.timestamp_ms, event.timestamp_ms);
        assert_eq!(parsed.message, event.message);
    }

    #[test]
    fn test_enriched_event_serialization() {
        let event = EnrichedEvent {
            timestamp: "2025-12-11T12:00:00Z".to_string(),
            event: EventMeta {
                id: "event-456".to_string(),
            },
            message: serde_json::Value::String("test message".to_string()),
            parsed: Some(serde_json::json!({"key": "value"})),
            target_index: Some("logs-2025.12.11".to_string()),
            tags: vec!["json_parsed".to_string(), "sync".to_string()],
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("@timestamp"));
        assert!(json.contains("_target_index"));
        assert!(json.contains("event-456"));

        let parsed: EnrichedEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.timestamp, event.timestamp);
        assert_eq!(parsed.event.id, "event-456");
        assert_eq!(parsed.tags.len(), 2);
    }

    #[test]
    fn test_enriched_event_without_parsed() {
        let event = EnrichedEvent {
            timestamp: "2025-12-11T12:00:00Z".to_string(),
            event: EventMeta {
                id: "event-789".to_string(),
            },
            message: serde_json::Value::String("plain text".to_string()),
            parsed: None,
            target_index: None,
            tags: vec![],
        };

        let json = serde_json::to_string(&event).unwrap();
        // parsed should be skipped when None
        assert!(!json.contains("\"parsed\""));
    }

    #[test]
    fn test_event_meta_serialization() {
        let meta = EventMeta {
            id: "meta-id-123".to_string(),
        };

        let json = serde_json::to_string(&meta).unwrap();
        let parsed: EventMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, meta.id);
    }
}
