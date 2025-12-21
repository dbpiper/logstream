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
    #[serde(rename = "log_group")]
    pub log_group: String,
    #[serde(rename = "message")]
    pub message: serde_json::Value,
    /// Parsed JSON form of the message when available (keeps original string in `message`)
    #[serde(rename = "parsed", skip_serializing_if = "Option::is_none")]
    pub parsed: Option<serde_json::Value>,
    #[serde(rename = "tags")]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMeta {
    pub id: String,
}
