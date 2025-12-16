use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use tracing::{info, warn};

#[derive(Clone)]
pub struct SchemaHealer {
    client: Client,
    base_url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
    timeout: Duration,
}

impl SchemaHealer {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self {
            client,
            base_url: base_url.into(),
            user: user.into(),
            pass: pass.into(),
            timeout,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn pass(&self) -> &str {
        &self.pass
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Wait for ES to be ready before proceeding.
    async fn wait_for_ready(&self) -> Result<()> {
        // Derive max attempts proportional to timeout (1 attempt per 2 seconds of timeout)
        let max_attempts = (self.timeout.as_secs() / 2).max(10) as u32;
        let wait_secs = 2u64;

        for attempt in 1..=max_attempts {
            let url = format!("{}/_cluster/health", self.base_url.trim_end_matches('/'));
            match self
                .client
                .get(&url)
                .basic_auth(&self.user, Some(&self.pass))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!("schema_heal: ES ready after {} attempts", attempt);
                    return Ok(());
                }
                Ok(_) | Err(_) => {
                    warn!(
                        "schema_heal: ES not ready (attempt {}/{}), waiting {}s",
                        attempt, max_attempts, wait_secs
                    );
                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                }
            }
        }
        anyhow::bail!("ES not ready after {} attempts", max_attempts)
    }

    /// Run schema healing: detect and fix mapping conflicts across ALL indices.
    /// Samples actual data and compares to ES mappings to find type mismatches.
    pub async fn heal(&self) -> Result<usize> {
        self.wait_for_ready().await?;
        let indices = self.list_all_user_indices().await?;
        if indices.is_empty() {
            return Ok(0);
        }

        info!(
            "schema_heal: checking {} indices for conflicts",
            indices.len()
        );

        let mut problematic = Vec::new();

        let cross_index_conflicts = self.detect_cross_index_conflicts(&indices).await?;
        if !cross_index_conflicts.is_empty() {
            info!(
                "schema_heal: found {} indices with cross-index mapping conflicts",
                cross_index_conflicts.len()
            );
            problematic.extend(cross_index_conflicts);
        }

        let within_index_conflicts = self.detect_problematic_indices(&indices).await?;
        if !within_index_conflicts.is_empty() {
            info!(
                "schema_heal: found {} indices with data-mapping type mismatches",
                within_index_conflicts.len()
            );
            for idx in within_index_conflicts {
                if !problematic.contains(&idx) {
                    problematic.push(idx);
                }
            }
        }

        if problematic.is_empty() {
            info!(
                "schema_heal: no conflicts detected across {} indices",
                indices.len()
            );
            return Ok(0);
        }

        info!(
            "schema_heal: detected {} indices with conflicts, fixing",
            problematic.len()
        );

        let mut fixed = 0;
        for idx in &problematic {
            info!("schema_heal: deleting index {} to fix conflicts", idx);
            if let Err(err) = self.delete_index(idx).await {
                warn!("schema_heal: failed to delete {}: {err:?}", idx);
            } else {
                fixed += 1;
            }
        }

        Ok(fixed)
    }

    /// Detect indices with cross-index mapping conflicts.
    /// Groups indices by pattern prefix and checks if the same field has different types across indices.
    async fn detect_cross_index_conflicts(&self, indices: &[String]) -> Result<Vec<String>> {
        use std::collections::{HashMap, HashSet};

        let mut pattern_groups: HashMap<String, Vec<String>> = HashMap::new();
        for idx in indices {
            let prefix = extract_index_prefix(idx);
            pattern_groups.entry(prefix).or_default().push(idx.clone());
        }

        let mut conflicted_indices = HashSet::new();

        for (pattern, group_indices) in pattern_groups {
            if group_indices.len() < 2 {
                continue;
            }

            let mut field_types: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();

            for idx in &group_indices {
                match self.fetch_index_mapping(idx).await {
                    Ok(mapping) => {
                        let fields = extract_all_field_types(&mapping);
                        for (field_path, field_type) in fields {
                            field_types
                                .entry(field_path)
                                .or_default()
                                .entry(field_type)
                                .or_default()
                                .push(idx.clone());
                        }
                    }
                    Err(e) => {
                        warn!("schema_heal: failed to fetch mapping for {}: {}", idx, e);
                    }
                }
            }

            for (field_path, type_map) in field_types {
                if type_map.len() > 1 {
                    let type_summary: Vec<String> = type_map
                        .iter()
                        .map(|(t, indices)| format!("{} in {} indices", t, indices.len()))
                        .collect();

                    info!(
                        "schema_heal: field '{}' has conflicting types in pattern '{}': {}",
                        field_path,
                        pattern,
                        type_summary.join(", ")
                    );

                    let minority_indices = find_minority_type_indices(&type_map);
                    for idx in minority_indices {
                        conflicted_indices.insert(idx);
                    }
                }
            }
        }

        Ok(conflicted_indices.into_iter().collect())
    }

    /// Detect indices with type mismatches by sampling actual data and comparing to mappings.
    /// Uses consensus-based type detection - compares what the data actually is vs what ES thinks it is.
    async fn detect_problematic_indices(&self, indices: &[String]) -> Result<Vec<String>> {
        let mut problematic = Vec::new();

        for idx in indices {
            match self.find_type_mismatches(idx).await {
                Ok(mismatches) if !mismatches.is_empty() => {
                    info!(
                        "schema_heal: index {} has {} type mismatches: {:?}",
                        idx,
                        mismatches.len(),
                        &mismatches[..mismatches.len().min(3)]
                    );
                    problematic.push(idx.clone());
                }
                Ok(_) => {}
                Err(e) => {
                    warn!("schema_heal: failed to check index {}: {}", idx, e);
                }
            }
        }

        if !problematic.is_empty() {
            info!(
                "schema_heal: found {} indices with type mismatches out of {} total",
                problematic.len(),
                indices.len()
            );
        }

        Ok(problematic)
    }

    /// Find all type mismatches in an index by sampling data and comparing to ES mapping.
    /// Returns list of field paths where the actual data type differs from ES mapping.
    async fn find_type_mismatches(&self, index: &str) -> Result<Vec<String>> {
        // Sample documents from the index
        let samples = self.sample_documents(index, 100).await?;
        if samples.is_empty() {
            return Ok(vec![]);
        }

        // Extract all field paths and their values from samples
        let field_samples = extract_field_samples(&samples);

        // Get the ES mapping for this index
        let mapping = self.fetch_index_mapping(index).await?;

        // Compare each field's actual type vs ES mapping type
        let mut mismatches = Vec::new();
        for (field_path, values) in &field_samples {
            let actual_type = analyze_field_type(values);
            // If we can't infer a type (e.g., field is absent/null/empty array in samples),
            // do not treat it as a mismatch. Otherwise we can end up deleting hot indices
            // just because a field is present but empty.
            if !is_reliable_inferred_type(actual_type) {
                continue;
            }
            let es_type = self.get_mapping_type_for_path(&mapping, field_path);

            if let Some(es_t) = es_type {
                if !types_compatible(&es_t, &actual_type) {
                    mismatches.push(format!(
                        "{} (ES:{:?} vs data:{:?})",
                        field_path, es_t, actual_type
                    ));
                }
            }
        }

        Ok(mismatches)
    }

    /// Sample documents from an index for type analysis.
    async fn sample_documents(&self, index: &str, count: usize) -> Result<Vec<Value>> {
        let url = format!("{}/{}/_search", self.base_url.trim_end_matches('/'), index);

        let body = serde_json::json!({
            "size": count,
            "query": { "match_all": {} },
            "_source": true
        });

        let resp = self
            .client
            .post(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(vec![]);
        }

        let result: Value = resp.json().await?;
        let hits = result
            .pointer("/hits/hits")
            .and_then(|h| h.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(hits
            .into_iter()
            .filter_map(|h| h.get("_source").cloned())
            .collect())
    }

    /// Get the ES mapping type for a dotted field path.
    fn get_mapping_type_for_path(&self, mapping: &Value, field_path: &str) -> Option<FieldType> {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = mapping.get("properties")?;

        for (i, part) in parts.iter().enumerate() {
            let field_def = current.get(part)?;

            if i == parts.len() - 1 {
                // Last part - get the type
                if let Some(type_str) = field_def.get("type").and_then(|t| t.as_str()) {
                    return Some(es_type_to_field_type(type_str));
                }
                // No explicit type but has properties = object
                if field_def.get("properties").is_some() {
                    return Some(FieldType::Object);
                }
                return None;
            }

            // Navigate deeper
            current = field_def.get("properties")?;
        }

        None
    }

    /// List indices matching pattern, sorted by name (date order).
    /// List all user indices (excluding system indices like .kibana, .security, etc.)
    async fn list_all_user_indices(&self) -> Result<Vec<String>> {
        let url = format!(
            "{}/_cat/indices?format=json&s=index",
            self.base_url.trim_end_matches('/')
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("list indices")?;

        if !resp.status().is_success() {
            return Ok(vec![]);
        }

        let items: Vec<Value> = resp.json().await.unwrap_or_default();
        let indices: Vec<String> = items
            .iter()
            .filter_map(|v| v.get("index").and_then(|i| i.as_str()).map(String::from))
            .filter(|idx| !is_system_index(idx))
            .collect();

        Ok(indices)
    }

    /// Fetch actual mapping for an index.
    async fn fetch_index_mapping(&self, index: &str) -> Result<Value> {
        let url = format!("{}/{}/_mapping", self.base_url.trim_end_matches('/'), index);

        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("fetch mapping")?;

        if !resp.status().is_success() {
            anyhow::bail!("failed to fetch mapping for {}", index);
        }

        let body: Value = resp.json().await.context("parse mapping")?;
        let mappings = body
            .pointer(&format!("/{}/mappings", index))
            .cloned()
            .unwrap_or(Value::Null);

        Ok(mappings)
    }

    async fn delete_index(&self, index: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), index);
        let resp = self
            .client
            .delete(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .context("delete index")?;

        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            anyhow::bail!("failed to delete {}", index);
        }
        Ok(())
    }

    /// Analyze mapping conflicts and fix them by deleting indices with wrong mappings.
    /// Returns the list of indices that were deleted and need re-population.
    pub async fn fix_mapping_conflicts(
        &self,
        conflicts: &[MappingConflictInfo],
    ) -> Result<Vec<String>> {
        if conflicts.is_empty() {
            return Ok(vec![]);
        }

        // Group conflicts by index
        let mut indices_to_fix: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for conflict in conflicts {
            // Analyze the data to determine what the correct type should be
            let correct_type = analyze_field_type(&conflict.sample_values);
            // If we can't infer the data type from samples, don't delete/recreate indices.
            // This avoids churn when a field is intermittently absent/null/empty.
            if !is_reliable_inferred_type(correct_type) {
                continue;
            }
            let current_type = self
                .get_field_type(&conflict.index, &conflict.field_path)
                .await;

            info!(
                "schema_heal: field {} in {} - current={:?} correct={:?} (from {} samples)",
                conflict.field_path,
                conflict.index,
                current_type,
                correct_type,
                conflict.sample_values.len()
            );

            // If the current ES type doesn't match what the data actually is,
            // mark this index for deletion and re-creation
            if let Some(current) = current_type {
                if !types_compatible(&current, &correct_type) {
                    warn!(
                        "schema_heal: index {} has wrong mapping for {}: ES has {:?}, data is {:?}",
                        conflict.index, conflict.field_path, current, correct_type
                    );
                    indices_to_fix.insert(conflict.index.clone());
                }
            }
        }

        // Delete indices with wrong mappings
        let mut deleted = Vec::new();
        for index in &indices_to_fix {
            info!(
                "schema_heal: deleting index {} to fix mapping conflicts",
                index
            );
            if let Err(err) = self.delete_index(index).await {
                warn!("schema_heal: failed to delete {}: {err:?}", index);
            } else {
                deleted.push(index.clone());
            }
        }

        Ok(deleted)
    }

    /// Get the ES mapping type for a specific field path
    async fn get_field_type(&self, index: &str, field_path: &str) -> Option<FieldType> {
        let url = format!(
            "{}/{}/_mapping/field/{}",
            self.base_url.trim_end_matches('/'),
            index,
            field_path
        );

        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.user, Some(&self.pass))
            .send()
            .await
            .ok()?;

        if !resp.status().is_success() {
            return None;
        }

        let body: Value = resp.json().await.ok()?;

        // Navigate to the mapping type
        // Response is like: {"index": {"mappings": {"field.path": {"mapping": {"fieldname": {"type": "text"}}}}}}
        let field_name = field_path.split('.').next_back()?;
        let type_str = body
            .pointer(&format!(
                "/{}/mappings/{}/mapping/{}/type",
                index, field_path, field_name
            ))
            .and_then(|v| v.as_str());

        type_str.map(|t| match t {
            "object" | "nested" => FieldType::Object,
            "text" | "keyword" => FieldType::String,
            "long" | "integer" | "short" | "byte" | "double" | "float" => FieldType::Number,
            "boolean" => FieldType::Boolean,
            "date" => FieldType::Date,
            _ => FieldType::Unknown,
        })
    }
}

/// Check if an index is a system/internal index that should not be healed.
fn is_system_index(index: &str) -> bool {
    // ES system indices start with .
    if index.starts_with('.') {
        return true;
    }

    // ILM and other internal indices
    let internal_prefixes = [
        "ilm-history",
        "slm-history",
        "shrink-",
        "restore-",
        "partial-",
    ];

    internal_prefixes.iter().any(|p| index.starts_with(p))
}

/// Extract all field paths and their sample values from a list of documents.
/// Returns a map of field_path -> list of values found at that path.
fn extract_field_samples(documents: &[Value]) -> std::collections::HashMap<String, Vec<Value>> {
    let mut samples: std::collections::HashMap<String, Vec<Value>> =
        std::collections::HashMap::new();

    for doc in documents {
        collect_field_values(doc, "", &mut samples);
    }

    samples
}

/// Recursively collect field values from a document.
fn collect_field_values(
    value: &Value,
    path: &str,
    samples: &mut std::collections::HashMap<String, Vec<Value>>,
) {
    match value {
        Value::Object(map) => {
            for (key, val) in map {
                let field_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };

                // Add this value to samples for this path
                samples
                    .entry(field_path.clone())
                    .or_default()
                    .push(val.clone());

                // Recurse into nested objects
                if val.is_object() {
                    collect_field_values(val, &field_path, samples);
                }
            }
        }
        Value::Array(arr) => {
            // For arrays, sample the elements
            for item in arr {
                if item.is_object() {
                    collect_field_values(item, path, samples);
                }
            }
        }
        _ => {}
    }
}

/// Convert ES type string to FieldType.
fn es_type_to_field_type(es_type: &str) -> FieldType {
    match es_type {
        "object" | "nested" => FieldType::Object,
        "text" | "keyword" => FieldType::String,
        "long" | "integer" | "short" | "byte" | "double" | "float" | "half_float"
        | "scaled_float" => FieldType::Number,
        "boolean" => FieldType::Boolean,
        "date" | "date_nanos" => FieldType::Date,
        _ => FieldType::Unknown,
    }
}

/// Information about a mapping conflict
#[derive(Debug, Clone)]
pub struct MappingConflictInfo {
    pub index: String,
    pub field_path: String,
    pub sample_values: Vec<Value>,
}

/// Field type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldType {
    Object,
    String,
    Number,
    Boolean,
    Date,
    Array,
    Null,
    Unknown,
}

pub fn is_reliable_inferred_type(field_type: FieldType) -> bool {
    !matches!(field_type, FieldType::Unknown | FieldType::Null)
}

/// Analyze a sample of values to determine the dominant type
pub fn analyze_field_type(samples: &[Value]) -> FieldType {
    if samples.is_empty() {
        return FieldType::Unknown;
    }

    let mut type_counts: std::collections::HashMap<FieldType, usize> =
        std::collections::HashMap::new();

    for value in samples {
        let ft = value_to_field_type(value);
        *type_counts.entry(ft).or_default() += 1;
    }

    // Find the most common type (excluding null)
    type_counts
        .iter()
        .filter(|(t, _)| **t != FieldType::Null)
        .max_by_key(|(_, count)| *count)
        .map(|(t, _)| *t)
        .unwrap_or(FieldType::Unknown)
}

fn value_to_field_type(value: &Value) -> FieldType {
    match value {
        Value::Null => FieldType::Null,
        Value::Bool(_) => FieldType::Boolean,
        Value::Number(_) => FieldType::Number,
        Value::String(s) => {
            // Check if it looks like a date
            if s.contains('T') && (s.contains('-') || s.contains(':')) {
                FieldType::Date
            } else {
                FieldType::String
            }
        }
        Value::Array(arr) => {
            // ES treats arrays transparently - look at element type
            // An array of strings is stored as a string field in ES
            if let Some(first) = arr.iter().find(|v| !v.is_null()) {
                match first {
                    Value::String(_) => FieldType::String,
                    Value::Number(_) => FieldType::Number,
                    Value::Bool(_) => FieldType::Boolean,
                    Value::Object(_) => FieldType::Object,
                    Value::Array(_) => FieldType::Array, // nested array
                    Value::Null => FieldType::Unknown,
                }
            } else {
                FieldType::Unknown // empty array
            }
        }
        Value::Object(_) => FieldType::Object,
    }
}

/// Check if two types are compatible (can coexist in same field)
fn types_compatible(es_type: &FieldType, data_type: &FieldType) -> bool {
    if es_type == data_type {
        return true;
    }

    // String and Date are compatible (dates are often stored as strings)
    if matches!(
        (es_type, data_type),
        (FieldType::String, FieldType::Date) | (FieldType::Date, FieldType::String)
    ) {
        return true;
    }

    // Number types can coexist
    if *es_type == FieldType::Number && *data_type == FieldType::Number {
        return true;
    }

    // ES treats arrays transparently - a String field can hold ["a", "b"] or "a"
    // So Array is compatible with the element type
    if *data_type == FieldType::Array {
        // Array data is compatible with String (arrays of strings), Number, etc.
        // ES doesn't distinguish array vs single value in mapping
        return matches!(
            es_type,
            FieldType::String | FieldType::Number | FieldType::Boolean | FieldType::Date
        );
    }

    // The critical incompatibility: Object vs primitive
    // This is what causes "tried to parse field as object, but found a concrete value"
    // Only flag Object vs non-Object mismatches
    false
}

fn extract_index_prefix(index: &str) -> String {
    index
        .rsplit_once('-')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| index.to_string())
}

pub fn extract_all_field_types(mapping: &Value) -> Vec<(String, String)> {
    let mut result = Vec::new();
    if let Some(properties) = mapping.get("properties") {
        extract_field_types_recursive(properties, "", &mut result);
    }
    result
}

fn extract_field_types_recursive(obj: &Value, prefix: &str, result: &mut Vec<(String, String)>) {
    if let Some(props) = obj.as_object() {
        for (key, val) in props {
            let field_path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{}.{}", prefix, key)
            };

            if let Some(type_str) = val.get("type").and_then(|t| t.as_str()) {
                result.push((field_path.clone(), type_str.to_string()));
            }

            if let Some(nested_props) = val.get("properties") {
                extract_field_types_recursive(nested_props, &field_path, result);
            }
        }
    }
}

pub fn find_minority_type_indices(
    type_map: &std::collections::HashMap<String, Vec<String>>,
) -> Vec<String> {
    if type_map.len() <= 1 {
        return vec![];
    }

    let max_count = type_map.values().map(|v| v.len()).max().unwrap_or(0);

    let mut minority = Vec::new();
    for indices in type_map.values() {
        if indices.len() < max_count {
            minority.extend(indices.clone());
        }
    }

    minority
}
