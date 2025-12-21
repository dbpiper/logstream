use logstream::es_bootstrap::{
    build_ilm_policy_body, build_index_template_body, compute_replicas, ilm_policy_name,
    index_template_name,
};

#[test]
fn compute_replicas_clamps_to_data_nodes_minus_one() {
    assert_eq!(compute_replicas(0, 1), 0);
    assert_eq!(compute_replicas(1, 1), 0);
    assert_eq!(compute_replicas(2, 1), 0);
    assert_eq!(compute_replicas(1, 2), 1);
    assert_eq!(compute_replicas(2, 2), 1);
    assert_eq!(compute_replicas(2, 3), 2);
}

#[test]
fn ilm_names_are_stable() {
    assert_eq!(
        ilm_policy_name("cloudwatch"),
        "logstream-cloudwatch-ilm".to_string()
    );
    assert_eq!(
        index_template_name("cloudwatch"),
        "logstream-cloudwatch-template".to_string()
    );
}

#[test]
fn ilm_policy_has_rollover_and_no_delete_by_default() {
    let body = build_ilm_policy_body("1d", "25gb", false, "30d");
    let phases = body
        .get("policy")
        .and_then(|p| p.get("phases"))
        .and_then(|p| p.as_object())
        .expect("phases object");

    assert!(phases.contains_key("hot"));
    assert!(!phases.contains_key("delete"));

    let hot = phases.get("hot").unwrap();
    let rollover = hot
        .get("actions")
        .and_then(|a| a.get("rollover"))
        .expect("rollover");

    assert_eq!(rollover.get("max_age").and_then(|v| v.as_str()), Some("1d"));
    assert_eq!(
        rollover
            .get("max_primary_shard_size")
            .and_then(|v| v.as_str()),
        Some("25gb")
    );
}

#[test]
fn index_template_enables_data_stream_and_timestamp_mapping() {
    let body = build_index_template_body("cloudwatch", "logstream-cloudwatch-ilm", 0);
    assert_eq!(
        body.get("index_patterns")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str()),
        Some("cloudwatch-*")
    );
    assert!(body.get("data_stream").is_some());

    let settings = body
        .get("template")
        .and_then(|t| t.get("settings"))
        .expect("settings");
    assert_eq!(
        settings.get("number_of_replicas").and_then(|v| v.as_u64()),
        Some(0)
    );
    assert_eq!(
        settings
            .get("index.lifecycle.name")
            .and_then(|v| v.as_str()),
        Some("logstream-cloudwatch-ilm")
    );

    let timestamp_type = body
        .get("template")
        .and_then(|t| t.get("mappings"))
        .and_then(|m| m.get("properties"))
        .and_then(|p| p.get("@timestamp"))
        .and_then(|t| t.get("type"))
        .and_then(|v| v.as_str());
    assert_eq!(timestamp_type, Some("date"));
}
