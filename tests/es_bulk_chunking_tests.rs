use logstream::types::{EnrichedEvent, EventMeta};

fn mk_event(id: &str) -> EnrichedEvent {
    EnrichedEvent {
        timestamp: "2025-12-21T00:00:00Z".to_string(),
        event: EventMeta { id: id.to_string() },
        log_group: "ecs-foo".to_string(),
        message: serde_json::json!({"id": id}),
        parsed: None,
        tags: vec![],
    }
}

#[test]
fn test_adaptive_chunking_does_not_create_tiny_remainder_chunk() {
    let batch = (0..1000)
        .map(|i| mk_event(&format!("id-{i}")))
        .collect::<Vec<EnrichedEvent>>();

    // Mirrors the chunk sizing logic in send_bulk_adaptive_tracked:
    let effective_in_flight = 3usize;
    let desired_chunks = ((batch.len() / 100).max(1)).min(effective_in_flight);
    let base = batch.len() / desired_chunks;
    let rem = batch.len() % desired_chunks;

    let mut it = batch.into_iter();
    let chunks = (0..desired_chunks)
        .filter_map(|i| {
            let size = base + if i < rem { 1 } else { 0 };
            (size > 0)
                .then_some(it.by_ref().take(size).collect::<Vec<EnrichedEvent>>())
                .filter(|c| !c.is_empty())
        })
        .collect::<Vec<Vec<EnrichedEvent>>>();

    let lens = chunks.iter().map(|c| c.len()).collect::<Vec<usize>>();
    assert_eq!(lens, vec![334, 333, 333]);
}
