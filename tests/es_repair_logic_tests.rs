use logstream::es_repair::next_versioned_stream;
use logstream::es_window::TimeBoundsMs;

#[test]
fn test_next_versioned_stream_toggles_v1_v2() {
    let stable = "cloudwatch-ecs-foo";
    assert_eq!(
        next_versioned_stream("cloudwatch-ecs-foo-v1", stable),
        "cloudwatch-ecs-foo-v2"
    );
    assert_eq!(
        next_versioned_stream("cloudwatch-ecs-foo-v2", stable),
        "cloudwatch-ecs-foo-v1"
    );
}

#[test]
fn test_next_versioned_stream_defaults_to_v2() {
    let stable = "cloudwatch-ecs-foo";
    assert_eq!(
        next_versioned_stream("cloudwatch-ecs-foo", stable),
        "cloudwatch-ecs-foo-v2"
    );
}

#[test]
fn test_time_bounds_overlap() {
    let b = TimeBoundsMs {
        min_ms: 10,
        max_ms: 20,
    };
    assert!(b.overlaps(0, 10));
    assert!(b.overlaps(20, 30));
    assert!(b.overlaps(11, 19));
    assert!(!b.overlaps(21, 30));
    assert!(!b.overlaps(0, 9));
}
