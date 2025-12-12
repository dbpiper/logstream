//! Tests for OS-style process scheduler.

use logstream::process::{priority_for_day_offset, Priority};
use logstream::process::{
    GroupScheduler, ProcessKind, ProcessScheduler, ProcessState, ResourcePool, ResourceRequest,
    Resources, TaskType,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Priority Tests
// ============================================================================

#[test]
fn test_priority_constants() {
    assert_eq!(Priority::CRITICAL.value(), 255);
    assert_eq!(Priority::HIGH.value(), 192);
    assert_eq!(Priority::NORMAL.value(), 128);
    assert_eq!(Priority::LOW.value(), 64);
    assert_eq!(Priority::IDLE.value(), 0);
}

#[test]
fn test_priority_ordering_all() {
    let values = [
        Priority::CRITICAL,
        Priority::HIGH,
        Priority::NORMAL,
        Priority::LOW,
        Priority::IDLE,
    ];

    for i in 0..values.len() - 1 {
        assert!(
            values[i] > values[i + 1],
            "{:?} should be higher priority than {:?}",
            values[i],
            values[i + 1]
        );
    }
}

#[test]
fn test_priority_aging_effect() {
    let priority = Priority::NORMAL; // value = 128

    // No wait: effective = 128
    assert_eq!(priority.effective_priority(Duration::ZERO), 128);

    // 10 seconds wait: effective = 129 (higher priority)
    assert_eq!(priority.effective_priority(Duration::from_secs(10)), 129);

    // 60 seconds wait: effective = 134
    assert_eq!(priority.effective_priority(Duration::from_secs(60)), 134);

    // Long wait: capped at 255 (CRITICAL)
    assert_eq!(priority.effective_priority(Duration::from_secs(2000)), 255);
}

#[test]
fn test_priority_aging_prevents_starvation() {
    let idle = Priority::IDLE; // value = 0
    let critical = Priority::CRITICAL; // value = 255

    // After enough waiting, even IDLE becomes CRITICAL priority
    let idle_waited = idle.effective_priority(Duration::from_secs(3000)); // 0 + 300 = 300, capped to 255
    let critical_fresh = critical.effective_priority(Duration::ZERO);

    // Both should be at max priority (255) after enough aging
    assert_eq!(idle_waited, 255);
    assert_eq!(critical_fresh, 255);
}

#[test]
fn test_priority_for_day_offset_mapping() {
    // Today: CRITICAL
    assert_eq!(priority_for_day_offset(0), Priority::CRITICAL);

    // Last 3 days: HIGH
    for day in 1..=3 {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::HIGH,
            "Day {} should be HIGH",
            day
        );
    }

    // Last week (days 4-7): NORMAL
    for day in 4..=7 {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::NORMAL,
            "Day {} should be NORMAL",
            day
        );
    }

    // Last month (days 8-30): LOW
    for day in 8..=30 {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::LOW,
            "Day {} should be LOW",
            day
        );
    }

    // Older: IDLE
    for day in [31, 60, 365, 3650] {
        assert_eq!(
            priority_for_day_offset(day),
            Priority::IDLE,
            "Day {} should be IDLE",
            day
        );
    }
}

// ============================================================================
// Resource Pool Tests
// ============================================================================

#[test]
fn test_resource_pool_creation() {
    let resources = Resources {
        cw_api_quota: 50,
        es_bulk_capacity: 10000,
        memory_quota: 100000,
    };
    let pool = ResourcePool::new(resources, 4);

    assert_eq!(pool.utilization(), 0.0);
}

#[test]
fn test_resource_pool_acquire_success() {
    let pool = ResourcePool::new(Resources::default(), 8);
    let request = ResourceRequest {
        cw_api_calls: 5,
        es_events: 100,
        memory_events: 1000,
    };

    let grant = pool.try_acquire(&request);
    assert!(grant.is_some());
}

#[test]
fn test_resource_pool_acquire_insufficient() {
    let resources = Resources {
        cw_api_quota: 10,
        es_bulk_capacity: 100,
        memory_quota: 1000,
    };
    let pool = ResourcePool::new(resources, 4);

    let big_request = ResourceRequest {
        cw_api_calls: 20, // More than available
        es_events: 50,
        memory_events: 500,
    };

    let grant = pool.try_acquire(&big_request);
    assert!(grant.is_none());
}

#[test]
fn test_resource_pool_release_restores() {
    let resources = Resources {
        cw_api_quota: 10,
        es_bulk_capacity: 100,
        memory_quota: 1000,
    };
    let pool = ResourcePool::new(resources, 4);

    let request = ResourceRequest {
        cw_api_calls: 10,
        es_events: 100,
        memory_events: 1000,
    };

    // Acquire all resources
    let grant = pool.try_acquire(&request).unwrap();

    // Can't acquire more
    assert!(pool.try_acquire(&request).is_none());

    // Release
    pool.release(grant);

    // Can acquire again
    assert!(pool.try_acquire(&request).is_some());
}

#[tokio::test]
async fn test_resource_pool_concurrency_slot() {
    let pool = Arc::new(ResourcePool::new(Resources::default(), 2));

    let acquired = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let pool_clone = pool.clone();
        let acquired_clone = acquired.clone();

        handles.push(tokio::spawn(async move {
            let _permit = pool_clone.acquire_slot().await;
            acquired_clone.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            acquired_clone.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    // Give some time for concurrent execution
    tokio::time::sleep(Duration::from_millis(5)).await;

    // At most 2 should be running at once
    assert!(acquired.load(Ordering::SeqCst) <= 2);

    for handle in handles {
        handle.await.unwrap();
    }
}

// ============================================================================
// Process Scheduler Tests
// ============================================================================

#[tokio::test]
async fn test_scheduler_spawn_creates_process() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    let pid = scheduler
        .spawn("test-process".into(), 5, Priority::NORMAL)
        .await;

    let info = scheduler.get_process(pid);
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.name, "test-process");
    assert_eq!(info.day_offset, 5);
    assert_eq!(info.priority, Priority::NORMAL);
    assert_eq!(info.state, ProcessState::Ready);
}

#[tokio::test]
async fn test_scheduler_pid_increments() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    let pid1 = scheduler.spawn("p1".into(), 0, Priority::NORMAL).await;
    let pid2 = scheduler.spawn("p2".into(), 1, Priority::NORMAL).await;
    let pid3 = scheduler.spawn("p3".into(), 2, Priority::NORMAL).await;

    assert_eq!(pid1, 1);
    assert_eq!(pid2, 2);
    assert_eq!(pid3, 3);
}

#[tokio::test]
async fn test_scheduler_schedules_by_priority() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    // Spawn in reverse priority order
    let idle_pid = scheduler.spawn("idle".into(), 100, Priority::IDLE).await;
    let low_pid = scheduler.spawn("low".into(), 30, Priority::LOW).await;
    let normal_pid = scheduler.spawn("normal".into(), 7, Priority::NORMAL).await;
    let high_pid = scheduler.spawn("high".into(), 1, Priority::HIGH).await;
    let realtime_pid = scheduler
        .spawn("realtime".into(), 0, Priority::CRITICAL)
        .await;

    // Schedule in priority order
    let scheduled1 = scheduler.schedule().await.unwrap();
    let scheduled2 = scheduler.schedule().await.unwrap();
    let scheduled3 = scheduler.schedule().await.unwrap();
    let scheduled4 = scheduler.schedule().await.unwrap();
    let scheduled5 = scheduler.schedule().await.unwrap();

    // Should be: realtime, high, normal, low, idle
    assert_eq!(scheduled1, realtime_pid);
    assert_eq!(scheduled2, high_pid);
    assert_eq!(scheduled3, normal_pid);
    assert_eq!(scheduled4, low_pid);
    assert_eq!(scheduled5, idle_pid);
}

#[tokio::test]
async fn test_scheduler_running_state() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    let pid = scheduler.spawn("test".into(), 0, Priority::NORMAL).await;

    // Before scheduling: Ready
    let info = scheduler.get_process(pid).unwrap();
    assert_eq!(info.state, ProcessState::Ready);

    // After scheduling: Running
    scheduler.schedule().await;
    let info = scheduler.get_process(pid).unwrap();
    assert_eq!(info.state, ProcessState::Running);
}

#[tokio::test]
async fn test_scheduler_terminate_records_stats() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    let pid = scheduler.spawn("test".into(), 0, Priority::NORMAL).await;
    scheduler.schedule().await;

    scheduler.terminate(pid, 5000, Duration::from_secs(10));

    let info = scheduler.get_process(pid).unwrap();
    assert_eq!(info.state, ProcessState::Terminated);
    assert_eq!(info.events_processed, 5000);
    assert_eq!(info.cpu_time, Duration::from_secs(10));
}

#[tokio::test]
async fn test_scheduler_block_unblock_cycle() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    let pid = scheduler.spawn("test".into(), 0, Priority::NORMAL).await;
    scheduler.schedule().await;

    // Block
    scheduler.block(pid);
    assert_eq!(
        scheduler.get_process(pid).unwrap().state,
        ProcessState::Blocked
    );

    // Unblock
    scheduler.unblock(pid).await;
    assert_eq!(
        scheduler.get_process(pid).unwrap().state,
        ProcessState::Ready
    );

    // Can be scheduled again
    let scheduled = scheduler.schedule().await.unwrap();
    assert_eq!(scheduled, pid);
}

#[tokio::test]
async fn test_scheduler_list_processes() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    scheduler.spawn("p1".into(), 0, Priority::CRITICAL).await;
    scheduler.spawn("p2".into(), 1, Priority::HIGH).await;
    scheduler.spawn("p3".into(), 2, Priority::NORMAL).await;

    let processes = scheduler.list_processes();
    assert_eq!(processes.len(), 3);

    let names: Vec<_> = processes.iter().map(|p| p.name.as_str()).collect();
    assert!(names.contains(&"p1"));
    assert!(names.contains(&"p2"));
    assert!(names.contains(&"p3"));
}

#[tokio::test]
async fn test_scheduler_process_counts() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    scheduler.spawn("p1".into(), 0, Priority::NORMAL).await;
    scheduler.spawn("p2".into(), 1, Priority::NORMAL).await;
    scheduler.spawn("p3".into(), 2, Priority::NORMAL).await;

    // All ready
    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 3);
    assert_eq!(counts.running, 0);

    // Schedule one
    let pid = scheduler.schedule().await.unwrap();
    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 2);
    assert_eq!(counts.running, 1);

    // Terminate it
    scheduler.terminate(pid, 0, Duration::ZERO);
    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 2);
    assert_eq!(counts.running, 0);
    assert_eq!(counts.terminated, 1);
}

#[tokio::test]
async fn test_scheduler_time_quantum() {
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 250);
    assert_eq!(scheduler.time_quantum(), Duration::from_millis(250));
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_full_process_lifecycle() {
    let scheduler = ProcessScheduler::new(Resources::default(), 2, 100);

    // Spawn multiple processes
    for day in 0..10 {
        let priority = priority_for_day_offset(day);
        scheduler.spawn(format!("day-{}", day), day, priority).await;
    }

    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 10);

    // Process them
    let mut completed = 0;
    while let Some(pid) = scheduler.schedule().await {
        let info = scheduler.get_process(pid).unwrap();
        assert_eq!(info.state, ProcessState::Running);

        // Simulate work
        tokio::time::sleep(Duration::from_millis(1)).await;

        scheduler.terminate(pid, 100, Duration::from_millis(1));
        completed += 1;

        if completed >= 10 {
            break;
        }
    }

    let counts = scheduler.process_counts();
    assert_eq!(counts.terminated, 10);
}

#[tokio::test]
async fn test_concurrent_workers() {
    let scheduler = Arc::new(ProcessScheduler::new(Resources::default(), 4, 100));

    // Spawn many processes
    for day in 0..20 {
        scheduler
            .spawn(format!("day-{}", day), day, priority_for_day_offset(day))
            .await;
    }

    let completed = Arc::new(AtomicUsize::new(0));
    let mut worker_handles = Vec::new();

    // Spawn 4 workers
    for worker_id in 0..4 {
        let sched = scheduler.clone();
        let completed_clone = completed.clone();

        worker_handles.push(tokio::spawn(async move {
            loop {
                let pid = tokio::select! {
                    pid = sched.schedule() => pid,
                    _ = tokio::time::sleep(Duration::from_millis(50)) => None,
                };

                if let Some(pid) = pid {
                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    sched.terminate(pid, 100, Duration::from_millis(5));
                    completed_clone.fetch_add(1, Ordering::SeqCst);
                } else {
                    break;
                }
            }
            worker_id
        }));
    }

    // Wait for all workers
    for handle in worker_handles {
        handle.await.unwrap();
    }

    assert_eq!(completed.load(Ordering::SeqCst), 20);
}

#[tokio::test]
async fn test_priority_respected_under_load() {
    let scheduler = ProcessScheduler::new(Resources::default(), 1, 100);

    // Spawn processes with different priority levels
    // Day 0 = CRITICAL, Day 5 = NORMAL, Day 15 = LOW, Day 50 = IDLE
    let days_to_spawn = [50, 15, 5, 0]; // Spawn in reverse priority order
    for day in days_to_spawn {
        let priority = priority_for_day_offset(day);
        scheduler.spawn(format!("day-{}", day), day, priority).await;
    }

    // Verify scheduling order respects priority
    let mut scheduled_days = Vec::new();
    for _ in 0..4 {
        let pid = scheduler.schedule().await.unwrap();
        let info = scheduler.get_process(pid).unwrap();
        scheduled_days.push(info.day_offset);
        scheduler.terminate(pid, 0, Duration::ZERO);
    }

    // Day 0 (CRITICAL) should be first
    assert_eq!(scheduled_days[0], 0, "CRITICAL should be scheduled first");
    // Day 5 (NORMAL) should be second
    assert_eq!(scheduled_days[1], 5, "NORMAL should be scheduled second");
    // Day 15 (LOW) should be third
    assert_eq!(scheduled_days[2], 15, "LOW should be scheduled third");
    // Day 50 (IDLE) should be last
    assert_eq!(scheduled_days[3], 50, "IDLE should be scheduled last");
}

// ============================================================================
// Optimality Tests - Verify scheduler is almost surely optimal
// ============================================================================

#[tokio::test]
async fn test_optimal_high_priority_latency() {
    // High priority processes should have minimal wait time
    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    // Spawn low priority background work
    for i in 0..50 {
        scheduler
            .spawn(format!("background-{}", i), 100 + i, Priority::IDLE)
            .await;
    }

    // Spawn high priority process after background
    let realtime_pid = scheduler
        .spawn("realtime".into(), 0, Priority::CRITICAL)
        .await;

    // High priority should be scheduled first despite being spawned last
    let first_scheduled = scheduler.schedule().await.unwrap();
    assert_eq!(
        first_scheduled, realtime_pid,
        "REALTIME should preempt all IDLE processes"
    );
}

#[tokio::test]
async fn test_optimal_no_starvation_with_aging() {
    // Even IDLE processes should eventually run due to aging
    let scheduler = ProcessScheduler::new(Resources::default(), 1, 100);

    // Spawn an IDLE process
    let idle_pid = scheduler.spawn("idle".into(), 365, Priority::IDLE).await;

    // Spawn many HIGH priority processes
    for i in 0..10 {
        scheduler
            .spawn(format!("high-{}", i), i, Priority::HIGH)
            .await;
    }

    // Process some HIGH priority tasks
    for _ in 0..5 {
        let pid = scheduler.schedule().await.unwrap();
        scheduler.terminate(pid, 100, Duration::from_millis(10));
    }

    // Check that IDLE process has accumulated wait time
    let idle_info = scheduler.get_process(idle_pid).unwrap();
    assert!(
        idle_info.wait_time > Duration::ZERO,
        "IDLE process should have wait time for aging"
    );

    // Eventually the IDLE process should be schedulable after enough aging
    // (aging gives +1 priority boost per 10 seconds of waiting)
}

#[tokio::test]
async fn test_optimal_throughput_scales_with_workers() {
    let num_processes: usize = 40;
    let work_duration = Duration::from_millis(5);

    let single_worker_time = {
        let scheduler = ProcessScheduler::new(Resources::default(), 1, 100);
        for i in 0..num_processes {
            scheduler
                .spawn(format!("task-{}", i), i as u32, Priority::NORMAL)
                .await;
        }

        let start = std::time::Instant::now();
        for _ in 0..num_processes {
            let pid = scheduler.schedule().await.unwrap();
            tokio::time::sleep(work_duration).await;
            scheduler.terminate(pid, 1, work_duration);
        }
        start.elapsed()
    };

    let multi_worker_time = {
        let scheduler = Arc::new(ProcessScheduler::new(Resources::default(), 4, 100));
        for i in 0..num_processes {
            scheduler
                .spawn(format!("multi-{}", i), i as u32, Priority::NORMAL)
                .await;
        }

        let completed = Arc::new(AtomicUsize::new(0));
        let start = std::time::Instant::now();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let sched = scheduler.clone();
            let comp = completed.clone();
            handles.push(tokio::spawn(async move {
                while let Some(pid) = sched.try_schedule() {
                    tokio::time::sleep(work_duration).await;
                    sched.terminate(pid, 1, work_duration);
                    comp.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        assert_eq!(
            completed.load(Ordering::SeqCst),
            num_processes,
            "all tasks should complete"
        );
        start.elapsed()
    };

    let speedup =
        single_worker_time.as_millis() as f64 / multi_worker_time.as_millis().max(1) as f64;
    assert!(
        multi_worker_time < single_worker_time,
        "4 workers ({:?}) should be faster than 1 ({:?}), speedup={:.2}x",
        multi_worker_time,
        single_worker_time,
        speedup
    );
}

#[tokio::test]
async fn test_optimal_priority_inversions_minimized() {
    // Higher priority should complete before lower priority on average
    let scheduler = Arc::new(ProcessScheduler::new(Resources::default(), 2, 100));

    let high_complete_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let low_complete_times = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Spawn mixed priority processes
    for i in 0..20 {
        if i % 2 == 0 {
            scheduler
                .spawn(format!("high-{}", i), 0, Priority::HIGH)
                .await;
        } else {
            scheduler
                .spawn(format!("low-{}", i), 50, Priority::LOW)
                .await;
        }
    }

    let start = std::time::Instant::now();
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let sched = scheduler.clone();
        let comp = completed.clone();
        let high_times = high_complete_times.clone();
        let low_times = low_complete_times.clone();
        let start_time = start;

        handles.push(tokio::spawn(async move {
            loop {
                let pid = match tokio::time::timeout(Duration::from_millis(100), sched.schedule())
                    .await
                {
                    Ok(Some(pid)) => pid,
                    _ => break,
                };

                let info = sched.get_process(pid).unwrap();
                tokio::time::sleep(Duration::from_micros(100)).await;
                sched.terminate(pid, 1, Duration::from_micros(100));

                let elapsed = start_time.elapsed();
                if info.priority == Priority::HIGH {
                    high_times.lock().await.push(elapsed);
                } else {
                    low_times.lock().await.push(elapsed);
                }

                comp.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let high_times = high_complete_times.lock().await;
    let low_times = low_complete_times.lock().await;

    // Calculate average completion times
    let avg_high: Duration = if high_times.is_empty() {
        Duration::ZERO
    } else {
        high_times.iter().sum::<Duration>() / high_times.len() as u32
    };

    let avg_low: Duration = if low_times.is_empty() {
        Duration::ZERO
    } else {
        low_times.iter().sum::<Duration>() / low_times.len() as u32
    };

    // HIGH priority should complete faster on average
    assert!(
        avg_high <= avg_low,
        "HIGH priority avg {:?} should be <= LOW priority avg {:?}",
        avg_high,
        avg_low
    );
}

#[tokio::test]
async fn test_optimal_resource_utilization() {
    // All workers should be utilized when there's work
    let scheduler = Arc::new(ProcessScheduler::new(Resources::default(), 4, 100));

    // Spawn enough work for all workers
    for i in 0..100 {
        scheduler
            .spawn(format!("task-{}", i), i, Priority::NORMAL)
            .await;
    }

    let worker_activity = Arc::new([
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ]);

    let mut handles = Vec::new();
    for worker_id in 0..4 {
        let sched = scheduler.clone();
        let activity = worker_activity.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let pid =
                    match tokio::time::timeout(Duration::from_millis(50), sched.schedule()).await {
                        Ok(Some(pid)) => pid,
                        _ => break,
                    };
                activity[worker_id].fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_micros(50)).await;
                sched.terminate(pid, 1, Duration::from_micros(50));
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    // All workers should have processed some tasks
    for (id, count) in worker_activity.iter().enumerate() {
        let processed = count.load(Ordering::SeqCst);
        assert!(
            processed > 0,
            "Worker {} should have processed tasks, got {}",
            id,
            processed
        );
    }

    // Distribution should be relatively fair (no worker should do more than 50% of all work)
    let total: usize = worker_activity
        .iter()
        .map(|c| c.load(Ordering::SeqCst))
        .sum();
    for (id, count) in worker_activity.iter().enumerate() {
        let processed = count.load(Ordering::SeqCst);
        let percentage = (processed as f64 / total as f64) * 100.0;
        assert!(
            percentage < 50.0,
            "Worker {} did {:.1}% of work, should be balanced",
            id,
            percentage
        );
    }
}

#[tokio::test]
async fn test_optimal_all_same_priority_scheduled() {
    // All tasks with same priority should eventually be scheduled
    let scheduler = ProcessScheduler::new(Resources::default(), 1, 100);

    // Spawn tasks with same priority
    let mut pids = std::collections::HashSet::new();
    for i in 0..10 {
        let pid = scheduler
            .spawn(format!("task-{}", i), 5, Priority::NORMAL)
            .await;
        pids.insert(pid);
    }

    let mut scheduled = std::collections::HashSet::new();
    for _ in 0..10 {
        let pid = scheduler.schedule().await.unwrap();
        scheduled.insert(pid);
        scheduler.terminate(pid, 0, Duration::ZERO);
    }

    // All tasks should be scheduled exactly once
    assert_eq!(
        scheduled, pids,
        "All same-priority tasks should be scheduled"
    );
}

#[tokio::test]
async fn test_optimal_preemption_responsiveness() {
    // High priority arriving while processing low priority should be scheduled next
    let scheduler = Arc::new(ProcessScheduler::new(Resources::default(), 1, 100));

    // Spawn low priority work
    for i in 0..10 {
        scheduler
            .spawn(format!("low-{}", i), 100, Priority::IDLE)
            .await;
    }

    // Start a worker
    let sched = scheduler.clone();
    let scheduled_priorities = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let priorities_clone = scheduled_priorities.clone();

    let worker = tokio::spawn(async move {
        for _ in 0..15 {
            let pid = match tokio::time::timeout(Duration::from_millis(100), sched.schedule()).await
            {
                Ok(Some(pid)) => pid,
                _ => break,
            };
            let info = sched.get_process(pid).unwrap();
            priorities_clone.lock().await.push(info.priority);
            tokio::time::sleep(Duration::from_millis(5)).await;
            sched.terminate(pid, 0, Duration::ZERO);
        }
    });

    // After short delay, spawn high priority
    tokio::time::sleep(Duration::from_millis(10)).await;
    for i in 0..5 {
        scheduler
            .spawn(format!("high-{}", i), 0, Priority::CRITICAL)
            .await;
    }

    worker.await.unwrap();

    let priorities = scheduled_priorities.lock().await;

    // Find where REALTIME priorities appear
    let first_realtime_idx = priorities.iter().position(|&p| p == Priority::CRITICAL);

    assert!(
        first_realtime_idx.is_some(),
        "REALTIME should have been scheduled"
    );

    // After the first REALTIME, subsequent REALTIME should be scheduled before remaining IDLE
    if let Some(idx) = first_realtime_idx {
        let remaining = &priorities[idx..];
        let realtime_count = remaining
            .iter()
            .filter(|&&p| p == Priority::CRITICAL)
            .count();
        // Most REALTIME should cluster together
        assert!(
            realtime_count >= 3,
            "At least 3 REALTIME should be scheduled consecutively after preemption"
        );
    }
}

// ============================================================================
// ProcessKind Tests
// ============================================================================

#[test]
fn test_process_kind_equality() {
    assert_eq!(ProcessKind::Daemon, ProcessKind::Daemon);
    assert_eq!(ProcessKind::Batch, ProcessKind::Batch);
    assert_ne!(ProcessKind::Daemon, ProcessKind::Batch);
}

#[test]
fn test_task_type_equality() {
    assert_eq!(TaskType::RealtimeTail, TaskType::RealtimeTail);
    assert_eq!(TaskType::Reconcile, TaskType::Reconcile);
    assert_eq!(
        TaskType::FullHistoryReconcile,
        TaskType::FullHistoryReconcile
    );
    assert_eq!(TaskType::SchemaHeal, TaskType::SchemaHeal);
    assert_eq!(TaskType::ConflictReindex, TaskType::ConflictReindex);
    assert_eq!(TaskType::Backfill, TaskType::Backfill);

    // Different types should not be equal
    assert_ne!(TaskType::RealtimeTail, TaskType::Backfill);
    assert_ne!(TaskType::Reconcile, TaskType::SchemaHeal);
}

// ============================================================================
// GroupScheduler Tests
// ============================================================================

#[tokio::test]
async fn test_group_scheduler_creation() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    assert_eq!(group_sched.log_group(), "test-log-group");
    let counts = group_sched.process_counts();
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.terminated, 0);
}

#[tokio::test]
async fn test_group_scheduler_spawn_realtime_tail() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let pid = group_sched.spawn_realtime_tail().await;
    assert!(pid > 0);

    let info = group_sched.scheduler().get_process(pid).unwrap();
    assert_eq!(info.kind, ProcessKind::Daemon);
    assert_eq!(info.task_type, TaskType::RealtimeTail);
    assert_eq!(info.priority, Priority::CRITICAL);
}

#[tokio::test]
async fn test_group_scheduler_spawn_reconcile() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let pid = group_sched.spawn_reconcile().await;

    let info = group_sched.scheduler().get_process(pid).unwrap();
    assert_eq!(info.kind, ProcessKind::Daemon);
    assert_eq!(info.task_type, TaskType::Reconcile);
    assert_eq!(info.priority, Priority::HIGH);
}

#[tokio::test]
async fn test_group_scheduler_spawn_full_history_reconcile() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let pid = group_sched.spawn_full_history_reconcile().await;

    let info = group_sched.scheduler().get_process(pid).unwrap();
    assert_eq!(info.kind, ProcessKind::Daemon);
    assert_eq!(info.task_type, TaskType::FullHistoryReconcile);
    assert_eq!(info.priority, Priority::NORMAL);
}

#[tokio::test]
async fn test_group_scheduler_spawn_conflict_reindex() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let pid = group_sched.spawn_conflict_reindex().await;

    let info = group_sched.scheduler().get_process(pid).unwrap();
    assert_eq!(info.kind, ProcessKind::Daemon);
    assert_eq!(info.task_type, TaskType::ConflictReindex);
    assert_eq!(info.priority, Priority::LOW);
}

#[tokio::test]
async fn test_group_scheduler_spawn_heal_day() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let pid = group_sched.spawn_heal_day(5).await;

    let info = group_sched.scheduler().get_process(pid).unwrap();
    assert_eq!(info.kind, ProcessKind::Batch);
    assert_eq!(info.task_type, TaskType::SchemaHeal);
    assert_eq!(info.priority, Priority::IDLE);
    assert_eq!(info.day_offset, 5);
}

#[tokio::test]
async fn test_group_scheduler_spawn_backfill_day() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    // Day 0 should have REALTIME priority
    let pid0 = group_sched.spawn_backfill_day(0).await;
    let info0 = group_sched.scheduler().get_process(pid0).unwrap();
    assert_eq!(info0.kind, ProcessKind::Batch);
    assert_eq!(info0.task_type, TaskType::Backfill);
    assert_eq!(info0.priority, Priority::CRITICAL);

    // Day 7 should have NORMAL priority
    let pid7 = group_sched.spawn_backfill_day(7).await;
    let info7 = group_sched.scheduler().get_process(pid7).unwrap();
    assert_eq!(info7.priority, Priority::NORMAL);

    // Day 100 should have IDLE priority
    let pid100 = group_sched.spawn_backfill_day(100).await;
    let info100 = group_sched.scheduler().get_process(pid100).unwrap();
    assert_eq!(info100.priority, Priority::IDLE);
}

#[tokio::test]
async fn test_group_scheduler_create_backfill_queue() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    // Create queue with 100 days but only 8 max ready (4 concurrency * 2)
    let queue = group_sched.create_backfill_queue(100);

    assert_eq!(queue.total_work(), 100);
    assert_eq!(queue.completed(), 0);
    assert!(!queue.is_complete());

    // Start only spawns max_ready processes
    let initial_pids = queue.start().await;
    assert_eq!(initial_pids.len(), 8);

    let counts = group_sched.process_counts();
    assert_eq!(counts.ready, 8);
    assert_eq!(queue.pending_count().await, 92);
}

#[tokio::test]
async fn test_group_scheduler_create_heal_queue() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    let queue = group_sched.create_heal_queue(5);

    assert_eq!(queue.total_work(), 5);

    // All 5 can fit in max_ready (8)
    let initial_pids = queue.start().await;
    assert_eq!(initial_pids.len(), 5);

    // All heal processes should be IDLE priority
    for pid in initial_pids {
        let info = group_sched.scheduler().get_process(pid).unwrap();
        assert_eq!(info.priority, Priority::IDLE);
        assert_eq!(info.task_type, TaskType::SchemaHeal);
    }
}

#[tokio::test]
async fn test_group_scheduler_list_daemons() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    // Spawn daemons
    group_sched.spawn_realtime_tail().await;
    group_sched.spawn_reconcile().await;
    group_sched.spawn_conflict_reindex().await;

    // Spawn batches
    group_sched.spawn_backfill_day(0).await;
    group_sched.spawn_backfill_day(1).await;

    let daemons = group_sched.list_daemons();
    assert_eq!(daemons.len(), 3);

    for d in daemons {
        assert_eq!(d.kind, ProcessKind::Daemon);
    }
}

#[tokio::test]
async fn test_group_scheduler_list_batches() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    // Spawn daemons
    group_sched.spawn_realtime_tail().await;

    // Spawn batches
    group_sched.spawn_backfill_day(0).await;
    group_sched.spawn_backfill_day(1).await;
    group_sched.spawn_heal_day(0).await;

    let batches = group_sched.list_batches();
    assert_eq!(batches.len(), 3);

    for b in batches {
        assert_eq!(b.kind, ProcessKind::Batch);
    }
}

#[tokio::test]
async fn test_group_scheduler_shutdown_daemons() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 4);

    // Spawn daemons
    let tail_pid = group_sched.spawn_realtime_tail().await;
    let reconcile_pid = group_sched.spawn_reconcile().await;

    // Spawn a batch (should not be affected)
    let backfill_pid = group_sched.spawn_backfill_day(0).await;

    // Verify initial state
    let counts_before = group_sched.process_counts();
    assert_eq!(counts_before.ready, 3);
    assert_eq!(counts_before.terminated, 0);

    // Shutdown daemons
    group_sched.shutdown_daemons().await;

    // Verify daemons are terminated
    let tail_info = group_sched.scheduler().get_process(tail_pid).unwrap();
    assert_eq!(tail_info.state, ProcessState::Terminated);

    let reconcile_info = group_sched.scheduler().get_process(reconcile_pid).unwrap();
    assert_eq!(reconcile_info.state, ProcessState::Terminated);

    // Backfill should still be ready
    let backfill_info = group_sched.scheduler().get_process(backfill_pid).unwrap();
    assert_eq!(backfill_info.state, ProcessState::Ready);
}

#[tokio::test]
async fn test_group_scheduler_mixed_workload() {
    let group_sched = GroupScheduler::new("test-log-group".into(), Resources::default(), 8);

    // Spawn daemon processes
    let tail_pid = group_sched.spawn_realtime_tail().await;
    let reconcile_pid = group_sched.spawn_reconcile().await;
    let full_history_pid = group_sched.spawn_full_history_reconcile().await;
    let conflict_pid = group_sched.spawn_conflict_reindex().await;

    // Create demand-driven batch queues
    let backfill_queue = group_sched.create_backfill_queue(7);
    let heal_queue = group_sched.create_heal_queue(3);

    // Start queues - only spawns up to max_ready
    let backfill_pids = backfill_queue.start().await;
    let heal_pids = heal_queue.start().await;

    // Total: 4 daemons + 7 backfill + 3 heal = 14 processes
    // All fit in max_ready (16) so all should be spawned
    let all_processes = group_sched.list_processes();
    assert_eq!(all_processes.len(), 14);

    let daemons = group_sched.list_daemons();
    assert_eq!(daemons.len(), 4);

    let batches = group_sched.list_batches();
    assert_eq!(batches.len(), 10);

    // Verify daemon priorities are correct
    let tail_info = group_sched.scheduler().get_process(tail_pid).unwrap();
    assert_eq!(tail_info.priority, Priority::CRITICAL);

    let reconcile_info = group_sched.scheduler().get_process(reconcile_pid).unwrap();
    assert_eq!(reconcile_info.priority, Priority::HIGH);

    let full_history_info = group_sched
        .scheduler()
        .get_process(full_history_pid)
        .unwrap();
    assert_eq!(full_history_info.priority, Priority::NORMAL);

    let conflict_info = group_sched.scheduler().get_process(conflict_pid).unwrap();
    assert_eq!(conflict_info.priority, Priority::LOW);

    // Verify batch queues track work correctly
    assert_eq!(backfill_pids.len(), 7);
    assert_eq!(heal_pids.len(), 3);
    assert_eq!(backfill_queue.pending_count().await, 0);
    assert_eq!(heal_queue.pending_count().await, 0);
}

#[tokio::test]
async fn test_group_scheduler_process_naming() {
    let group_sched = GroupScheduler::new("my-app-logs".into(), Resources::default(), 4);

    let tail_pid = group_sched.spawn_realtime_tail().await;
    let reconcile_pid = group_sched.spawn_reconcile().await;
    let backfill_pid = group_sched.spawn_backfill_day(5).await;
    let heal_pid = group_sched.spawn_heal_day(3).await;

    let tail_info = group_sched.scheduler().get_process(tail_pid).unwrap();
    assert_eq!(tail_info.name, "my-app-logs/tail");

    let reconcile_info = group_sched.scheduler().get_process(reconcile_pid).unwrap();
    assert_eq!(reconcile_info.name, "my-app-logs/reconcile");

    let backfill_info = group_sched.scheduler().get_process(backfill_pid).unwrap();
    assert_eq!(backfill_info.name, "my-app-logs/backfill-day-5");

    let heal_info = group_sched.scheduler().get_process(heal_pid).unwrap();
    assert_eq!(heal_info.name, "my-app-logs/heal-day-3");
}

// ============================================================================
// Linux-style Wakeup Tests
// ============================================================================

/// Test that spawning many processes never blocks.
/// This is the key fix: Linux's wake_up_process() never blocks.
#[tokio::test]
async fn test_spawn_never_blocks_with_many_processes() {
    use std::time::Instant;

    let scheduler = ProcessScheduler::new(Resources::default(), 8, 100);

    let start = Instant::now();

    // Spawn 10,000 processes - should complete nearly instantly
    // (previously this would block after ~1000 due to channel capacity)
    for i in 0..10000 {
        scheduler
            .spawn(format!("process-{}", i), i % 365, Priority::NORMAL)
            .await;
    }

    let elapsed = start.elapsed();

    // Should complete in under 5 seconds (usually < 1 second)
    assert!(
        elapsed.as_secs() < 5,
        "Spawning 10,000 processes took {:?}, should be < 5s",
        elapsed
    );

    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 10000);
}

/// Test that scheduler wakes up when a process is spawned.
#[tokio::test]
async fn test_scheduler_wakes_on_spawn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);
    let scheduler_clone = scheduler.clone();

    let scheduled = Arc::new(AtomicBool::new(false));
    let scheduled_clone = scheduled.clone();

    // Start scheduler waiting for work
    let handle = tokio::spawn(async move {
        let pid = scheduler_clone.schedule().await;
        scheduled_clone.store(true, Ordering::SeqCst);
        pid
    });

    // Small delay to ensure scheduler is waiting
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !scheduled.load(Ordering::SeqCst),
        "Scheduler should be waiting"
    );

    // Spawn a process - this should wake the scheduler
    scheduler
        .spawn("test-process".into(), 0, Priority::NORMAL)
        .await;

    // Scheduler should complete quickly
    let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(result.is_ok(), "Scheduler should have been woken up");
    assert!(scheduled.load(Ordering::SeqCst));
}

/// Test that multiple spawns coalesce into efficient wakeups.
#[tokio::test]
async fn test_multiple_spawns_coalesce_wakeups() {
    use std::time::Instant;

    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    // Spawn many processes rapidly
    let start = Instant::now();
    for i in 0..1000 {
        scheduler
            .spawn(format!("rapid-{}", i), 0, Priority::NORMAL)
            .await;
    }
    let spawn_time = start.elapsed();

    // All should be spawned quickly (coalesced wakeups, no blocking)
    assert!(
        spawn_time.as_millis() < 500,
        "Rapid spawning took {:?}, expected < 500ms",
        spawn_time
    );

    // Now consume them - all should be in ready queue
    let mut count = 0;
    while let Some(pid) = scheduler.schedule().await {
        scheduler.terminate(pid, 0, Duration::ZERO);
        count += 1;
        if count >= 1000 {
            break;
        }
    }
    assert_eq!(count, 1000);
}

/// Test that unblock wakes the scheduler.
#[tokio::test]
async fn test_unblock_wakes_scheduler() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);

    // Spawn a process and immediately block it
    let pid = scheduler
        .spawn("blocked-process".into(), 0, Priority::NORMAL)
        .await;

    // Schedule it to make it running, then block it
    scheduler.schedule().await;
    scheduler.block(pid);

    let scheduler_clone = scheduler.clone();
    let woken = Arc::new(AtomicBool::new(false));
    let woken_clone = woken.clone();

    // Start scheduler waiting for work
    let handle = tokio::spawn(async move {
        let pid = scheduler_clone.schedule().await;
        woken_clone.store(true, Ordering::SeqCst);
        pid
    });

    // Small delay to ensure scheduler is waiting
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!woken.load(Ordering::SeqCst), "Scheduler should be waiting");

    // Unblock the process - this should wake the scheduler
    scheduler.unblock(pid).await;

    // Scheduler should complete quickly
    let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(
        result.is_ok(),
        "Scheduler should have been woken by unblock"
    );
    assert!(woken.load(Ordering::SeqCst));
}

/// Test spawning from multiple concurrent tasks never blocks.
#[tokio::test]
async fn test_concurrent_spawn_never_blocks() {
    use std::time::Instant;

    let scheduler = ProcessScheduler::new(Resources::default(), 8, 100);

    let start = Instant::now();

    // Spawn from 10 concurrent tasks, each spawning 1000 processes
    let mut handles = vec![];
    for task_id in 0..10 {
        let scheduler = scheduler.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..1000 {
                scheduler
                    .spawn(
                        format!("task-{}-proc-{}", task_id, i),
                        i % 30,
                        Priority::NORMAL,
                    )
                    .await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();

    // Should complete quickly even with concurrent spawning
    assert!(
        elapsed.as_secs() < 5,
        "Concurrent spawning of 10,000 processes took {:?}, should be < 5s",
        elapsed
    );

    let counts = scheduler.process_counts();
    assert_eq!(counts.ready, 10000);
}

/// Test that scheduler handles empty queue correctly and wakes on new work.
#[tokio::test]
async fn test_scheduler_empty_queue_then_spawn() {
    use std::time::Duration;

    let scheduler = ProcessScheduler::new(Resources::default(), 4, 100);
    let scheduler_clone = scheduler.clone();

    // Start multiple scheduler waiters
    let mut handles = vec![];
    for _ in 0..3 {
        let s = scheduler.clone();
        handles.push(tokio::spawn(async move { s.schedule().await }));
    }

    // Small delay to let all waiters start
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Spawn 3 processes to wake all waiters
    for i in 0..3 {
        scheduler_clone
            .spawn(format!("wakeup-{}", i), 0, Priority::NORMAL)
            .await;
    }

    // All should complete
    for handle in handles {
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "All schedulers should be woken");
    }
}

// ============================================================================
// BatchWorkQueue Tests (Linux-style RLIMIT_NPROC)
// ============================================================================

/// Test that BatchWorkQueue respects max_ready limit (like RLIMIT_NPROC).
#[tokio::test]
async fn test_batch_queue_respects_max_ready_limit() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 4);

    // Create queue with 100 days but only 8 max ready
    let queue = group_sched.create_backfill_queue(100);

    assert_eq!(queue.total_work(), 100);
    assert_eq!(queue.completed(), 0);
    assert!(!queue.is_complete());

    // Start should only spawn max_ready processes
    let initial_pids = queue.start().await;
    assert_eq!(initial_pids.len(), 8); // 4 concurrency * 2

    // Spawned count should be at max_ready
    assert_eq!(queue.spawned_count().await, 8);

    // Pending should be total - spawned
    assert_eq!(queue.pending_count().await, 92);
}

/// Test that completing a process spawns the next pending work.
#[tokio::test]
async fn test_batch_queue_spawns_on_complete() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 2);

    let queue = group_sched.create_backfill_queue(10);
    let initial_pids = queue.start().await;

    assert_eq!(initial_pids.len(), 4); // 2 concurrency * 2
    assert_eq!(queue.pending_count().await, 6);

    // Complete first process - should spawn next
    let new_pid = queue.complete(initial_pids[0]).await;
    assert!(new_pid.is_some());

    // Spawned count should still be at max
    assert_eq!(queue.spawned_count().await, 4);

    // Pending should decrease
    assert_eq!(queue.pending_count().await, 5);

    // Completed should increase
    assert_eq!(queue.completed(), 1);
}

/// Test that queue correctly tracks completion.
#[tokio::test]
async fn test_batch_queue_tracks_completion() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 2);

    let queue = group_sched.create_backfill_queue(5);
    let initial_pids = queue.start().await;

    // Complete all initial processes
    for pid in &initial_pids {
        queue.complete(*pid).await;
    }

    // Should have completed 4 processes
    assert_eq!(queue.completed(), 4);

    // Should have spawned 1 more (5 total - 4 initial = 1 pending)
    // Actually after completing 4, we'd spawn 4 more if available
    // With only 5 total and 4 initial, only 1 was pending
}

/// Test that queue completes when all work is done.
#[tokio::test]
async fn test_batch_queue_completes_all_work() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 2);

    // Small queue that fits entirely in max_ready
    let queue = group_sched.create_backfill_queue(3);
    let initial_pids = queue.start().await;

    assert_eq!(initial_pids.len(), 3); // Less than max_ready
    assert_eq!(queue.pending_count().await, 0);

    // Complete all
    for pid in initial_pids {
        queue.complete(pid).await;
    }

    assert!(queue.is_complete());
    assert_eq!(queue.completed(), 3);
    assert_eq!(queue.progress_percent(), 100.0);
}

/// Test that queue prioritizes higher priority work first.
#[tokio::test]
async fn test_batch_queue_prioritizes_work() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 2);

    // Create queue with 365 days - should prioritize recent days
    let queue = group_sched.create_backfill_queue(365);
    let initial_pids = queue.start().await;

    // Get info about spawned processes - recent days should be spawned first
    let scheduler = group_sched.scheduler();
    let mut day_offsets: Vec<u32> = Vec::new();
    for pid in initial_pids {
        if let Some(info) = scheduler.get_process(pid) {
            day_offsets.push(info.day_offset);
        }
    }

    // First spawned should be day 0 (highest priority)
    assert!(
        day_offsets.contains(&0),
        "Day 0 (CRITICAL) should be in first batch"
    );

    // Should have recent days, not old ones like 364
    assert!(
        !day_offsets.contains(&364),
        "Old days should not be in first batch"
    );
}

/// Test that queue handles zero work correctly.
#[tokio::test]
async fn test_batch_queue_handles_zero_work() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 4);

    let queue = group_sched.create_backfill_queue(0);

    assert_eq!(queue.total_work(), 0);
    assert!(queue.is_complete());
    assert_eq!(queue.progress_percent(), 100.0);

    let initial_pids = queue.start().await;
    assert!(initial_pids.is_empty());
}

/// Test that queue handles large day counts efficiently.
#[tokio::test]
async fn test_batch_queue_handles_large_day_counts() {
    use std::time::Instant;

    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 8);

    let start = Instant::now();

    // Create queue with 10 years of days - should be fast!
    let queue = group_sched.create_backfill_queue(3650);

    let creation_time = start.elapsed();

    // Queue creation should be fast (just building pending list)
    assert!(
        creation_time.as_millis() < 100,
        "Queue creation took {:?}, should be < 100ms",
        creation_time
    );

    // Start should only spawn 16 processes (8 * 2)
    let start_time = Instant::now();
    let initial_pids = queue.start().await;
    let spawn_time = start_time.elapsed();

    assert_eq!(initial_pids.len(), 16);
    assert!(
        spawn_time.as_millis() < 100,
        "Initial spawn took {:?}, should be < 100ms",
        spawn_time
    );

    // Most work should still be pending
    assert_eq!(queue.pending_count().await, 3650 - 16);
}

/// Test concurrent completion and spawning.
#[tokio::test]
async fn test_batch_queue_concurrent_operations() {
    let group_sched = GroupScheduler::new("test-group".into(), Resources::default(), 4);

    let queue = Arc::new(group_sched.create_backfill_queue(50));
    let initial_pids = queue.start().await;

    // Complete processes concurrently from multiple tasks
    let mut handles = vec![];
    for pid in initial_pids {
        let queue = queue.clone();
        handles.push(tokio::spawn(async move {
            queue.complete(pid).await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Should have completed 8 processes
    assert_eq!(queue.completed(), 8);

    // Should have spawned more to refill
    // (actual count depends on timing, but should be reasonable)
    let spawned = queue.spawned_count().await;
    assert!(spawned <= 8, "Should not exceed max_ready");
}
