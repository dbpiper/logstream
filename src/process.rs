//! OS-style process scheduler for all processing tasks.
//! Models tasks as processes with resources, priorities, states, and fair scheduling.
//! Supports both long-running daemon processes (tail, reconcile) and batch processes (backfill).

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, RwLock, Semaphore};

// ============================================================================
// Priority System
// ============================================================================

/// Priority levels (higher number = higher priority).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(pub u8);

impl Priority {
    /// Critical: real-time tail events - never delayed
    pub const CRITICAL: Priority = Priority(255);
    /// High: recent reconciliation
    pub const HIGH: Priority = Priority(192);
    /// Normal: standard backfill for today
    pub const NORMAL: Priority = Priority(128);
    /// Low: historical backfill (last week)
    pub const LOW: Priority = Priority(64);
    /// Idle: old backfill, healing - only when nothing else
    pub const IDLE: Priority = Priority(0);

    /// Get the raw priority value.
    pub fn value(&self) -> u8 {
        self.0
    }

    /// Calculate effective priority including aging.
    /// Processes waiting longer get priority boost (higher value = higher priority).
    /// Boosts by 1 priority unit per 10 seconds waiting, capped at CRITICAL.
    pub fn effective_priority(&self, wait_time: Duration) -> u8 {
        let base = self.0 as u16;
        // Aging: increase priority by 1 per 10 seconds waiting
        let aging_boost = (wait_time.as_secs() / 10) as u16;
        (base.saturating_add(aging_boost)).min(255) as u8
    }
}

impl Default for Priority {
    fn default() -> Self {
        Priority::NORMAL
    }
}

/// Determine the priority for a given day offset.
/// - Day 0 (today): CRITICAL priority (real-time backfill)
/// - Days 1-3: HIGH priority
/// - Days 4-7: NORMAL priority
/// - Days 8-30: LOW priority
/// - Days 31+: IDLE priority
pub fn priority_for_day_offset(day_offset: u32) -> Priority {
    match day_offset {
        0 => Priority::CRITICAL,
        1..=3 => Priority::HIGH,
        4..=7 => Priority::NORMAL,
        8..=30 => Priority::LOW,
        _ => Priority::IDLE,
    }
}

// ============================================================================
// Process Kind
// ============================================================================

/// Distinguishes daemon (long-running) from batch (one-shot) processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessKind {
    /// Long-running daemon process (e.g., real-time tail, reconcile loop).
    /// Never terminates naturally; must be explicitly stopped.
    Daemon,
    /// One-shot batch process (e.g., backfill day, heal day).
    /// Terminates when work is complete.
    Batch,
}

/// Type of processing task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskType {
    /// Real-time log tailing (CRITICAL priority).
    RealtimeTail,
    /// Rolling reconciliation (HIGH priority).
    Reconcile,
    /// Full history reconciliation (NORMAL priority).
    FullHistoryReconcile,
    /// Schema healing (IDLE priority).
    SchemaHeal,
    /// Conflict reindexing (LOW priority).
    ConflictReindex,
    /// Historical backfill (priority by age).
    Backfill,
}

// ============================================================================
// Resource Types
// ============================================================================

/// System resources that processes compete for.
#[derive(Debug, Clone)]
pub struct Resources {
    /// CloudWatch API quota (requests available).
    pub cw_api_quota: usize,
    /// Elasticsearch bulk capacity (events per batch).
    pub es_bulk_capacity: usize,
    /// Memory quota (buffered events).
    pub memory_quota: usize,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            cw_api_quota: 100,
            es_bulk_capacity: 50000,
            memory_quota: 1_000_000,
        }
    }
}

/// Shared resource pool with atomic tracking.
#[derive(Debug)]
pub struct ResourcePool {
    /// Available CloudWatch API calls.
    cw_available: AtomicUsize,
    /// Available ES bulk slots.
    es_available: AtomicUsize,
    /// Available memory slots.
    memory_available: AtomicUsize,
    /// Semaphore for concurrency control.
    concurrency: Semaphore,
    /// Max concurrent processes.
    max_concurrency: usize,
}

impl ResourcePool {
    pub fn new(resources: Resources, max_concurrency: usize) -> Self {
        Self {
            cw_available: AtomicUsize::new(resources.cw_api_quota),
            es_available: AtomicUsize::new(resources.es_bulk_capacity),
            memory_available: AtomicUsize::new(resources.memory_quota),
            concurrency: Semaphore::new(max_concurrency),
            max_concurrency,
        }
    }

    /// Try to acquire resources for a process.
    pub fn try_acquire(&self, request: &ResourceRequest) -> Option<ResourceGrant> {
        // Check if all resources are available
        let cw = self.cw_available.load(Ordering::Relaxed);
        let es = self.es_available.load(Ordering::Relaxed);
        let mem = self.memory_available.load(Ordering::Relaxed);

        if cw < request.cw_api_calls || es < request.es_events || mem < request.memory_events {
            return None;
        }

        // Atomically decrement all resources
        self.cw_available
            .fetch_sub(request.cw_api_calls, Ordering::Relaxed);
        self.es_available
            .fetch_sub(request.es_events, Ordering::Relaxed);
        self.memory_available
            .fetch_sub(request.memory_events, Ordering::Relaxed);

        Some(ResourceGrant {
            cw_api_calls: request.cw_api_calls,
            es_events: request.es_events,
            memory_events: request.memory_events,
        })
    }

    /// Release resources back to the pool.
    pub fn release(&self, grant: ResourceGrant) {
        self.cw_available
            .fetch_add(grant.cw_api_calls, Ordering::Relaxed);
        self.es_available
            .fetch_add(grant.es_events, Ordering::Relaxed);
        self.memory_available
            .fetch_add(grant.memory_events, Ordering::Relaxed);
    }

    /// Acquire a concurrency slot.
    pub async fn acquire_slot(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.concurrency.acquire().await.unwrap()
    }

    /// Current resource utilization (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        let cw_used = self
            .max_concurrency
            .saturating_sub(self.cw_available.load(Ordering::Relaxed));
        cw_used as f64 / self.max_concurrency as f64
    }

    /// Get maximum concurrency (like Linux's RLIMIT_NPROC).
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

/// Resource request from a process.
#[derive(Debug, Clone, Default)]
pub struct ResourceRequest {
    pub cw_api_calls: usize,
    pub es_events: usize,
    pub memory_events: usize,
}

/// Granted resources (must be released).
#[derive(Debug)]
pub struct ResourceGrant {
    pub cw_api_calls: usize,
    pub es_events: usize,
    pub memory_events: usize,
}

// ============================================================================
// Process States (like OS process states)
// ============================================================================

/// Process state machine (mirrors OS process states).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// New: Just created, not yet admitted.
    New,
    /// Ready: Waiting in run queue, can be scheduled.
    Ready,
    /// Running: Currently executing on a "CPU" (worker).
    Running,
    /// Blocked: Waiting for I/O or resources.
    Blocked,
    /// Terminated: Completed execution.
    Terminated,
}

// ============================================================================
// Process Control Block (PCB)
// ============================================================================

/// Process Control Block - contains all process metadata.
#[derive(Debug)]
pub struct ProcessControlBlock {
    /// Unique process ID.
    pub pid: u64,
    /// Human-readable name.
    pub name: String,
    /// Current state.
    pub state: ProcessState,
    /// Priority level.
    pub priority: Priority,
    /// Process kind (daemon or batch).
    pub kind: ProcessKind,
    /// Task type.
    pub task_type: TaskType,
    /// Time when process was created.
    pub created_at: Instant,
    /// Time when process entered ready queue.
    pub ready_at: Option<Instant>,
    /// Time when process started running.
    pub started_at: Option<Instant>,
    /// Total CPU time used.
    pub cpu_time: Duration,
    /// Total events processed.
    pub events_processed: usize,
    /// Parent process ID (if any).
    pub parent_pid: Option<u64>,
    /// Day offset this process is handling (0 for daemons).
    pub day_offset: u32,
    /// Resource request for this process.
    pub resource_request: ResourceRequest,
}

impl ProcessControlBlock {
    pub fn new(pid: u64, name: String, day_offset: u32, priority: Priority) -> Self {
        Self {
            pid,
            name,
            state: ProcessState::New,
            priority,
            kind: ProcessKind::Batch,
            task_type: TaskType::Backfill,
            created_at: Instant::now(),
            ready_at: None,
            started_at: None,
            cpu_time: Duration::ZERO,
            events_processed: 0,
            parent_pid: None,
            day_offset,
            resource_request: ResourceRequest::default(),
        }
    }

    pub fn new_daemon(pid: u64, name: String, priority: Priority, task_type: TaskType) -> Self {
        Self {
            pid,
            name,
            state: ProcessState::New,
            priority,
            kind: ProcessKind::Daemon,
            task_type,
            created_at: Instant::now(),
            ready_at: None,
            started_at: None,
            cpu_time: Duration::ZERO,
            events_processed: 0,
            parent_pid: None,
            day_offset: 0,
            resource_request: ResourceRequest::default(),
        }
    }

    pub fn new_batch(
        pid: u64,
        name: String,
        day_offset: u32,
        priority: Priority,
        task_type: TaskType,
    ) -> Self {
        Self {
            pid,
            name,
            state: ProcessState::New,
            priority,
            kind: ProcessKind::Batch,
            task_type,
            created_at: Instant::now(),
            ready_at: None,
            started_at: None,
            cpu_time: Duration::ZERO,
            events_processed: 0,
            parent_pid: None,
            day_offset,
            resource_request: ResourceRequest::default(),
        }
    }

    /// Wait time in ready queue.
    pub fn wait_time(&self) -> Duration {
        self.ready_at.map(|t| t.elapsed()).unwrap_or(Duration::ZERO)
    }

    /// Effective priority with aging.
    pub fn effective_priority(&self) -> u8 {
        self.priority.effective_priority(self.wait_time())
    }

    /// Transition to ready state.
    pub fn make_ready(&mut self) {
        self.state = ProcessState::Ready;
        self.ready_at = Some(Instant::now());
    }

    /// Transition to running state.
    pub fn make_running(&mut self) {
        self.state = ProcessState::Running;
        self.started_at = Some(Instant::now());
    }

    /// Transition to blocked state.
    pub fn make_blocked(&mut self) {
        self.state = ProcessState::Blocked;
    }

    /// Transition to terminated state.
    pub fn make_terminated(&mut self) {
        self.state = ProcessState::Terminated;
    }
}

// ============================================================================
// Scheduler (Multi-Level Feedback Queue)
// ============================================================================

/// Entry in the priority queue.
struct SchedulerEntry {
    effective_priority: u8,
    pid: u64,
}

impl Ord for SchedulerEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher effective priority value = higher actual priority
        self.effective_priority.cmp(&other.effective_priority)
    }
}

impl PartialOrd for SchedulerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SchedulerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.effective_priority == other.effective_priority && self.pid == other.pid
    }
}

impl Eq for SchedulerEntry {}

/// Multi-level feedback queue scheduler.
///
/// Uses Linux-style wakeup semantics:
/// - `spawn()` never blocks - like `wake_up_process()` setting TIF_NEED_RESCHED
/// - `schedule()` waits efficiently on a Notify - like sleeping on a wait queue
/// - Multiple wakeups are coalesced - like the need_resched flag
pub struct ProcessScheduler {
    /// Next PID to assign.
    next_pid: AtomicU64,
    /// All processes (keyed by PID).
    processes: RwLock<std::collections::HashMap<u64, ProcessControlBlock>>,
    /// Ready queue (priority queue).
    ready_queue: Mutex<BinaryHeap<SchedulerEntry>>,
    /// Resource pool.
    resources: Arc<ResourcePool>,
    /// Time quantum for round-robin (ms).
    time_quantum_ms: u64,
    /// Wakeup signal for scheduler (like Linux's TIF_NEED_RESCHED).
    /// Never blocks on signal, multiple signals coalesce.
    wakeup: Notify,
}

impl ProcessScheduler {
    pub fn new(resources: Resources, max_concurrency: usize, time_quantum_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            next_pid: AtomicU64::new(1),
            processes: RwLock::new(std::collections::HashMap::new()),
            ready_queue: Mutex::new(BinaryHeap::new()),
            resources: Arc::new(ResourcePool::new(resources, max_concurrency)),
            time_quantum_ms,
            wakeup: Notify::new(),
        })
    }

    /// Create a new batch process and add to ready queue.
    pub async fn spawn(&self, name: String, day_offset: u32, priority: Priority) -> u64 {
        self.spawn_batch(name, day_offset, priority, TaskType::Backfill)
            .await
    }

    /// Create a daemon process (long-running).
    pub async fn spawn_daemon(&self, name: String, priority: Priority, task_type: TaskType) -> u64 {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
        let mut pcb = ProcessControlBlock::new_daemon(pid, name, priority, task_type);
        pcb.make_ready();

        let effective_priority = pcb.effective_priority();

        {
            let mut processes = self.processes.write().await;
            processes.insert(pid, pcb);
        }

        {
            let mut queue = self.ready_queue.lock().await;
            queue.push(SchedulerEntry {
                effective_priority,
                pid,
            });
        }

        // Linux-style wakeup: notify_one() never blocks, just sets the
        // "need_resched" equivalent. Multiple calls coalesce into one wakeup.
        self.wakeup.notify_one();
        pid
    }

    /// Create a batch process (one-shot) and add to ready queue.
    pub async fn spawn_batch(
        &self,
        name: String,
        day_offset: u32,
        priority: Priority,
        task_type: TaskType,
    ) -> u64 {
        let pid = self.next_pid.fetch_add(1, Ordering::SeqCst);
        let mut pcb = ProcessControlBlock::new_batch(pid, name, day_offset, priority, task_type);
        pcb.make_ready();

        let effective_priority = pcb.effective_priority();

        {
            let mut processes = self.processes.write().await;
            processes.insert(pid, pcb);
        }

        {
            let mut queue = self.ready_queue.lock().await;
            queue.push(SchedulerEntry {
                effective_priority,
                pid,
            });
        }

        // Linux-style wakeup: notify_one() never blocks, just sets the
        // "need_resched" equivalent. Multiple calls coalesce into one wakeup.
        self.wakeup.notify_one();
        pid
    }

    /// Get the next process to run (blocks until one is available).
    ///
    /// Uses Linux-style wait queue semantics:
    /// - First checks if work is available (like checking TIF_NEED_RESCHED)
    /// - If not, sleeps on the wakeup signal (like sleeping on a wait queue)
    /// - Wakes up when any process is spawned or unblocked
    pub async fn schedule(&self) -> Option<u64> {
        loop {
            // Try to get from ready queue (like checking need_resched)
            {
                let mut queue = self.ready_queue.lock().await;
                if let Some(entry) = queue.pop() {
                    // Update process state
                    let mut processes = self.processes.write().await;
                    if let Some(pcb) = processes.get_mut(&entry.pid) {
                        if pcb.state == ProcessState::Ready {
                            pcb.make_running();
                            return Some(entry.pid);
                        }
                    }
                    // Process was terminated or in wrong state, try next
                    continue;
                }
            }

            // Sleep on wait queue until woken by spawn/unblock
            // Like Linux's schedule() sleeping when no runnable tasks
            self.wakeup.notified().await;
        }
    }

    /// Mark a process as completed.
    pub async fn terminate(&self, pid: u64, events_processed: usize, cpu_time: Duration) {
        let mut processes = self.processes.write().await;
        if let Some(pcb) = processes.get_mut(&pid) {
            pcb.events_processed = events_processed;
            pcb.cpu_time = cpu_time;
            pcb.make_terminated();
        }
    }

    /// Mark a process as blocked (waiting for I/O).
    pub async fn block(&self, pid: u64) {
        let mut processes = self.processes.write().await;
        if let Some(pcb) = processes.get_mut(&pid) {
            pcb.make_blocked();
        }
    }

    /// Unblock a process and return to ready queue.
    ///
    /// Like Linux's `wake_up_process()` - moves process to runnable state
    /// and signals the scheduler.
    pub async fn unblock(&self, pid: u64) {
        let effective_priority;
        {
            let mut processes = self.processes.write().await;
            if let Some(pcb) = processes.get_mut(&pid) {
                pcb.make_ready();
                effective_priority = pcb.effective_priority();
            } else {
                return;
            }
        }

        {
            let mut queue = self.ready_queue.lock().await;
            queue.push(SchedulerEntry {
                effective_priority,
                pid,
            });
        }

        // Linux-style wakeup
        self.wakeup.notify_one();
    }

    /// Get process info.
    pub async fn get_process(&self, pid: u64) -> Option<ProcessInfo> {
        let processes = self.processes.read().await;
        processes.get(&pid).map(|pcb| ProcessInfo {
            pid: pcb.pid,
            name: pcb.name.clone(),
            state: pcb.state,
            priority: pcb.priority,
            kind: pcb.kind,
            task_type: pcb.task_type,
            day_offset: pcb.day_offset,
            wait_time: pcb.wait_time(),
            cpu_time: pcb.cpu_time,
            events_processed: pcb.events_processed,
        })
    }

    /// Get all processes.
    pub async fn list_processes(&self) -> Vec<ProcessInfo> {
        let processes = self.processes.read().await;
        processes
            .values()
            .map(|pcb| ProcessInfo {
                pid: pcb.pid,
                name: pcb.name.clone(),
                state: pcb.state,
                priority: pcb.priority,
                kind: pcb.kind,
                task_type: pcb.task_type,
                day_offset: pcb.day_offset,
                wait_time: pcb.wait_time(),
                cpu_time: pcb.cpu_time,
                events_processed: pcb.events_processed,
            })
            .collect()
    }

    /// Get resource pool.
    pub fn resources(&self) -> &Arc<ResourcePool> {
        &self.resources
    }

    /// Get maximum concurrency (like Linux's RLIMIT_NPROC).
    pub fn max_concurrency(&self) -> usize {
        self.resources.max_concurrency()
    }

    /// Get time quantum.
    pub fn time_quantum(&self) -> Duration {
        Duration::from_millis(self.time_quantum_ms)
    }

    /// Count processes by state.
    pub async fn process_counts(&self) -> ProcessCounts {
        let processes = self.processes.read().await;
        let mut counts = ProcessCounts::default();
        for pcb in processes.values() {
            match pcb.state {
                ProcessState::New => counts.new += 1,
                ProcessState::Ready => counts.ready += 1,
                ProcessState::Running => counts.running += 1,
                ProcessState::Blocked => counts.blocked += 1,
                ProcessState::Terminated => counts.terminated += 1,
            }
        }
        counts
    }
}

/// Summary info about a process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u64,
    pub name: String,
    pub state: ProcessState,
    pub priority: Priority,
    pub kind: ProcessKind,
    pub task_type: TaskType,
    pub day_offset: u32,
    pub wait_time: Duration,
    pub cpu_time: Duration,
    pub events_processed: usize,
}

/// Count of processes by state.
#[derive(Debug, Clone, Default)]
pub struct ProcessCounts {
    pub new: usize,
    pub ready: usize,
    pub running: usize,
    pub blocked: usize,
    pub terminated: usize,
}

// ============================================================================
// Worker Pool - Maps processes to OS threads
// ============================================================================

/// Task type for worker execution.
pub type TaskFn = Box<dyn FnOnce() -> usize + Send + 'static>;

/// Async task type for worker execution.
pub type AsyncTaskFn =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = usize> + Send>> + Send>;

/// Worker pool that maps processes to dedicated OS threads.
/// Uses tokio's blocking thread pool for CPU-bound work and
/// the async runtime for I/O-bound work.
pub struct WorkerPool {
    /// Number of worker threads.
    num_workers: usize,
    /// Scheduler reference.
    scheduler: Arc<ProcessScheduler>,
    /// Shutdown signal.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl WorkerPool {
    /// Create a new worker pool.
    pub fn new(scheduler: Arc<ProcessScheduler>, num_workers: usize) -> Self {
        Self {
            num_workers,
            scheduler,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Get the number of workers.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check if shutdown was signaled.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Run CPU-bound work on a dedicated OS thread.
    /// This uses tokio's spawn_blocking to run on a real OS thread.
    pub async fn run_cpu_bound<F, R>(f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .expect("spawn_blocking failed")
    }

    /// Get optimal worker count based on CPU cores.
    pub fn optimal_worker_count() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
    }

    /// Spawn workers with demand-driven spawning (Linux-style).
    ///
    /// Uses a BatchWorkQueue to only keep max_ready processes in the ready
    /// queue at a time, spawning more as workers complete work.
    ///
    /// This mimics Linux's process management where:
    /// - RLIMIT_NPROC limits concurrent processes
    /// - New processes are only created when slots are available
    /// - fork() fails with EAGAIN when at limit
    ///
    /// The optional `pause_check` function is called before each task.
    /// If it returns true, the worker pauses for the returned duration.
    /// This enables cluster-stress-aware backoff.
    pub fn spawn_workers<F, Fut>(
        &self,
        work_queue: Arc<BatchWorkQueue>,
        task_factory: F,
    ) -> Vec<tokio::task::JoinHandle<WorkerStats>>
    where
        F: Fn(u64, ProcessInfo) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = usize> + Send + 'static,
    {
        self.spawn_workers_with_pause(work_queue, task_factory, |_priority| None)
    }

    /// Spawn workers with a priority-aware pause check function.
    ///
    /// The pause_check function is called with the process priority before each task.
    /// If it returns Some(duration), the worker pauses for that duration.
    /// This enables cluster-stress-aware throttling based on task priority.
    ///
    /// Lower priority tasks pause more aggressively:
    /// - CRITICAL: Never pauses
    /// - HIGH: Only pauses under critical stress
    /// - NORMAL: Pauses under elevated stress
    /// - LOW/IDLE: Pauses under any stress
    pub fn spawn_workers_with_pause<F, Fut, P>(
        &self,
        work_queue: Arc<BatchWorkQueue>,
        task_factory: F,
        pause_check: P,
    ) -> Vec<tokio::task::JoinHandle<WorkerStats>>
    where
        F: Fn(u64, ProcessInfo) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = usize> + Send + 'static,
        P: Fn(u8) -> Option<Duration> + Send + Sync + Clone + 'static,
    {
        let mut handles = Vec::with_capacity(self.num_workers);

        for worker_id in 0..self.num_workers {
            let sched = self.scheduler.clone();
            let shutdown = self.shutdown.clone();
            let factory = task_factory.clone();
            let queue = work_queue.clone();
            let pause_fn = pause_check.clone();

            handles.push(tokio::spawn(async move {
                let mut stats = WorkerStats {
                    worker_id,
                    processes_completed: 0,
                    total_events: 0,
                    total_cpu_time: Duration::ZERO,
                };

                loop {
                    if shutdown.load(Ordering::SeqCst) {
                        break;
                    }

                    // Check if all work is complete
                    if queue.is_complete() {
                        break;
                    }

                    // Schedule next process with timeout
                    let pid = match tokio::time::timeout(Duration::from_secs(5), sched.schedule())
                        .await
                    {
                        Ok(Some(pid)) => pid,
                        Ok(None) => break,
                        Err(_) => {
                            // Timeout - check if work is complete
                            if queue.is_complete() || shutdown.load(Ordering::SeqCst) {
                                break;
                            }
                            continue;
                        }
                    };

                    let info = match sched.get_process(pid).await {
                        Some(info) => info,
                        None => continue,
                    };

                    // Priority-aware pause check
                    // Lower priority work pauses more aggressively under cluster stress
                    let priority_value = info.priority.value();
                    if let Some(pause_duration) = pause_fn(priority_value) {
                        tracing::info!(
                            "worker-{}: pausing {:?} for priority {} (pid={})",
                            worker_id,
                            pause_duration,
                            priority_value,
                            pid
                        );
                        // Put process back in ready queue before pausing
                        sched.unblock(pid).await;
                        tokio::time::sleep(pause_duration).await;
                        continue; // Re-schedule after pause
                    }

                    // Execute the task
                    let started = Instant::now();
                    let events = factory(pid, info.clone()).await;
                    let cpu_time = started.elapsed();

                    // Record completion in scheduler
                    sched.terminate(pid, events, cpu_time).await;

                    // Demand-driven: complete() spawns next pending work
                    // This is the key Linux-style behavior - completing a process
                    // frees a slot and allows the next pending work to be spawned
                    queue.complete(pid).await;

                    stats.processes_completed += 1;
                    stats.total_events += events;
                    stats.total_cpu_time += cpu_time;

                    tracing::debug!(
                        "worker-{}: pid={} completed events={} cpu={:?} progress={:.1}%",
                        worker_id,
                        pid,
                        events,
                        cpu_time,
                        queue.progress_percent()
                    );
                }

                stats
            }));
        }

        handles
    }
}

/// Statistics from a worker.
#[derive(Debug, Clone, Default)]
pub struct WorkerStats {
    pub worker_id: usize,
    pub processes_completed: usize,
    pub total_events: usize,
    pub total_cpu_time: Duration,
}

impl WorkerStats {
    /// Calculate events per second.
    pub fn events_per_second(&self) -> f64 {
        if self.total_cpu_time.as_secs_f64() > 0.0 {
            self.total_events as f64 / self.total_cpu_time.as_secs_f64()
        } else {
            0.0
        }
    }
}

// ============================================================================
// Process Registry - Global process tracking
// ============================================================================

/// Global process registry for all tasks in the system.
pub struct ProcessRegistry {
    /// All schedulers by name.
    schedulers: RwLock<std::collections::HashMap<String, Arc<ProcessScheduler>>>,
    /// System start time.
    start_time: Instant,
}

impl ProcessRegistry {
    /// Create a new registry.
    pub fn new() -> Self {
        Self {
            schedulers: RwLock::new(std::collections::HashMap::new()),
            start_time: Instant::now(),
        }
    }

    /// Register a scheduler.
    pub async fn register(&self, name: String, scheduler: Arc<ProcessScheduler>) {
        let mut scheds = self.schedulers.write().await;
        scheds.insert(name, scheduler);
    }

    /// Get a scheduler by name.
    pub async fn get(&self, name: &str) -> Option<Arc<ProcessScheduler>> {
        let scheds = self.schedulers.read().await;
        scheds.get(name).cloned()
    }

    /// Get system uptime.
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get aggregate stats across all schedulers.
    pub async fn aggregate_stats(&self) -> AggregateStats {
        let scheds = self.schedulers.read().await;
        let mut stats = AggregateStats::default();

        for (name, sched) in scheds.iter() {
            let counts = sched.process_counts().await;
            stats.total_processes +=
                counts.new + counts.ready + counts.running + counts.blocked + counts.terminated;
            stats.running += counts.running;
            stats.ready += counts.ready;
            stats.blocked += counts.blocked;
            stats.terminated += counts.terminated;
            stats.scheduler_names.push(name.clone());
        }

        stats.uptime = self.uptime();
        stats
    }
}

impl Default for ProcessRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate statistics across all schedulers.
#[derive(Debug, Clone, Default)]
pub struct AggregateStats {
    pub uptime: Duration,
    pub scheduler_names: Vec<String>,
    pub total_processes: usize,
    pub running: usize,
    pub ready: usize,
    pub blocked: usize,
    pub terminated: usize,
}

impl AggregateStats {
    /// Throughput in processes per second.
    pub fn throughput(&self) -> f64 {
        if self.uptime.as_secs_f64() > 0.0 {
            self.terminated as f64 / self.uptime.as_secs_f64()
        } else {
            0.0
        }
    }
}

// ============================================================================
// Group Scheduler - Manages all processing for a log group
// ============================================================================

/// Manages all processing tasks for a single log group.
/// Wraps ProcessScheduler with higher-level task management.
pub struct GroupScheduler {
    /// Log group name.
    log_group: String,
    /// Underlying process scheduler.
    scheduler: Arc<ProcessScheduler>,
    /// PIDs of daemon processes (for cleanup).
    daemon_pids: RwLock<Vec<u64>>,
}

impl GroupScheduler {
    /// Create a new group scheduler.
    pub fn new(log_group: String, resources: Resources, max_concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            log_group,
            scheduler: ProcessScheduler::new(resources, max_concurrency, 100),
            daemon_pids: RwLock::new(Vec::new()),
        })
    }

    /// Get the underlying scheduler.
    pub fn scheduler(&self) -> &Arc<ProcessScheduler> {
        &self.scheduler
    }

    /// Get the log group name.
    pub fn log_group(&self) -> &str {
        &self.log_group
    }

    /// Spawn the real-time tail daemon (CRITICAL priority).
    pub async fn spawn_realtime_tail(&self) -> u64 {
        let pid = self
            .scheduler
            .spawn_daemon(
                format!("{}/tail", self.log_group),
                Priority::CRITICAL,
                TaskType::RealtimeTail,
            )
            .await;
        self.daemon_pids.write().await.push(pid);
        pid
    }

    /// Spawn the reconcile daemon (HIGH priority).
    pub async fn spawn_reconcile(&self) -> u64 {
        let pid = self
            .scheduler
            .spawn_daemon(
                format!("{}/reconcile", self.log_group),
                Priority::HIGH,
                TaskType::Reconcile,
            )
            .await;
        self.daemon_pids.write().await.push(pid);
        pid
    }

    /// Spawn full history reconcile daemon (NORMAL priority).
    pub async fn spawn_full_history_reconcile(&self) -> u64 {
        let pid = self
            .scheduler
            .spawn_daemon(
                format!("{}/full-history", self.log_group),
                Priority::NORMAL,
                TaskType::FullHistoryReconcile,
            )
            .await;
        self.daemon_pids.write().await.push(pid);
        pid
    }

    /// Spawn conflict reindex daemon (LOW priority).
    pub async fn spawn_conflict_reindex(&self) -> u64 {
        let pid = self
            .scheduler
            .spawn_daemon(
                format!("{}/conflict-reindex", self.log_group),
                Priority::LOW,
                TaskType::ConflictReindex,
            )
            .await;
        self.daemon_pids.write().await.push(pid);
        pid
    }

    /// Spawn a schema heal batch process for a day (IDLE priority).
    pub async fn spawn_heal_day(&self, day_offset: u32) -> u64 {
        self.scheduler
            .spawn_batch(
                format!("{}/heal-day-{}", self.log_group, day_offset),
                day_offset,
                Priority::IDLE,
                TaskType::SchemaHeal,
            )
            .await
    }

    /// Spawn a backfill batch process for a day (priority by age).
    pub async fn spawn_backfill_day(&self, day_offset: u32) -> u64 {
        let priority = priority_for_day_offset(day_offset);
        self.scheduler
            .spawn_batch(
                format!("{}/backfill-day-{}", self.log_group, day_offset),
                day_offset,
                priority,
                TaskType::Backfill,
            )
            .await
    }

    /// Create a demand-driven batch work queue for backfill.
    ///
    /// Like Linux's process limits (RLIMIT_NPROC), this only keeps a limited
    /// number of processes in the ready queue and spawns more as workers
    /// complete work. This prevents memory exhaustion when processing many days.
    pub fn create_backfill_queue(self: &Arc<Self>, total_days: u32) -> BatchWorkQueue {
        BatchWorkQueue::new(
            self.clone(),
            TaskType::Backfill,
            total_days,
            self.scheduler.max_concurrency() * 2, // Like RLIMIT_NPROC
        )
    }

    /// Create a demand-driven batch work queue for healing.
    pub fn create_heal_queue(self: &Arc<Self>, total_days: u32) -> BatchWorkQueue {
        BatchWorkQueue::new(
            self.clone(),
            TaskType::SchemaHeal,
            total_days,
            self.scheduler.max_concurrency() * 2,
        )
    }

    /// Get process counts.
    pub async fn process_counts(&self) -> ProcessCounts {
        self.scheduler.process_counts().await
    }

    /// List all processes.
    pub async fn list_processes(&self) -> Vec<ProcessInfo> {
        self.scheduler.list_processes().await
    }

    /// List daemon processes.
    pub async fn list_daemons(&self) -> Vec<ProcessInfo> {
        let all = self.scheduler.list_processes().await;
        all.into_iter()
            .filter(|p| p.kind == ProcessKind::Daemon)
            .collect()
    }

    /// List batch processes.
    pub async fn list_batches(&self) -> Vec<ProcessInfo> {
        let all = self.scheduler.list_processes().await;
        all.into_iter()
            .filter(|p| p.kind == ProcessKind::Batch)
            .collect()
    }

    /// Terminate all daemon processes (for shutdown).
    pub async fn shutdown_daemons(&self) {
        let pids = self.daemon_pids.read().await.clone();
        for pid in pids {
            self.scheduler.terminate(pid, 0, Duration::ZERO).await;
        }
    }
}

// ============================================================================
// Demand-Driven Batch Work Queue (Linux-style RLIMIT_NPROC)
// ============================================================================

/// Pending work item waiting to be spawned.
#[derive(Debug, Clone)]
struct PendingWork {
    day_offset: u32,
    priority: Priority,
}

/// Demand-driven batch work queue.
///
/// Like Linux's process limits (RLIMIT_NPROC, kernel.pid_max), this prevents
/// spawning more processes than the system can handle. Instead of pre-spawning
/// thousands of processes:
///
/// 1. Pending work is tracked in a queue (not as spawned processes)
/// 2. Only `max_ready` processes are spawned at a time
/// 3. When a process completes, the next pending work is spawned
/// 4. Higher priority work is spawned first (like nice values)
///
/// This mimics how Linux handles fork():
/// - fork() fails with EAGAIN if RLIMIT_NPROC is exceeded
/// - Work must wait until a process slot is available
pub struct BatchWorkQueue {
    /// Group scheduler to spawn processes on.
    group_scheduler: Arc<GroupScheduler>,
    /// Task type for spawned processes.
    task_type: TaskType,
    /// Pending work items (not yet spawned).
    pending: Mutex<std::collections::BinaryHeap<PendingWorkEntry>>,
    /// Currently spawned PIDs.
    spawned: Mutex<Vec<u64>>,
    /// Maximum processes to keep in ready state (like RLIMIT_NPROC).
    max_ready: usize,
    /// Total work items (for progress tracking).
    total_work: u32,
    /// Completed work count.
    completed: AtomicUsize,
}

/// Entry for priority queue (higher priority first).
struct PendingWorkEntry {
    work: PendingWork,
}

impl Ord for PendingWorkEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority value = higher priority
        self.work.priority.cmp(&other.work.priority)
    }
}

impl PartialOrd for PendingWorkEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PendingWorkEntry {
    fn eq(&self, other: &Self) -> bool {
        self.work.priority == other.work.priority
    }
}

impl Eq for PendingWorkEntry {}

impl BatchWorkQueue {
    /// Create a new demand-driven batch work queue.
    ///
    /// # Arguments
    /// * `group_scheduler` - The scheduler to spawn processes on
    /// * `task_type` - Type of task (Backfill or SchemaHeal)
    /// * `total_days` - Total days to process
    /// * `max_ready` - Maximum processes to keep spawned (like RLIMIT_NPROC)
    pub fn new(
        group_scheduler: Arc<GroupScheduler>,
        task_type: TaskType,
        total_days: u32,
        max_ready: usize,
    ) -> Self {
        // Build pending work queue with priorities
        let mut pending = std::collections::BinaryHeap::new();
        for day_offset in 0..total_days {
            let priority = match task_type {
                TaskType::Backfill => priority_for_day_offset(day_offset),
                TaskType::SchemaHeal => Priority::IDLE,
                _ => Priority::NORMAL,
            };
            pending.push(PendingWorkEntry {
                work: PendingWork {
                    day_offset,
                    priority,
                },
            });
        }

        Self {
            group_scheduler,
            task_type,
            pending: Mutex::new(pending),
            spawned: Mutex::new(Vec::with_capacity(max_ready)),
            max_ready,
            total_work: total_days,
            completed: AtomicUsize::new(0),
        }
    }

    /// Get total work items.
    pub fn total_work(&self) -> u32 {
        self.total_work
    }

    /// Get completed work count.
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }

    /// Get pending work count (not yet spawned).
    pub async fn pending_count(&self) -> usize {
        self.pending.lock().await.len()
    }

    /// Get spawned process count.
    pub async fn spawned_count(&self) -> usize {
        self.spawned.lock().await.len()
    }

    /// Check if all work is complete.
    pub fn is_complete(&self) -> bool {
        self.completed() >= self.total_work as usize
    }

    /// Initialize the queue by spawning initial batch of processes.
    /// Like Linux's init process spawning initial daemons.
    pub async fn start(&self) -> Vec<u64> {
        let mut initial_pids = Vec::new();

        // Spawn up to max_ready processes initially
        for _ in 0..self.max_ready {
            if let Some(pid) = self.spawn_next().await {
                initial_pids.push(pid);
            } else {
                break;
            }
        }

        initial_pids
    }

    /// Spawn the next pending work item if under the limit.
    /// Returns None if no pending work or at capacity.
    async fn spawn_next(&self) -> Option<u64> {
        // Check if we're at capacity (like RLIMIT_NPROC)
        let spawned_count = self.spawned.lock().await.len();
        if spawned_count >= self.max_ready {
            return None;
        }

        // Get next highest priority work
        let work = {
            let mut pending = self.pending.lock().await;
            pending.pop().map(|e| e.work)
        }?;

        // Spawn the process
        let pid = match self.task_type {
            TaskType::Backfill => {
                self.group_scheduler
                    .spawn_backfill_day(work.day_offset)
                    .await
            }
            TaskType::SchemaHeal => self.group_scheduler.spawn_heal_day(work.day_offset).await,
            _ => return None,
        };

        self.spawned.lock().await.push(pid);
        Some(pid)
    }

    /// Mark a process as complete and spawn the next pending work.
    /// Like Linux's wait() followed by fork() pattern.
    pub async fn complete(&self, pid: u64) -> Option<u64> {
        // Remove from spawned list
        {
            let mut spawned = self.spawned.lock().await;
            spawned.retain(|&p| p != pid);
        }

        // Increment completed count
        self.completed.fetch_add(1, Ordering::SeqCst);

        // Spawn next work item if available
        self.spawn_next().await
    }

    /// Get all currently spawned PIDs.
    pub async fn spawned_pids(&self) -> Vec<u64> {
        self.spawned.lock().await.clone()
    }

    /// Progress as a percentage (0.0 to 100.0).
    pub fn progress_percent(&self) -> f64 {
        if self.total_work == 0 {
            return 100.0;
        }
        (self.completed() as f64 / self.total_work as f64) * 100.0
    }
}
