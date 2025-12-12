//! OS-style process scheduler for all processing tasks.
//! Models tasks as processes with resources, priorities, states, and fair scheduling.
//! Supports both long-running daemon processes (tail, reconcile) and batch processes (backfill).

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};

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
    /// Channel to notify scheduler of new processes.
    notify_tx: mpsc::Sender<u64>,
    /// Channel to receive notifications.
    notify_rx: Mutex<mpsc::Receiver<u64>>,
}

impl ProcessScheduler {
    pub fn new(resources: Resources, max_concurrency: usize, time_quantum_ms: u64) -> Arc<Self> {
        let (notify_tx, notify_rx) = mpsc::channel(1000);
        Arc::new(Self {
            next_pid: AtomicU64::new(1),
            processes: RwLock::new(std::collections::HashMap::new()),
            ready_queue: Mutex::new(BinaryHeap::new()),
            resources: Arc::new(ResourcePool::new(resources, max_concurrency)),
            time_quantum_ms,
            notify_tx,
            notify_rx: Mutex::new(notify_rx),
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

        let _ = self.notify_tx.send(pid).await;
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

        let _ = self.notify_tx.send(pid).await;
        pid
    }

    /// Get the next process to run (blocks until one is available).
    pub async fn schedule(&self) -> Option<u64> {
        loop {
            // Try to get from ready queue
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

            // Wait for notification of new process
            let mut rx = self.notify_rx.lock().await;
            rx.recv().await?;
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

        let mut queue = self.ready_queue.lock().await;
        queue.push(SchedulerEntry {
            effective_priority,
            pid,
        });
        let _ = self.notify_tx.send(pid).await;
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

    /// Spawn workers that execute scheduled processes.
    /// Each worker runs on its own OS thread via spawn_blocking for CPU-bound work.
    /// Returns handles to all worker tasks.
    pub fn spawn_workers<F, Fut>(
        &self,
        task_factory: F,
    ) -> Vec<tokio::task::JoinHandle<WorkerStats>>
    where
        F: Fn(u64, ProcessInfo) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = usize> + Send + 'static,
    {
        let mut handles = Vec::with_capacity(self.num_workers);

        for worker_id in 0..self.num_workers {
            let sched = self.scheduler.clone();
            let shutdown = self.shutdown.clone();
            let factory = task_factory.clone();

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

                    // Schedule next process with timeout
                    let pid = match tokio::time::timeout(Duration::from_secs(5), sched.schedule())
                        .await
                    {
                        Ok(Some(pid)) => pid,
                        Ok(None) => break,
                        Err(_) => {
                            if shutdown.load(Ordering::SeqCst) {
                                break;
                            }
                            continue;
                        }
                    };

                    let info = match sched.get_process(pid).await {
                        Some(info) => info,
                        None => continue,
                    };

                    // Execute the task
                    let started = Instant::now();
                    let events = factory(pid, info.clone()).await;
                    let cpu_time = started.elapsed();

                    // Record completion
                    sched.terminate(pid, events, cpu_time).await;

                    stats.processes_completed += 1;
                    stats.total_events += events;
                    stats.total_cpu_time += cpu_time;

                    tracing::debug!(
                        "worker-{}: pid={} completed events={} cpu={:?}",
                        worker_id,
                        pid,
                        events,
                        cpu_time
                    );
                }

                stats
            }));
        }

        handles
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

    /// Spawn all backfill processes for the given number of days.
    pub async fn spawn_all_backfill(&self, days: u32) -> Vec<u64> {
        let mut pids = Vec::with_capacity(days as usize);
        for day_offset in 0..days {
            let pid = self.spawn_backfill_day(day_offset).await;
            pids.push(pid);
        }
        pids
    }

    /// Spawn all heal processes for the given number of days.
    pub async fn spawn_all_heal(&self, days: u32) -> Vec<u64> {
        let mut pids = Vec::with_capacity(days as usize);
        for day_offset in 0..days {
            let pid = self.spawn_heal_day(day_offset).await;
            pids.push(pid);
        }
        pids
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
