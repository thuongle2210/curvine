#![cfg(feature = "spdk")]

//! SPDK I/O poller thread - handles NVMe submit/poll on dedicated thread.
use crate::io::spdk_ffi;
/// Qpairs not thread-safe: submit + poll must on same thread.
/// Single poller to demonstrate the correctness work.
/// Uses eventfd for instant wake on new I/O submission.
/// TODO: shard to multiple pollers (one per controller).
///
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use log::{error, info, warn};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

const EVENTSZ: usize = std::mem::size_of::<u64>();
/// I/O operation submitted to the poller thread.
pub enum IoOp {
    Read {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
    },
    Write {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
    },
    Flush {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    },
    /// Unregister a qpair from the poller before it is freed.
    UnregisterQpair {
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        ack: mpsc::Sender<()>,
    },
    /// Unregister a controller from the poller (stop admin completion polling).
    UnregisterCtrlr {
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        ack: mpsc::Sender<()>,
    },
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoOp {}

/// Completion state shared between poller callback and waiting handler.
pub struct IoCompletion {
    inner: Mutex<IoCompletionInner>,
    cond: Condvar,
}

struct IoCompletionInner {
    done: bool,
    status: i32,
}

impl IoCompletion {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(IoCompletionInner {
                done: false,
                status: 0,
            }),
            cond: Condvar::new(),
        })
    }

    /// Called by C callback on completion. Returns true if this call
    /// won the race and should perform accounting (inflight decrement).
    /// Returns false if the completion was already signaled (e.g., by
    /// force_complete_qpair) and accounting was already performed.
    pub fn complete(&self, status: i32) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.done {
            return false;
        }
        inner.done = true;
        inner.status = status;
        self.cond.notify_one();
        true
    }

    /// Block until complete or timeout. Returns NVMe status.
    pub fn wait(&self, timeout_us: u64) -> i32 {
        let mut inner = self.inner.lock().unwrap();
        if timeout_us == 0 {
            while !inner.done {
                inner = self.cond.wait(inner).unwrap();
            }
        } else {
            let timeout = std::time::Duration::from_micros(timeout_us);
            let deadline = std::time::Instant::now() + timeout;
            while !inner.done {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return -libc::ETIMEDOUT;
                }
                let (guard, result) = self.cond.wait_timeout(inner, remaining).unwrap();
                inner = guard;
                if result.timed_out() && !inner.done {
                    return -libc::ETIMEDOUT;
                }
            }
        }
        inner.status
    }
}

/// Request sent from handler threads to the poller.
pub struct IoRequest {
    pub op: IoOp,
    pub completion: Arc<IoCompletion>,
    /// Per-bdev in-flight counter. Decremented on completion.
    pub bdev_inflight: Arc<AtomicUsize>,
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoRequest {}

/// Poller states
enum PollerState {
    /// Active processing I/O - try_recv loop
    Active,
    /// Idle, blocked on eventfd waiting for work
    Idle,
}

/// Configuration for the poller thread.
pub struct PollerConfig {
    pub poll_interval_ms: u64,
    pub spin_iter: u32,
    /// I/O queue depth (max in-flight per qpair). Used for Vec pre-allocation.
    pub io_queue_depth: u32,
    /// Unique controllers for admin completion polling (keep-alive).
    pub ctrlrs: Vec<*mut spdk_ffi::spdk_nvme_ctrlr>,
}

// SAFETY: PollerConfig is moved into the dedicated poller thread;
// raw pointers are only accessed from that thread.
unsafe impl Send for PollerConfig {}

/// Per-qpair state tracked by the poller thread.
struct QpairState {
    /// True if force_complete_qpair was called for this qpair.
    dead: bool,
    /// Commands submitted to SPDK, awaiting completion or force-complete.
    pending: Vec<*mut CallbackCtx>,
    /// Commands force-completed but kept alive for late SPDK callbacks.
    stale: Vec<*mut CallbackCtx>,
}

impl QpairState {
    fn with_capacity(cap: usize) -> Self {
        Self {
            dead: false,
            pending: Vec::with_capacity(cap),
            stale: Vec::new(),
        }
    }
}

/// A qpair tracked by the poller with its state.
struct ActiveQpair {
    qpair: *mut spdk_ffi::spdk_nvme_qpair,
    state: Pin<Box<QpairState>>,
}

// SAFETY: ActiveQpair is only accessed from the poller thread
// (mutex-locked for cross-thread orphaned access).
unsafe impl Send for ActiveQpair {}

/// Poller thread handle.
pub struct SpdkPoller {
    /// Channel sender for I/O submissions
    tx: Option<crossbeam::channel::Sender<IoRequest>>,
    /// Eventfd for instant wake signaling
    eventfd: Arc<EventFd>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    /// Whether poller is blocked on eventfd (idle). Bdevs check this to
    /// skip eventfd write syscall when poller is already active.
    is_sleeping: Arc<AtomicBool>,
    /// Orphaned ActiveQpair entries keyed by qpair address.
    /// Keeps QpairState alive for late SPDK callbacks during free_io_qpair.
    orphaned: Arc<Mutex<HashMap<usize, ActiveQpair>>>,
}

impl SpdkPoller {
    /// Spawn a new poller thread with the given config.
    pub fn start(config: PollerConfig) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<IoRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let is_sleeping = Arc::new(AtomicBool::new(false));
        let is_sleeping_clone = is_sleeping.clone();

        // Create eventfd for wake signaling
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK)
            .expect("Failed to create eventfd");
        let eventfd_raw = eventfd.as_raw_fd();
        let eventfd_arc = Arc::new(eventfd);

        let orphaned = Arc::new(Mutex::new(HashMap::new()));
        let orphaned_clone = orphaned.clone();

        let handle = std::thread::Builder::new()
            .name("spdk-poller".to_string())
            .spawn(move || {
                Self::poller_loop(
                    rx,
                    shutdown_clone,
                    is_sleeping_clone,
                    eventfd_raw,
                    config,
                    orphaned_clone,
                );
            })
            .expect("Failed to spawn SPDK poller thread");

        Self {
            tx: Some(tx),
            eventfd: eventfd_arc,
            shutdown,
            handle: Some(handle),
            is_sleeping,
            orphaned,
        }
    }

    /// Get sender for SpdkBdev to hold.
    pub fn sender(&self) -> crossbeam::channel::Sender<IoRequest> {
        self.tx.as_ref().expect("Poller stopped").clone()
    }

    /// Get eventfd for signaling new I/O
    pub fn eventfd(&self) -> RawFd {
        self.eventfd.as_raw_fd()
    }

    /// Get eventfd as Arc for sharing with multiple bdevs
    pub fn eventfd_arc(&self) -> Arc<EventFd> {
        self.eventfd.clone()
    }

    /// Get is_sleeping flag for bdevs to check before eventfd write
    pub fn is_sleeping_arc(&self) -> Arc<AtomicBool> {
        self.is_sleeping.clone()
    }

    /// Unregister qpair from poller, blocking until removed to prevent UAF.
    /// Returns false if poller didn't ack within timeout (likely stuck/dead).
    pub fn unregister_qpair(&self, qpair: *mut spdk_ffi::spdk_nvme_qpair) -> bool {
        let (ack_tx, ack_rx) = mpsc::channel::<()>();
        // TODO: use dedicated control channel instead of IoRequest to avoid dummy allocation (negligible cost)
        let req = IoRequest {
            op: IoOp::UnregisterQpair { qpair, ack: ack_tx },
            completion: IoCompletion::new(),
            bdev_inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        if let Some(tx) = &self.tx {
            let _ = tx.send(req);
            let _ = self.eventfd.write(1);
            match ack_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(()) => true,
                Err(_) => {
                    error!("Unregister timeout: poller may be stuck, qpair not removed");
                    false
                }
            }
        } else {
            false // Poller stopped
        }
    }

    /// Unregister controller from the poller, blocking until removed to prevent UAF.
    /// Returns false if poller didn't ack within timeout (likely stuck/dead).
    pub fn unregister_ctrlr(&self, ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr) -> bool {
        let (ack_tx, ack_rx) = mpsc::channel::<()>();
        let req = IoRequest {
            op: IoOp::UnregisterCtrlr { ctrlr, ack: ack_tx },
            completion: IoCompletion::new(),
            bdev_inflight: Arc::new(AtomicUsize::new(0)),
        };
        if let Some(tx) = &self.tx {
            let _ = tx.send(req);
            let _ = self.eventfd.write(1);
            match ack_rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(()) => true,
                Err(_) => {
                    error!("Unregister ctrlr timeout: poller may be stuck, ctrlr not removed");
                    false
                }
            }
        } else {
            false
        }
    }

    /// True if orphaned map contains entries for this qpair.
    pub fn has_orphaned_for_qpair(&self, qpair: *mut spdk_ffi::spdk_nvme_qpair) -> bool {
        if let Ok(guard) = self.orphaned.lock() {
            guard.contains_key(&(qpair as usize))
        } else {
            false
        }
    }

    /// Remove orphaned ActiveQpair for this qpair and free all entries.
    /// Safe to call only after free_io_qpair has completed for this qpair
    /// (all late SPDK callbacks have fired).
    pub fn reclaim_orphaned_for_qpair(&self, qpair: *mut spdk_ffi::spdk_nvme_qpair) -> bool {
        if let Ok(mut guard) = self.orphaned.lock() {
            if let Some(mut aq) = guard.remove(&(qpair as usize)) {
                for ptr in aq.state.stale.drain(..) {
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }
                for ptr in aq.state.pending.drain(..) {
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }
                return true;
            }
        }
        false
    }

    /// Reclaim all orphaned ActiveQpair entries.
    /// SAFETY: Call only after all late SPDK callbacks have fired
    /// (i.e., after qpair_pool::drain_all in SpdkEnv::shutdown).
    pub fn reclaim_stale(&self) {
        if let Ok(mut guard) = self.orphaned.lock() {
            for (_qpair_key, mut aq) in guard.drain() {
                for ptr in aq.state.stale.drain(..) {
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }
                for ptr in aq.state.pending.drain(..) {
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }
            }
        }
    }

    /// Shut down the poller thread.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.eventfd.write(1); // Wake poll(eventfd, timeout) so shutdown is observed promptly
        self.tx.take(); // Drop sender to disconnect the channel during shutdown
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Poll all active qpairs, handle errors.
    /// On error: force_complete + move entire ActiveQpair to orphaned HashMap (keyed by qpair).
    fn poll_and_sweep(
        active_qpairs: &mut HashMap<usize, ActiveQpair>,
        orphaned: &Mutex<HashMap<usize, ActiveQpair>>,
        context: &str,
    ) {
        let err_keys: Vec<usize> = active_qpairs
            .iter()
            .filter_map(|(&key, aq)| {
                let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(aq.qpair, 0) };
                if rc < 0 {
                    error!("{}: qpair poll error: {}", context, rc);
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        if err_keys.is_empty() {
            return;
        }
        if let Ok(mut guard) = orphaned.lock() {
            for key in err_keys {
                if let Some(mut aq) = active_qpairs.remove(&key) {
                    force_complete_qpair(&mut aq);
                    if let Some(mut prev) = guard.remove(&key) {
                        warn!(
                            "qpair {:p} already orphaned, merging old entries",
                            key as *mut c_void
                        );
                        aq.state.stale.extend(prev.state.stale.drain(..));
                        aq.state.stale.extend(prev.state.pending.drain(..));
                    }
                    guard.insert(key, aq);
                }
            }
        }
    }

    /// Main poller loop. Runs on dedicated thread.
    fn poller_loop(
        rx: crossbeam::channel::Receiver<IoRequest>,
        shutdown: Arc<AtomicBool>,
        is_sleeping: Arc<AtomicBool>,
        eventfd: RawFd,
        config: PollerConfig,
        orphaned: Arc<Mutex<HashMap<usize, ActiveQpair>>>,
    ) {
        let io_queue_depth = config.io_queue_depth as usize;
        let mut active_qpairs: HashMap<usize, ActiveQpair> = HashMap::with_capacity(8);
        let mut active_ctrlrs: Vec<*mut spdk_ffi::spdk_nvme_ctrlr> = config.ctrlrs;
        let mut state = PollerState::Idle;
        // Verify curvine_async_ctx buffer fits the C struct.
        debug_assert!(
            unsafe { spdk_ffi::curvine_spdk_async_ctx_sizeof() }
                <= std::mem::size_of::<spdk_ffi::curvine_async_ctx>(),
            "curvine_async_ctx C struct exceeds Rust buffer"
        );

        // Poll interval for keep-alive check (parameterized to detect disconnects)
        let poll_interval = config.poll_interval_ms as i32;
        loop {
            // Check shutdown first
            if shutdown.load(Ordering::Acquire) && rx.is_empty() && active_qpairs.is_empty() {
                break;
            }

            // Active state: drain all pending I/Os and poll completions
            if matches!(state, PollerState::Active) {
                // Drain pending requests (non-blocking)
                while let Ok(req) = rx.try_recv() {
                    if matches!(req.op, IoOp::UnregisterCtrlr { .. }) {
                        Self::handle_unregister_ctrlr(&req, &mut active_ctrlrs);
                    } else if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                        Self::handle_unregister(&req, &mut active_qpairs, &*orphaned);
                    } else {
                        Self::submit_one(&req, &mut active_qpairs, io_queue_depth);
                    }
                }

                // Spin briefly before Idle to avoid eventfd round-trip for back-to-back I/O
                if rx.is_empty() && active_qpairs.is_empty() {
                    state = PollerState::Idle;
                    for _ in 0..config.spin_iter {
                        std::hint::spin_loop();
                        if !rx.is_empty() {
                            state = PollerState::Active;
                            break;
                        }
                    }
                }
                continue;
            }

            // Idle state: wait for work (eventfd or channel)
            if matches!(state, PollerState::Idle) {
                // Check if channel has pending data (peek)
                let has_pending = !rx.is_empty();

                if has_pending {
                    // Channel has data, transition to Active
                    state = PollerState::Active;
                    continue;
                }

                // Mark as sleeping before blocking — bdevs will write eventfd to wake us
                is_sleeping.store(true, Ordering::SeqCst);

                // Recheck channel after setting flag (closes race with bdev send)
                if !rx.is_empty() {
                    is_sleeping.store(false, Ordering::Release);
                    state = PollerState::Active;
                    continue;
                }

                // Wait on eventfd with timeout for keep-alive check
                let mut eventfd_pollfd = libc::pollfd {
                    fd: eventfd,
                    events: libc::POLLIN,
                    revents: 0,
                };

                let result = unsafe { libc::poll(&mut eventfd_pollfd, 1, poll_interval) };

                match result {
                    n if n > 0 => {
                        // Woken up — clear sleeping flag
                        is_sleeping.store(false, Ordering::Release);

                        // Eventfd signaled - drain it
                        let mut buf = [0u8; EVENTSZ];
                        let _ = unsafe {
                            libc::read(eventfd, buf.as_mut_ptr() as *mut c_void, EVENTSZ)
                        };

                        // Drain any pending channel data
                        while let Ok(req) = rx.try_recv() {
                            if matches!(req.op, IoOp::UnregisterCtrlr { .. }) {
                                Self::handle_unregister_ctrlr(&req, &mut active_ctrlrs);
                            } else if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                                Self::handle_unregister(&req, &mut active_qpairs, &*orphaned);
                            } else {
                                Self::submit_one(&req, &mut active_qpairs, io_queue_depth);
                            }
                        }

                        // Transition to Active to process work
                        state = PollerState::Active;
                    }
                    0 => {
                        // Timeout - poll active qpairs to check connection health
                        Self::poll_and_sweep(&mut active_qpairs, &*orphaned, "keep-alive");
                        // Process admin completions (keep-alive)
                        Self::process_admin_completions(&active_ctrlrs);
                        state = PollerState::Idle;
                    }
                    -1 => {
                        let errno = unsafe { *libc::__errno_location() };
                        if errno == libc::EINTR {
                            continue;
                        }
                        error!("Poller: poll error: {}", errno);
                        break;
                    }
                    _ => {
                        break;
                    }
                }
            }
        }

        // Thread exiting — drain remaining ActiveQpairs into orphaned HashMap for safe reclamation.
        if let Ok(mut guard) = orphaned.lock() {
            for (key, aq) in active_qpairs.drain() {
                if let Some(mut prev) = guard.remove(&key) {
                    warn!(
                        "qpair {:p} already orphaned on thread exit, merging old entries",
                        key as *mut c_void
                    );
                    aq.state.stale.extend(prev.state.stale.drain(..));
                    aq.state.stale.extend(prev.state.pending.drain(..));
                }
                guard.insert(key, aq);
            }
        }

        info!("SPDK poller thread exiting");
    }

    /// Handle unregister request, remove qpair from active set and ack.
    ///
    /// Always orphans — disconnect+poll does not guarantee all late callbacks
    /// have fired, so we must keep the ActiveQpair alive in orphaned for
    /// late callbacks during free_io_qpair.
    fn handle_unregister(
        req: &IoRequest,
        active_qpairs: &mut HashMap<usize, ActiveQpair>,
        orphaned: &Mutex<HashMap<usize, ActiveQpair>>,
    ) {
        if let IoOp::UnregisterQpair { qpair, ack } = &req.op {
            if let Some(mut aq) = active_qpairs.remove(&(*qpair as usize)) {
                if !aq.state.dead {
                    // Disconnect + poll — may process some abort completions
                    // synchronously, but does NOT guarantee all late callbacks
                    // have fired. free_io_qpair can still fire callbacks.
                    unsafe {
                        spdk_ffi::spdk_nvme_ctrlr_disconnect_io_qpair(*qpair);
                        spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0);
                    }
                }

                // Always orphan — keep ActiveQpair alive for late callbacks
                // during free_io_qpair (graceful path removed).
                for &ptr in &aq.state.pending {
                    unsafe {
                        if (*ptr).completion.complete(-libc::ESHUTDOWN) {
                            (*ptr).bdev_inflight.fetch_sub(1, Ordering::Release);
                        }
                    }
                }
                aq.state.stale.append(&mut aq.state.pending);
                let qpair_key = *qpair as usize;
                if let Ok(mut guard) = orphaned.lock() {
                    if let Some(mut prev) = guard.remove(&qpair_key) {
                        warn!(
                            "qpair {:p} already orphaned during unregister, merging old entries",
                            qpair_key as *mut c_void
                        );
                        aq.state.stale.extend(prev.state.stale.drain(..));
                        aq.state.stale.extend(prev.state.pending.drain(..));
                    }
                    guard.insert(qpair_key, aq);
                }
            }
            let _ = ack.send(());
        }
    }

    /// Handle unregister controller request, remove from active set and ack.
    fn handle_unregister_ctrlr(
        req: &IoRequest,
        active_ctrlrs: &mut Vec<*mut spdk_ffi::spdk_nvme_ctrlr>,
    ) {
        if let IoOp::UnregisterCtrlr { ctrlr, ack } = &req.op {
            active_ctrlrs.retain(|c| *c != *ctrlr);
            let _ = ack.send(());
        }
    }

    /// Submit a single I/O request on the poller thread.
    fn submit_one(
        req: &IoRequest,
        active_qpairs: &mut HashMap<usize, ActiveQpair>,
        io_queue_depth: usize,
    ) {
        let qpair = match &req.op {
            IoOp::Read { qpair, .. } => *qpair,
            IoOp::Write { qpair, .. } => *qpair,
            IoOp::Flush { qpair, .. } => *qpair,
            IoOp::UnregisterQpair { .. } => {
                unreachable!("UnregisterQpair handled by handle_unregister")
            }
            IoOp::UnregisterCtrlr { .. } => {
                unreachable!("UnregisterCtrlr handled by handle_unregister_ctrlr")
            }
        };

        let state = &mut active_qpairs
            .entry(qpair as usize)
            .or_insert_with(|| ActiveQpair {
                qpair,
                state: Pin::new(Box::new(QpairState::with_capacity(io_queue_depth))),
            })
            .state;

        let pending_idx = state.pending.len();
        // Must be computed BEFORE push to pending.
        // Cast to raw pointer AFTER mutable deref to ensure we get the heap address, not a reborrowed stack address.
        let owner_ptr = &mut **state as *mut QpairState;

        let cb_ctx = Box::new(CallbackCtx {
            completion: req.completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: req.bdev_inflight.clone(),
            pending_idx,
            owner: owner_ptr,
        });
        let cb_ctx_ptr = Box::into_raw(cb_ctx);

        // Initialize async_ctx via C helper
        unsafe {
            spdk_ffi::curvine_spdk_async_ctx_init(
                &mut (*cb_ctx_ptr).async_ctx,
                poller_callback,
                cb_ctx_ptr as *mut c_void,
            );
        }

        // Push to pending BEFORE SPDK submit to handle synchronous completions.
        // If SPDK calls poller_callback during submit (some transports/backends),
        // the callback finds this entry in pending, removes+frees it inline.
        // If not, the entry stays and the callback removes it on the next poll.
        state.pending.push(cb_ctx_ptr);

        let rc = match &req.op {
            IoOp::Read {
                ns,
                qpair,
                buf,
                offset,
                num_bytes,
            } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_read(
                    *ns,
                    *qpair,
                    *buf,
                    *offset,
                    *num_bytes,
                    &mut (*cb_ctx_ptr).async_ctx,
                )
            },
            IoOp::Write {
                ns,
                qpair,
                buf,
                offset,
                num_bytes,
            } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_write(
                    *ns,
                    *qpair,
                    *buf,
                    *offset,
                    *num_bytes,
                    &mut (*cb_ctx_ptr).async_ctx,
                )
            },
            IoOp::Flush { ns, qpair } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_flush(*ns, *qpair, &mut (*cb_ctx_ptr).async_ctx)
            },
            IoOp::UnregisterQpair { .. } => {
                unreachable!("UnregisterQpair handled by handle_unregister")
            }
            IoOp::UnregisterCtrlr { .. } => {
                unreachable!("UnregisterCtrlr handled by handle_unregister_ctrlr")
            }
        };

        if rc != 0 {
            // Submission failed — reclaim allocation and complete with error.
            // If the callback fired synchronously (saw entry in pending, removed+freed it),
            // we skip the remove/free because the callback already handled it.
            // Use position() scan instead of cached pending_idx: other callbacks
            // during submit may have moved our entry via swap_remove, making the
            // local variable stale. Comparing raw pointer values is safe even if
            // the target was freed — position() returns None when not in pending.
            if let Some(actual_idx) = state.pending.iter().position(|&p| p == cb_ctx_ptr) {
                let last = state.pending.len() - 1;
                if actual_idx != last {
                    state.pending[actual_idx] = state.pending[last];
                    (*state.pending[actual_idx]).pending_idx = actual_idx;
                }
                state.pending.pop();
                drop(Box::from_raw(cb_ctx_ptr));
            }
            if req.completion.complete(rc) {
                req.bdev_inflight.fetch_sub(1, Ordering::Release);
            }
            return;
        }
        // rc == 0: submission accepted. Entry is in pending, awaiting completion
        // callback. If the callback fired synchronously during submit, it already
        // removed the entry from pending — no further work needed.
    }

    /// Process admin completions on all controllers to service keep-alive.
    fn process_admin_completions(ctrlrs: &[*mut spdk_ffi::spdk_nvme_ctrlr]) {
        for &ctrlr in ctrlrs {
            let rc = unsafe { spdk_ffi::spdk_nvme_ctrlr_process_admin_completions(ctrlr) };
            if rc < 0 {
                warn!("ctrlr {:p} admin completion error: rc={}", ctrlr, rc);
            }
        }
    }
}

/// C callback context. Heap-allocated for SPDK to hold pointer.
struct CallbackCtx {
    completion: Arc<IoCompletion>,
    async_ctx: spdk_ffi::curvine_async_ctx,
    bdev_inflight: Arc<AtomicUsize>,
    /// Index in QpairState::pending. Updated by swap_remove when other entries complete.
    pending_idx: usize,
    /// Pointer to owning QpairState (Pin<Box<QpairState>> — stable heap address).
    owner: *mut QpairState,
}

/// C callback invoked by SPDK when NVMe command completes.
/// Hot path: removes self from pending + Box::from_raw(self).
/// Late callback (during free_io_qpair on orphaned qpair): entry is in stale,
/// not in pending. The pending_idx check fails so we skip remove/free and just signal.
unsafe extern "C" fn poller_callback(cb_arg: *mut c_void, status: i32) {
    let ctx = cb_arg as *mut CallbackCtx;
    if (*ctx).completion.complete(status) {
        (*ctx).bdev_inflight.fetch_sub(1, Ordering::Release);
    }

    let qs = &mut *(*ctx).owner;
    let idx = (*ctx).pending_idx;
    // Only remove from pending if the entry is still there (hot path).
    // If not (late callback on orphaned entry), skip remove/free.
    if idx < qs.pending.len() && qs.pending[idx] == ctx {
        let last_idx = qs.pending.len() - 1;
        if idx != last_idx {
            qs.pending[idx] = qs.pending[last_idx];
            (*qs.pending[idx]).pending_idx = idx;
        }
        qs.pending.pop();
        drop(Box::from_raw(ctx));
    }
}

/// Force-complete all pending I/Os for a qpair that has failed.
/// Signals waiters, decrements inflight, moves pending to stale.
/// CallbackCtx entries are kept alive for late SPDK callbacks
/// (QpairState stays alive via orphaned ActiveQpair).
fn force_complete_qpair(aq: &mut ActiveQpair) {
    aq.state.dead = true;
    for &cb_ctx_ptr in &aq.state.pending {
        unsafe {
            // No reclaimed check needed — entries where callback fired were
            // removed from pending inline by the callback itself.
            if (*cb_ctx_ptr).completion.complete(-libc::EIO) {
                (*cb_ctx_ptr).bdev_inflight.fetch_sub(1, Ordering::Release);
            }
        }
    }
    // Move pending to stale — keep allocations alive for late callbacks
    aq.state.stale.append(&mut aq.state.pending);
}

impl Drop for SpdkPoller {
    fn drop(&mut self) {
        self.stop();
        // Note: We do NOT call reclaim_stale() here because late SPDK callbacks
        // may still fire (e.g., in the timeout path where pool wasn't drained).
        // The caller (SpdkEnv::shutdown) is responsible for calling reclaim_stale()
        // after pool drain when late callbacks are guaranteed finished.
        // Any remaining orphaned entries leak in the timeout path — bounded leak.
    }
}
