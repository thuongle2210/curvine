//! SPDK I/O poller — handles NVMe submit/poll on dedicated thread.
//! One poller per controller (1:1 mapping, legacy).
//! Uses eventfd for instant wake on new I/O submission.
//!
//! When spdk_native_reactor feature is enabled, uses SPDK native reactor threads
//! with round-robin controller-to-reactor mapping.
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use crate::io::spdk_ffi;
use futures::task::AtomicWaker;
use log::{error, info, warn};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::ffi::{c_int, c_void};
use std::future::Future;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
const EVENT_SZ: usize = std::mem::size_of::<u64>();

// I/O operation submitted to poller thread
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
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoOp {}

/// Completion state shared between poller callback and waiting handler.
/// Supports both sync (Condvar) and async (Waker) waiters.
pub struct IoCompletion {
    inner: Mutex<IoCompletionInner>,
    cond: Condvar,
    atomic_waker: AtomicWaker,
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
            atomic_waker: AtomicWaker::new(),
        })
    }

    /// Called by C callback on completion.
    pub fn complete(&self, status: i32) {
        let mut inner = self.inner.lock().unwrap();
        inner.done = true;
        inner.status = status;
        self.cond.notify_one();
        self.atomic_waker.wake();
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

    /// Poll completion status for async await.
    pub fn poll_async(&self, cx: &mut Context<'_>) -> Poll<i32> {
        let inner = self.inner.lock().unwrap();
        if inner.done {
            return Poll::Ready(inner.status);
        }
        self.atomic_waker.register(cx.waker());
        Poll::Pending
    }

    /// Convert into a Future that resolves when I/O completes.
    pub fn into_future(self: Arc<Self>) -> IoCompletionFuture {
        IoCompletionFuture { completion: self }
    }
}

/// Future wrapping IoCompletion for async await.
pub struct IoCompletionFuture {
    completion: Arc<IoCompletion>,
}

impl Future for IoCompletionFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.completion.poll_async(cx)
    }
}

/// Type alias for IoCompletionFuture
pub type AsyncSpdkRead = IoCompletionFuture;

// Request sent from handler threads to the poller
pub struct IoRequest {
    pub op: IoOp,
    pub completion: Arc<IoCompletion>,
    /// Per-bdev in-flight counter. Decremented on completion.
    pub bdev_inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Message to create qpair on reactor thread (spdk_native_reactor only)
#[cfg(feature = "spdk_native_reactor")]
pub struct QpairCreateRequest {
    pub ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    pub qpair_out: std::sync::Arc<std::sync::Mutex<Option<*mut spdk_ffi::spdk_nvme_qpair>>>,
    pub completion: Arc<IoCompletion>,
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoRequest {}

// Poller states
enum PollerState {
    /// Active processing I/O — try_recv loop
    Active,
    /// Idle, blocked on eventfd waiting for work
    Idle,
}

/// Poller thread handle.
pub struct SpdkPoller {
    ctrlr_id: usize,
    /// Channel sender for I/O submissions
    tx: Option<crossbeam::channel::Sender<IoRequest>>,
    /// Eventfd for instant wake signaling
    eventfd: Arc<EventFd>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl SpdkPoller {
    /// Spawn a new poller thread.
    pub fn start(ctrlr_id: usize, poll_interval_ms: u64) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<IoRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        // Create eventfd for wake signaling
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK)
            .expect("Failed to create eventfd");
        let eventfd_raw = eventfd.as_raw_fd();
        let eventfd_arc = Arc::new(eventfd);

        let handle = std::thread::Builder::new()
            .name(format!("spdk-poller-{}", ctrlr_id))
            .spawn(move || {
                Self::poller_loop(rx, shutdown_clone, eventfd_raw, poll_interval_ms);
            })
            .expect("Failed to spawn SPDK poller thread");

        Self {
            ctrlr_id,
            tx: Some(tx),
            eventfd: eventfd_arc,
            shutdown,
            handle: Some(handle),
        }
    }

    /// Get the controller ID this poller handles.
    pub fn ctrlr_id(&self) -> usize {
        self.ctrlr_id
    }

    /// Get sender for SpdkBdev to hold.
    pub fn sender(&self) -> crossbeam::channel::Sender<IoRequest> {
        self.tx.as_ref().expect("Poller stopped").clone()
    }

    /// Get eventfd for signaling new I/O
    pub fn eventfd(&self) -> Arc<EventFd> {
        self.eventfd.clone()
    }

    /// Get raw eventfd
    pub fn eventfd_raw(&self) -> RawFd {
        self.eventfd.as_raw_fd()
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
            // // Use eventfd_write via libc to avoid AsFd trait issues
            // // libc::eventfd_write expects fd as c_int (i32)
            // let _ = unsafe { libc::eventfd_write(self.eventfd_raw(), 1) };
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

    /// Shut down the poller thread.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.eventfd.write(1); // Wake poll(eventfd, timeout) so shutdown is observed promptly
                                       // // Use libc::eventfd_write to avoid AsFd trait issues
                                       // let _ = unsafe { libc::eventfd_write(self.eventfd_raw(), 1) };
        self.tx.take(); // Drop sender to disconnect the channel during shutdown
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Main poller loop. Runs on dedicated thread.
    fn poller_loop(
        rx: crossbeam::channel::Receiver<IoRequest>,
        shutdown: Arc<AtomicBool>,
        eventfd: RawFd,
        poll_interval_ms: u64,
    ) {
        let mut active_qpairs: Vec<*mut spdk_ffi::spdk_nvme_qpair> = Vec::new();
        let mut state = PollerState::Idle;

        // Verify curvine_async_ctx buffer fits the C struct.
        debug_assert!(
            unsafe { spdk_ffi::curvine_spdk_async_ctx_sizeof() }
                <= std::mem::size_of::<spdk_ffi::curvine_async_ctx>(),
            "curvine_async_ctx C struct exceeds Rust buffer"
        );

        // Poll interval for keep-alive check (parameterized to detect disconnects)
        let poll_interval = poll_interval_ms as i32;

        loop {
            // Check shutdown first
            if shutdown.load(Ordering::Acquire) && rx.is_empty() && active_qpairs.is_empty() {
                break;
            }

            // Active state: drain all pending I/Os and poll completions
            if matches!(state, PollerState::Active) {
                // Drain pending requests (non-blocking)
                while let Ok(req) = rx.try_recv() {
                    if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                        Self::handle_unregister(&req, &mut active_qpairs);
                    } else {
                        Self::submit_one(&req, &mut active_qpairs);
                    }
                }

                // Poll qpairs for completions
                active_qpairs.retain(|qpair| {
                    let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                    if rc < 0 {
                        error!("qpair poll error: {}", rc);
                        return false;
                    }
                    true
                });

                // Transition to Idle if no more work
                if rx.is_empty() && active_qpairs.is_empty() {
                    state = PollerState::Idle;
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

                // Wait on eventfd with timeout for keep-alive check
                let mut eventfd_pollfd = libc::pollfd {
                    fd: eventfd,
                    events: libc::POLLIN,
                    revents: 0,
                };

                let result = unsafe { libc::poll(&mut eventfd_pollfd, 1, poll_interval) };

                match result {
                    n if n > 0 => {
                        // Eventfd signaled - drain it
                        let mut buf = [0u8; EVENT_SZ];
                        let _ = unsafe {
                            libc::read(eventfd, buf.as_mut_ptr() as *mut c_void, EVENT_SZ)
                        };

                        // Drain any pending channel data
                        while let Ok(req) = rx.try_recv() {
                            if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                                Self::handle_unregister(&req, &mut active_qpairs);
                            } else {
                                Self::submit_one(&req, &mut active_qpairs);
                            }
                        }

                        // Transition to Active to process work
                        state = PollerState::Active;
                    }
                    0 => {
                        // Timeout - poll active qpairs to check connection health
                        active_qpairs.retain(|qpair| {
                            let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                            if rc < 0 {
                                error!("Poller: keep-alive poll error: {}, removing qpair", rc);
                                return false;
                            }
                            true
                        });
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

        info!("SPDK poller thread exiting");
    }

    /// Handle unregister request, remove qpair from active set and ack.
    fn handle_unregister(req: &IoRequest, active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>) {
        if let IoOp::UnregisterQpair { qpair, ack } = &req.op {
            active_qpairs.retain(|qp| *qp != *qpair);
            let _ = ack.send(());
        }
    }

    /// Submit a single I/O request on the poller thread.
    fn submit_one(req: &IoRequest, active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>) {
        let qpair = match &req.op {
            IoOp::Read { qpair, .. } => *qpair,
            IoOp::Write { qpair, .. } => *qpair,
            IoOp::Flush { qpair, .. } => *qpair,
            IoOp::UnregisterQpair { .. } => {
                unreachable!("UnregisterQpair handled by handle_unregister")
            }
        };

        // Box::into_raw ensures CallbackCtx survives until poller_callback reclaims it
        let cb_ctx = Box::new(CallbackCtx {
            completion: req.completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: req.bdev_inflight.clone(),
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
        };

        if rc != 0 {
            // Submission failed - reclaim allocation and complete with error
            unsafe { drop(Box::from_raw(cb_ctx_ptr)) };
            req.bdev_inflight.fetch_sub(1, Ordering::Release);
            req.completion.complete(rc);
            return;
        }

        // Track active qpair
        if !active_qpairs.contains(&qpair) {
            active_qpairs.push(qpair);
        }
    }
}

impl Drop for SpdkPoller {
    fn drop(&mut self) {
        self.stop();
    }
}

/// C callback context.
/// TODO: use object pool to avoid per-I/O heap allocation
struct CallbackCtx {
    completion: Arc<IoCompletion>,
    async_ctx: spdk_ffi::curvine_async_ctx,
    bdev_inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// C callback invoked by SPDK when NVMe command completes.
unsafe extern "C" fn poller_callback(cb_arg: *mut c_void, status: i32) {
    let ctx = Box::from_raw(cb_arg as *mut CallbackCtx);
    ctx.bdev_inflight.fetch_sub(1, Ordering::Release);
    ctx.completion.complete(status);
}

// ---------------------------------------------------------------------------
// SPDK Native Reactor Support
// ---------------------------------------------------------------------------

/// Message handler for native reactor: processes I/O requests sent via spdk_thread_send_msg.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn spdk_native_reactor_msg_handler(arg: *mut c_void) {
    eprintln!("[Reactor] spdk_native_reactor_msg_handler called");
    let req = Box::from_raw(arg as *mut IoRequest);
    let qpair = match &req.op {
        IoOp::Read { qpair, .. } => *qpair,
        IoOp::Write { qpair, .. } => *qpair,
        IoOp::Flush { qpair, .. } => *qpair,
        IoOp::UnregisterQpair { .. } => {
            eprintln!("[Reactor] UnregisterQpair op");
            return;
        }
    };
    eprintln!("[Reactor] Received IoRequest: qpair={:?}, submitting I/O", qpair);
    submit_one(&req, &mut Vec::new());
    eprintln!("[Reactor] submit_one completed, waiting for completion...");
    
    // Poll via spdk_thread_poll a few times to allow callbacks to execute
    // The reactor_poller_cb will continue polling in the background
    for i in 0..10 {
        let current_thread = spdk_ffi::spdk_get_thread();
        if !current_thread.is_null() {
            let msgs = spdk_ffi::spdk_thread_poll(current_thread, 0, 0);
            if i < 3 {
                eprintln!("[Reactor] Poll iteration {}, msgs={}", i, msgs);
            }
        }
        
        if req.completion.inner.lock().unwrap().done {
            let status = req.completion.inner.lock().unwrap().status;
            eprintln!("[Reactor] Completion done after {} polls: status={}", i + 1, status);
            return;
        }
    }
    
    let done = req.completion.inner.lock().unwrap().done;
    let status = req.completion.inner.lock().unwrap().status;
    eprintln!("[Reactor] Completion not done after 10 polls: done={}, status={}", done, status);
}

/// Register a single poller on the reactor thread for ALL qpairs.
/// Called via spdk_thread_send_msg on the reactor thread.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn reactor_poller_register_handler(arg: *mut c_void) {
    use crate::io::spdk_ffi;
    use std::sync::atomic::Ordering;
    
    info!("[Reactor] Registering poller on reactor thread");
    
    let state = Arc::from_raw(arg as *const crate::io::spdk_env::ReactorState);
    
    // Create poller that processes completions for ALL qpairs on this reactor
    let state_ptr = Arc::into_raw(state.clone()) as *mut c_void;
    info!("[Reactor] Calling spdk_poller_register");
    let poller = spdk_ffi::spdk_poller_register(
        reactor_poller_cb,
        state_ptr,
        0,  // period_us = 0 means poll every reactor iteration
    );
    
    if poller.is_null() {
        error!("[Reactor] Failed to register reactor poller");
        return;
    }
    
    info!("[Reactor] Poller registered at {:?}", poller);
    
    // Store poller pointer in state
    let state_mut = &mut *(arg as *mut crate::io::spdk_env::ReactorState);
    state_mut.poller = poller;
    
    info!("[Reactor] Reactor poller registered successfully, stored in state");
}

/// Unregister the reactor poller.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn reactor_poller_unregister_handler(arg: *mut c_void) {
    use crate::io::spdk_ffi;

    let state = &mut *(arg as *mut crate::io::spdk_env::ReactorState);

    if !state.poller.is_null() {
        let poller = state.poller;
        spdk_ffi::spdk_poller_unregister(poller);
        state.poller = std::ptr::null_mut();
        info!("Reactor poller unregistered");
    }
}

/// Poller callback: processes completions for ALL qpairs on this reactor.
/// This is called by SPDK reactor on every iteration.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn reactor_poller_cb(arg: *mut c_void) -> c_int {
    use crate::io::spdk_ffi;
    
    static mut CALLBACK_COUNT: u64 = 0;
    unsafe { CALLBACK_COUNT += 1; }
    
    // Cast arg to ReactorState pointer
    let state_ptr = arg as *const crate::io::spdk_env::ReactorState;
    let state = &*state_ptr;
    let qpairs = state.qpairs.lock().unwrap();
    
    let qpair_count = qpairs.len();
    
    if unsafe { CALLBACK_COUNT <= 5 } || CALLBACK_COUNT % 1000000 == 0 {
        eprintln!("[Reactor poller #{}] state={:?}, thread={:?}, qpair_count={}", 
            CALLBACK_COUNT, state_ptr, state.thread, qpair_count);
    }
    
    if qpair_count == 0 {
        // Only print this occasionally to avoid spam
        if unsafe { CALLBACK_COUNT <= 100 || CALLBACK_COUNT % 1000000 == 0 } {
            eprintln!("[Reactor poller #{}] state={:?}: No qpairs registered!", CALLBACK_COUNT, state_ptr);
        }
        return 0;
    }
    
    let mut total_completions = 0i32;
    
    for &qpair in qpairs.iter() {
        if !qpair.is_null() {
            let rc = spdk_ffi::spdk_nvme_qpair_process_completions(qpair, 0);
            if rc > 0 && unsafe { CALLBACK_COUNT <= 1000 } {
                eprintln!("[Reactor poller #{}] Processed {} completions for qpair {:?}", CALLBACK_COUNT, rc, qpair);
            } else if rc < 0 {
                eprintln!("[Reactor poller #{}] Error processing qpair {:?}: rc={}", CALLBACK_COUNT, qpair, rc);
            }
            total_completions += rc;
        } else {
            eprintln!("[Reactor poller #{}] Found NULL qpair in list!", CALLBACK_COUNT);
        }
    }
    
    // Return 1 if busy (more completions possible), 0 if idle
    let busy = if total_completions > 0 { 1 } else { 0 };
    if unsafe { CALLBACK_COUNT % 1000000 == 0 } {
        eprintln!("[Reactor poller #{}] Total completions: {} across {} qpairs, busy={}", 
            CALLBACK_COUNT, total_completions, qpair_count, busy);
    }
    busy
}

/// Message handler to create qpair on reactor thread (spdk_native_reactor only).
/// SPDK requires qpairs to be created on the thread that processes them.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn qpair_create_handler(arg: *mut c_void) {
    use crate::io::spdk_env::find_reactor_for_controller;
    static mut QPAIR_CREATE_COUNT: u64 = 0;
    unsafe { QPAIR_CREATE_COUNT += 1; }
    
    let req = Box::from_raw(arg as *mut QpairCreateRequest);
    
    // Get current thread to verify we're on the reactor thread
    let current_thread = spdk_ffi::spdk_get_thread();
    eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}] current_thread={:?}, ctrlr={:?}", 
        current_thread, req.ctrlr);
    
    // Find which reactor this controller belongs to
    let state = find_reactor_for_controller(req.ctrlr);
    
    eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}] found state={:?}", state.as_ref().map(|s| s.thread));
    
    let qpair = spdk_ffi::curvine_spdk_alloc_io_qpair(req.ctrlr);
    
    eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}] qpair={:?}", qpair);
    
    if qpair.is_null() {
        eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: FAILED to allocate qpair");
        error!("[Reactor] Failed to allocate qpair on reactor thread, ctrlr={:?}", req.ctrlr);
        let mut out = req.qpair_out.lock().unwrap();
        *out = None;
        req.completion.complete(-1);
        return;
    }
    
    eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: SUCCESS allocated qpair={:?}", qpair);
    info!("[Reactor] Qpair allocated on reactor thread: {:?}", qpair);
    
    // CRITICAL: Add qpair to reactor state's qpairs list so poller can process it
    if let Some(ref state) = state {
        let mut qpairs = state.qpairs.lock().unwrap();
        qpairs.push(qpair);
        eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: Added to state, qpairs.len={}", qpairs.len());
    } else {
        eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: WARNING: could not find reactor state for ctrlr");
    }
    
    let mut out = req.qpair_out.lock().unwrap();
    *out = Some(qpair);
    req.completion.complete(0);
}

/// Message to free qpair on reactor thread (spdk_native_reactor only)
#[cfg(feature = "spdk_native_reactor")]
pub struct QpairFreeRequest {
    pub qpair: *mut spdk_ffi::spdk_nvme_qpair,
}

/// Message handler to free qpair on reactor thread.
#[cfg(feature = "spdk_native_reactor")]
pub unsafe extern "C" fn qpair_free_handler(arg: *mut c_void) {
    let req = Box::from_raw(arg as *mut QpairFreeRequest);
    if !req.qpair.is_null() {
        spdk_ffi::curvine_spdk_free_io_qpair(req.qpair);
        info!("[Reactor] Qpair freed on reactor thread: {:?}", req.qpair);
    }
}

/// Submit a single I/O request (safe to call from native reactor thread).
/// Exported for use by spdk_bdev.rs direct submission path.
pub fn submit_one(req: &IoRequest, _active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>) {
    let qpair = match &req.op {
        IoOp::Read { qpair, .. } => *qpair,
        IoOp::Write { qpair, .. } => *qpair,
        IoOp::Flush { qpair, .. } => *qpair,
        IoOp::UnregisterQpair { .. } => {
            unreachable!("UnregisterQpair handled separately");
        }
    };

    // Box::into_raw ensures CallbackCtx survives until poller_callback reclaims it
    let cb_ctx = Box::new(CallbackCtx {
        completion: req.completion.clone(),
        async_ctx: unsafe { std::mem::zeroed() },
        bdev_inflight: req.bdev_inflight.clone(),
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
            unreachable!("UnregisterQpair handled separately");
        }
    };

    if rc != 0 {
        // Submission failed - reclaim allocation and complete with error
        unsafe { drop(Box::from_raw(cb_ctx_ptr)) };
        req.bdev_inflight.fetch_sub(1, Ordering::Release);
        req.completion.complete(rc);
        return;
    }
}
