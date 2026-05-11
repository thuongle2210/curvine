//! SPDK I/O poller — handles NVMe submit/poll on dedicated thread.
//! Uses SPDK native reactor threads with round-robin controller-to-reactor mapping.
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use crate::io::spdk_ffi;
use futures::task::AtomicWaker;
use log::{error, info};
use std::ffi::{c_int, c_void};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};

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

/// Message to create qpair on reactor thread.
pub struct QpairCreateRequest {
    pub ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    pub qpair_out: std::sync::Arc<std::sync::Mutex<Option<*mut spdk_ffi::spdk_nvme_qpair>>>,
    pub completion: Arc<IoCompletion>,
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoRequest {}

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
/// Submits the NVMe command then runs one reactor poll cycle to process completions
/// that arrived immediately (common over localhost TCP), avoiding wait for next reactor iteration.
pub unsafe extern "C" fn spdk_native_reactor_msg_handler(arg: *mut c_void) {
    let req = Box::from_raw(arg as *mut IoRequest);
    submit_one(&req, &mut Vec::new());

    // One non-blocking poll cycle to process completions that arrived near-instantly.
    // This calls all registered pollers (SPDK transport, reactor_poller_cb, etc.)
    // and processes any queued messages. Returns immediately (~1μs) if nothing pending.
    let thread = spdk_ffi::spdk_get_thread();
    spdk_ffi::spdk_thread_poll(thread, 0, 0);
}

/// Register a single poller on the reactor thread for ALL qpairs.
/// Called via spdk_thread_send_msg on the reactor thread.
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
        0, // period_us = 0 means poll every reactor iteration
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
pub unsafe extern "C" fn reactor_poller_cb(arg: *mut c_void) -> c_int {
    use crate::io::spdk_ffi;

    static mut CALLBACK_COUNT: u64 = 0;
    unsafe {
        CALLBACK_COUNT += 1;
    }

    // Cast arg to ReactorState pointer
    let state_ptr = arg as *const crate::io::spdk_env::ReactorState;
    let state = &*state_ptr;
    let qpairs = state.qpairs.lock().unwrap();

    let qpair_count = qpairs.len();

    if unsafe { CALLBACK_COUNT <= 5 } || CALLBACK_COUNT % 1000000 == 0 {
        // eprintln!(
        //     "[Reactor poller #{}] state={:?}, thread={:?}, qpair_count={}",
        //     CALLBACK_COUNT, state_ptr, state.thread, qpair_count
        // );
    }

    if qpair_count == 0 {
        // Only print this occasionally to avoid spam
        if unsafe { CALLBACK_COUNT <= 100 || CALLBACK_COUNT % 1000000 == 0 } {
            // eprintln!(
            //     "[Reactor poller #{}] state={:?}: No qpairs registered!",
            //     CALLBACK_COUNT, state_ptr
            // );
        }
        return 0;
    }

    let mut total_completions = 0i32;

    for &qpair in qpairs.iter() {
        if !qpair.is_null() {
            let rc = spdk_ffi::spdk_nvme_qpair_process_completions(qpair, 0);
            if rc > 0 && unsafe { CALLBACK_COUNT <= 1000 } {
                // eprintln!(
                //     "[Reactor poller #{}] Processed {} completions for qpair {:?}",
                //     CALLBACK_COUNT, rc, qpair
                // );
            } else if rc < 0 {
                // eprintln!(
                //     "[Reactor poller #{}] Error processing qpair {:?}: rc={}",
                //     CALLBACK_COUNT, qpair, rc
                // );
            }
            total_completions += rc;
        } else {
            // eprintln!(
            //     "[Reactor poller #{}] Found NULL qpair in list!",
            //     CALLBACK_COUNT
            // );
        }
    }

    // Return 1 if busy (more completions possible), 0 if idle
    let busy = if total_completions > 0 { 1 } else { 0 };
    if unsafe { CALLBACK_COUNT % 1000000 == 0 } {
        // eprintln!(
        //     "[Reactor poller #{}] Total completions: {} across {} qpairs, busy={}",
        //     CALLBACK_COUNT, total_completions, qpair_count, busy
        // );
    }
    busy
}

/// Message handler to create qpair on reactor thread.
/// SPDK requires qpairs to be created on the thread that processes them.
pub unsafe extern "C" fn qpair_create_handler(arg: *mut c_void) {
    use crate::io::spdk_env::find_reactor_for_controller;
    static mut QPAIR_CREATE_COUNT: u64 = 0;
    unsafe {
        QPAIR_CREATE_COUNT += 1;
    }

    let req = Box::from_raw(arg as *mut QpairCreateRequest);

    // Get current thread to verify we're on the reactor thread
    let current_thread = spdk_ffi::spdk_get_thread();
    eprintln!(
        "[Reactor qpair_create #{QPAIR_CREATE_COUNT}] current_thread={:?}, ctrlr={:?}",
        current_thread, req.ctrlr
    );

    // Find which reactor this controller belongs to
    let state = find_reactor_for_controller(req.ctrlr);

    eprintln!(
        "[Reactor qpair_create #{QPAIR_CREATE_COUNT}] found state={:?}",
        state.as_ref().map(|s| s.thread)
    );

    let qpair = spdk_ffi::curvine_spdk_alloc_io_qpair(req.ctrlr);

    eprintln!(
        "[Reactor qpair_create #{QPAIR_CREATE_COUNT}] qpair={:?}",
        qpair
    );

    if qpair.is_null() {
        eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: FAILED to allocate qpair");
        error!(
            "[Reactor] Failed to allocate qpair on reactor thread, ctrlr={:?}",
            req.ctrlr
        );
        let mut out = req.qpair_out.lock().unwrap();
        *out = None;
        req.completion.complete(-1);
        return;
    }

    eprintln!(
        "[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: SUCCESS allocated qpair={:?}",
        qpair
    );
    info!("[Reactor] Qpair allocated on reactor thread: {:?}", qpair);

    // CRITICAL: Add qpair to reactor state's qpairs list so poller can process it
    if let Some(ref state) = state {
        let mut qpairs = state.qpairs.lock().unwrap();
        qpairs.push(qpair);
        eprintln!(
            "[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: Added to state, qpairs.len={}",
            qpairs.len()
        );
    } else {
        eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}]: WARNING: could not find reactor state for ctrlr");
    }

    let mut out = req.qpair_out.lock().unwrap();
    *out = Some(qpair);
    req.completion.complete(0);
}

/// Message to free qpair on reactor thread.
pub struct QpairFreeRequest {
    pub qpair: *mut spdk_ffi::spdk_nvme_qpair,
}

/// Message handler to free qpair on reactor thread.
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
    };

    if rc != 0 {
        // Submission failed - reclaim allocation and complete with error
        unsafe { drop(Box::from_raw(cb_ctx_ptr)) };
        req.bdev_inflight.fetch_sub(1, Ordering::Release);
        req.completion.complete(rc);
        return;
    }
}
