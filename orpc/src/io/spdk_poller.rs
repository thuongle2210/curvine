//! SPDK I/O poller — handles NVMe submit/poll on dedicated thread.
//! Uses SPDK native reactor threads with round-robin controller-to-reactor mapping.
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use crate::io::spdk_ffi;
use futures::task::AtomicWaker;
use libc;
use log::{error, info};
use std::ffi::{c_int, c_void};
use std::future::Future;
use std::os::unix::io::AsRawFd;
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

/// Poller callback: drains the lock-free I/O channel, submits requests, and
/// polls all qpairs for completions. Runs every `spdk_thread_poll` iteration.
///
/// Replaces the old `spdk_native_reactor_msg_handler` + per-message spin with
/// a batch-oriented approach:
///   1. Drain ALL pending I/O requests from the channel (lock-free)
///   2. Submit each via `submit_one`
///   3. Poll ALL qpairs for completions (catches keep-alive + async completions)
///
/// The lock-free channel replaces `spdk_thread_send_msg` for the hot I/O path,
/// eliminating SPDK message queue lock contention and per-message batch overhead.
pub unsafe extern "C" fn reactor_poller_cb(arg: *mut c_void) -> c_int {
    let state = &*(arg as *const crate::io::spdk_env::ReactorState);

    // Phase 1: Drain all pending I/O requests from the lock-free channel.
    // This replaces spdk_thread_send_msg for the hot I/O path.
    while let Ok(req) = state.io_rx.try_recv() {
        submit_one(&req, &mut Vec::new());
    }

    // Drain eventfd so repeated writes don't accumulate
    let mut buf = [0u8; 8];
    let _ = libc::read(
        state.io_eventfd.as_raw_fd(),
        buf.as_mut_ptr() as *mut libc::c_void,
        8,
    );

    // Phase 2: Poll all registered qpairs for completions.
    // This handles keep-alive, async completions, and the responses
    // to requests submitted in Phase 1.
    let qpairs = state.qpairs.lock().unwrap();
    let mut total = 0i32;
    for &qpair in qpairs.iter() {
        if !qpair.is_null() {
            let rc = spdk_ffi::spdk_nvme_qpair_process_completions(qpair, 0);
            if rc > 0 {
                total += rc;
            }
        }
    }
    drop(qpairs);

    // Return 1 if busy (more completions or more work pending).
    // This keeps spdk_thread_poll "hot" between iterations.
    if total > 0 || !state.io_rx.is_empty() {
        1
    } else {
        0
    }
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

    // Try cache first — avoids ~21ms TCP fabric connect on reopen
    if let Some(ref state) = state {
        let ctrlr_key = req.ctrlr as usize;
        let mut cache = state.qpair_cache.lock().unwrap();
        if let Some(qpairs) = cache.get_mut(&ctrlr_key) {
            if let Some(cached_qpair) = qpairs.pop() {
                // Cache hit! Reuse this qpair.
                eprintln!(
                    "[Reactor qpair_create #{QPAIR_CREATE_COUNT}] cache HIT: reusing qpair={:?}",
                    cached_qpair
                );
                info!("[Reactor] Qpair cache hit, reusing {:?}", cached_qpair);

                // Re-register with poller
                state.qpairs.lock().unwrap().push(cached_qpair);

                let mut out = req.qpair_out.lock().unwrap();
                *out = Some(cached_qpair);
                req.completion.complete(0);
                return;
            }
        }
    }

    // Cache miss — allocate new qpair (~21ms TCP fabric connect)
    eprintln!("[Reactor qpair_create #{QPAIR_CREATE_COUNT}] cache MISS: allocating new qpair");

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

    // Register qpair in reactor state so the poller processes it
    if let Some(ref state) = state {
        state.qpairs.lock().unwrap().push(qpair);
    }

    let mut out = req.qpair_out.lock().unwrap();
    *out = Some(qpair);
    req.completion.complete(0);
}

/// Message to free qpair on reactor thread (or cache it for reuse).
pub struct QpairFreeRequest {
    pub qpair: *mut spdk_ffi::spdk_nvme_qpair,
    /// Controller the qpair belongs to (needed for caching).
    pub ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
}

/// Message handler to free qpair on reactor thread (or cache it for reuse).
pub unsafe extern "C" fn qpair_free_handler(arg: *mut c_void) {
    let req = Box::from_raw(arg as *mut QpairFreeRequest);
    if !req.qpair.is_null() {
        // Remove from reactor state's qpair list so poller stops polling it
        if let Some(states) = crate::io::spdk_env::get_reactor_states().get() {
            if let Ok(guard) = states.lock() {
                for state in guard.iter() {
                    let mut qpairs = state.qpairs.lock().unwrap();
                    qpairs.retain(|&qp| qp != req.qpair);
                }
            }
        }

        // Cache the qpair for reuse instead of freeing it.
        // This avoids ~21ms TCP fabric connect on the next open.
        if let Some(states) = crate::io::spdk_env::get_reactor_states().get() {
            if let Ok(guard) = states.lock() {
                for state in guard.iter() {
                    let controllers = state.controllers.lock().unwrap();
                    if controllers.values().any(|&c| c == req.ctrlr) {
                        drop(controllers);
                        let mut cache = state.qpair_cache.lock().unwrap();
                        let qpairs = cache.entry(req.ctrlr as usize).or_default();
                        const MAX_CACHED: usize = 16;
                        if qpairs.len() < MAX_CACHED {
                            qpairs.push(req.qpair);
                            info!(
                                "[Reactor] Qpair cached for ctrlr {:p} (size {}/{})",
                                req.ctrlr,
                                qpairs.len(),
                                MAX_CACHED
                            );
                        } else {
                            // Pool full — free the qpair
                            spdk_ffi::curvine_spdk_free_io_qpair(req.qpair);
                            info!(
                                "[Reactor] Qpair freed (cache full, {}/{} for ctrlr {:p})",
                                qpairs.len(),
                                MAX_CACHED,
                                req.ctrlr
                            );
                        }
                        return;
                    }
                }
            }
        }

        // Fallback: no reactor state found — free directly
        spdk_ffi::curvine_spdk_free_io_qpair(req.qpair);
        info!(
            "[Reactor] Qpair freed on reactor thread (no cache): {:?}",
            req.qpair
        );
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
