//! SPDK block device I/O — implements BlockIO for NVMe-oF/RDMA.
//! Mirrors LocalFile API but uses SPDK bdev layer instead of kernel I/O.
//!
//! # Alignment: all I/O aligned to block size.
//! # Threading: I/O forwarded to dedicated poller thread via channel.
//!
//! # TODOs
//! - Reliability: NVMe retry (exp backoff via SpdkConf::io_retry_count). Retriable: SCT=0x03,0x00 SC=0x02/0x0A.
//! - Memory: DMA buffer size configurable via SpdkConf::dma_buf_size.
//! - Perf: zero-copy read for large sequential reads.

#![cfg(feature = "spdk")]

use crate::err_box;
use crate::io::block_io::BlockIO;
use crate::io::IOResult;
use crate::sys::DataSlice;
use bytes::BytesMut;
use log::{debug, error, warn};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
// ---------------------------------------------------------------------------
// SPDK I/O channel — bridge between Tokio and SPDK reactor
// ---------------------------------------------------------------------------

/// SPDK I/O channel — wraps qpair and poller sender.
pub struct SpdkIoChannel {
    pub qpair: *mut crate::io::spdk_ffi::spdk_nvme_qpair,
    pub poller_tx: crossbeam::channel::Sender<crate::io::spdk_poller::IoRequest>,
    /// Eventfd for waking poller on new I/O
    pub eventfd: std::sync::Arc<nix::sys::eventfd::EventFd>,
    /// Flag: poller is idle and blocked on eventfd; only write eventfd when true
    pub poller_is_sleeping: std::sync::Arc<AtomicBool>,
}
unsafe impl Send for SpdkIoChannel {}
unsafe impl Sync for SpdkIoChannel {}

/// DMA buffer (hugepage-backed).
pub struct DmaBuf {
    ptr: *mut std::ffi::c_void,
    capacity: usize,
    block_size: u32,
}

// SAFETY: exclusively owned, not aliased
unsafe impl Send for DmaBuf {}
unsafe impl Sync for DmaBuf {}

impl DmaBuf {
    /// Allocate DMA buffer (aligned to block_size, hugepage-backed).
    pub fn alloc(size: usize, block_size: u32) -> IOResult<Self> {
        if block_size == 0 || !block_size.is_power_of_two() {
            return err_box!("block_size must be a power of two, got {}", block_size);
        }

        // Round up to block alignment
        let aligned_size = Self::align_up(size, block_size as usize);
        let ptr = unsafe {
            crate::io::spdk_ffi::curvine_spdk_dma_malloc(aligned_size as u64, block_size as u64)
        };
        if ptr.is_null() {
            return err_box!(
                "curvine_spdk_dma_malloc failed: size={}, align={}",
                aligned_size,
                block_size
            );
        }
        Ok(Self {
            ptr,
            capacity: aligned_size,
            block_size,
        })
    }

    /// Round n up to nearest multiple of align.
    #[inline]
    pub fn align_up(n: usize, align: usize) -> usize {
        (n + align - 1) & !(align - 1)
    }

    /// Raw pointer for FFI.
    #[inline]
    pub fn as_ptr(&self) -> *mut std::ffi::c_void {
        self.ptr
    }

    /// Usable slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.capacity) }
    }

    /// Usable mutable slice.
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.capacity) }
    }

    /// Allocated capacity (aligned to block_size).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Block size.
    #[inline]
    pub fn block_size(&self) -> u32 {
        self.block_size
    }
}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { crate::io::spdk_ffi::curvine_spdk_dma_free(self.ptr) }
        }
    }
}

/// Pool of DMA buffers for pipelined async I/O.
/// Pre-allocates `depth` buffers; semaphore limits concurrent usage.
pub struct DmaPool {
    bufs: Vec<DmaBuf>,
    sem: tokio::sync::Semaphore,
    next: std::sync::atomic::AtomicUsize,
    buf_capacity: usize,
}

unsafe impl Send for DmaPool {}
unsafe impl Sync for DmaPool {}

impl DmaPool {
    pub fn new(depth: usize, buf_size: usize, block_size: u32) -> IOResult<Self> {
        let mut bufs = Vec::with_capacity(depth);
        for _ in 0..depth {
            bufs.push(DmaBuf::alloc(buf_size, block_size)?);
        }
        Ok(Self {
            bufs,
            sem: tokio::sync::Semaphore::new(depth),
            next: std::sync::atomic::AtomicUsize::new(0),
            buf_capacity: buf_size,
        })
    }

    /// Acquire a buffer from the pool, blocking if all are in use.
    /// Returns (index, raw pointer). Caller must call `release()` after use.
    pub async fn acquire(&self) -> (usize, *mut std::ffi::c_void) {
        let _permit = self.sem.acquire().await;
        let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.bufs.len();
        (idx, self.bufs[idx].as_ptr())
    }

    /// Return a buffer to the pool after I/O completes.
    pub fn release(&self, _idx: usize) {
        self.sem.add_permits(1);
    }

    pub fn buf_capacity(&self) -> usize {
        self.buf_capacity
    }

    pub fn depth(&self) -> usize {
        self.bufs.len()
    }
}

/// Opened SPDK block device. Analogous to LocalFile.
/// Tracks position and size; I/O via SpdkIoChannel.
pub struct SpdkBdev {
    /// Bdev name (e.g., "Nvme0n1" or "NvmeOF0n1").
    name: String,

    /// Total device size in bytes.
    size: i64,
    /// Block size.
    block_size: u32,
    /// Current cursor position.
    pos: i64,
    /// Whether opened for writing.
    writable: bool,
    /// Pool of DMA buffers for pipelined I/O.
    dma_pool: DmaPool,
    /// Raw namespace pointer.
    ns: *mut crate::io::spdk_ffi::spdk_nvme_ns,
    /// I/O channel for this bdev.
    io_channel: SpdkIoChannel,
    /// Raw controller pointer (for qpair pool on drop).
    ctrlr: *mut crate::io::spdk_ffi::spdk_nvme_ctrlr,
    /// I/O timeout in microseconds. 0 = no timeout.
    io_timeout_us: u64,
    inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

// SAFETY: owns qpair and DMA buffers exclusively
unsafe impl Send for SpdkBdev {}
unsafe impl Sync for SpdkBdev {}
impl SpdkBdev {
    // mirror LocalFile::with_read/with_write

    pub fn open_read(name: &str, offset: u64, max_len: i64) -> IOResult<Self> {
        Self::open(name, offset as i64, false, max_len)
    }

    pub fn open_write(name: &str, offset: i64, max_len: i64) -> IOResult<Self> {
        Self::open(name, offset, true, max_len)
    }

    fn open(name: &str, offset: i64, writable: bool, max_len: i64) -> IOResult<Self> {
        // Look up bdev metadata from SpdkEnv
        {
            use crate::io::spdk_env::SpdkEnv;
            use crate::io::spdk_ffi;
            let env = SpdkEnv::global_or_err()?;
            // Register handle so shutdown knows we're alive
            env.acquire_handle()?;
            let info = match env.get_bdev(name) {
                Some(v) => v,
                None => {
                    env.release_handle();
                    return err_box!("bdev '{}' not found in SpdkEnv", name);
                }
            };

            let bdev_size = info.size_bytes as i64;
            let block_size = info.block_size;

            // Cap size to caller's allocated range
            let size = if max_len > 0 {
                let end = offset.saturating_add(max_len);
                end.min(bdev_size)
            } else {
                bdev_size
            };
            if offset < 0 || offset > size {
                env.release_handle();
                return err_box!(
                    "offset {} out of range for bdev '{}' (size={})",
                    offset,
                    name,
                    size
                );
            }

            // DMA buffer pool for pipelined async I/O
            let buf_size = DmaBuf::align_up(1024 * 1024, block_size as usize);
            let pipeline_depth = env.conf().pipeline_depth.max(1) as usize;
            let dma_pool = match DmaPool::new(pipeline_depth, buf_size, block_size) {
                Ok(pool) => pool,
                Err(e) => {
                    env.release_handle();
                    return Err(e);
                }
            };

            // Recover raw SPDK pointers from BdevInfo
            let ctrlr = info.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr;
            let ns = info.ns as *mut spdk_ffi::spdk_nvme_ns;
            // Acquire a qpair from the pool (reuses a cached one or allocates new).
            let qpair = match env.acquire_qpair(ctrlr) {
                Ok(qp) => qp,
                Err(e) => {
                    env.release_handle();
                    return Err(e.into());
                }
            };
            let poller_tx = env.poller_sender(); // Get sender from SpdkEnv
            let eventfd = env.poller_eventfd(); // Get eventfd for wake signaling
            let poller_is_sleeping = env.poller_is_sleeping(); // Skip eventfd if active
            let io_channel = SpdkIoChannel {
                qpair,
                poller_tx,
                eventfd,
                poller_is_sleeping,
            };
            let io_timeout_us = env.conf().io_timeout_us;
            Ok(Self {
                name: name.to_string(),
                size,
                block_size,
                pos: offset,
                writable,
                dma_pool,
                io_channel,
                ns,
                ctrlr,
                io_timeout_us,
                inflight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            })
        }
    }

    /// Device block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Whether this handle is opened for writing.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Device name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Align offset down to block boundary.
    #[inline]
    fn align_down(&self, n: i64) -> i64 {
        let bs = self.block_size as i64;
        n & !(bs - 1)
    }

    /// Decode NVMe error from C callback.
    fn nvme_error_str(rc: i32) -> String {
        if rc >= 0 {
            return "success".to_string();
        }
        let code = (-rc) as u32;
        let sct = code / 256;
        let sc = code % 256;
        let sct_name = match sct {
            0 => "Generic",
            1 => "CmdSpecific",
            2 => "Media",
            3 => "Path",
            7 => "Vendor",
            _ => "Unknown",
        };
        format!(
            "NVMe error: SCT=0x{:02x}({}), SC=0x{:02x}",
            sct, sct_name, sc
        )
    }

    /// Check that the current position + len doesn't exceed device size.
    fn check_bounds(&self, len: i64) -> IOResult<()> {
        if self.pos + len > self.size {
            return err_box!(
                "I/O range [{}, {}) exceeds bdev '{}' size {}",
                self.pos,
                self.pos + len,
                self.name,
                self.size
            );
        }
        Ok(())
    }

    /// Submit read to SPDK, wait for completion. Uses DMA buffer (large reads chunked).
    fn spdk_read(&mut self, offset: i64, len: usize) -> IOResult<BytesMut> {
        let buf_cap = self.dma_pool.buf_capacity();
        let dma_buf = self.dma_pool.bufs[0].as_ptr();

        let bs = self.block_size as usize;
        let mut result = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut cur_off = offset;
        while remaining > 0 {
            // Align down, compute head skip
            let aligned_off = self.align_down(cur_off);
            let head_skip = (cur_off - aligned_off) as usize;
            // Usable payload = buf_cap - head_skip
            debug_assert!(buf_cap > head_skip, "DMA buffer too small");
            let chunk_data = remaining.min(buf_cap - head_skip);
            let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs);

            // Submit NVMe read
            use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest};
            let completion = IoCompletion::new();
            self.inflight
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            let req = IoRequest {
                op: IoOp::Read {
                    ns: self.ns,
                    qpair: self.io_channel.qpair,
                    buf: dma_buf,
                    offset: aligned_off as u64,
                    num_bytes: aligned_len as u64,
                },
                completion: completion.clone(),
                bdev_inflight: self.inflight.clone(),
            };
            if self.io_channel.poller_tx.send(req).is_err() {
                self.inflight
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
                return err_box!("SPDK poller thread is gone");
            }
            if self.io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                if let Err(e) = self.io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                         I/O may be delayed until timeout.",
                        self.name, e
                    );
                }
            }
            let rc = completion.wait(self.io_timeout_us);
            if rc != 0 {
                return err_box!(
                    "NVMe read failed on '{}' at offset={}: {}",
                    self.name,
                    aligned_off,
                    Self::nvme_error_str(rc)
                );
            }
            // Copy to heap (DMA buffer reused)
            let slice = unsafe {
                std::slice::from_raw_parts((dma_buf as *const u8).add(head_skip), chunk_data)
            };
            result.extend_from_slice(slice);
            cur_off += chunk_data as i64;
            remaining -= chunk_data;
        }
        Ok(result)
    }

    /// Submit write to SPDK, wait for completion. Uses DMA buffer (large writes chunked).
    fn spdk_write(&mut self, offset: i64, data: &[u8]) -> IOResult<()> {
        use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest};

        if !self.writable {
            return err_box!("bdev '{}' not opened for writing", self.name);
        }

        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        let buf_cap = self.dma_pool.buf_capacity();
        let dma_buf = self.dma_pool.bufs[0].as_ptr();

        let bs = self.block_size as usize;
        let mut written = 0usize;
        let mut cur_off = offset;
        while written < len {
            // Align current offset down and compute head skip for this chunk
            let aligned_off = self.align_down(cur_off);
            let head_skip = (cur_off - aligned_off) as usize;
            // How much user data fits in this iteration?
            let chunk_data = (len - written).min(buf_cap - head_skip);
            let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs);
            // Read-modify-write if the chunk doesn't cover full aligned blocks
            if head_skip > 0 || (chunk_data + head_skip) < aligned_len {
                let completion = IoCompletion::new();
                self.inflight
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                let req = IoRequest {
                    op: IoOp::Read {
                        ns: self.ns,
                        qpair: self.io_channel.qpair,
                        buf: dma_buf,
                        offset: aligned_off as u64,
                        num_bytes: aligned_len as u64,
                    },
                    completion: completion.clone(),
                    bdev_inflight: self.inflight.clone(),
                };
                if self.io_channel.poller_tx.send(req).is_err() {
                    self.inflight
                        .fetch_sub(1, std::sync::atomic::Ordering::Release);
                    return err_box!("SPDK poller thread is gone");
                }
                if self.io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                    if let Err(e) = self.io_channel.eventfd.write(1) {
                        warn!(
                            "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                             I/O may be delayed until timeout.",
                            self.name, e
                        );
                    }
                }
                let rc = completion.wait(self.io_timeout_us);

                if rc != 0 {
                    return err_box!(
                        "Read-modify-write: read failed on '{}' at offset={}: {}",
                        self.name,
                        aligned_off,
                        Self::nvme_error_str(rc)
                    );
                }
            }
            // Copy caller's data into the DMA buffer at the correct offset
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data[written..].as_ptr(),
                    (dma_buf as *mut u8).add(head_skip),
                    chunk_data,
                );
            }
            // Submit synchronous NVMe write
            let completion = IoCompletion::new();
            self.inflight
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            let req = IoRequest {
                op: IoOp::Write {
                    ns: self.ns,
                    qpair: self.io_channel.qpair,
                    buf: dma_buf,
                    offset: aligned_off as u64,
                    num_bytes: aligned_len as u64,
                },
                completion: completion.clone(),
                bdev_inflight: self.inflight.clone(),
            };
            if self.io_channel.poller_tx.send(req).is_err() {
                self.inflight
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
                return err_box!("SPDK poller thread is gone");
            }
            if self.io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                if let Err(e) = self.io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                         I/O may be delayed until timeout.",
                        self.name, e
                    );
                }
            }
            let rc = completion.wait(self.io_timeout_us);
            if rc != 0 {
                return err_box!(
                    "NVMe write failed on '{}' at offset={}, len={}: {}",
                    self.name,
                    aligned_off,
                    aligned_len,
                    Self::nvme_error_str(rc)
                );
            }
            cur_off += chunk_data as i64;
            written += chunk_data;
        }

        Ok(())
    }

    /// Async read with pipeline: submits chunks in batches of pool depth,
    /// allowing NVMe controller to process multiple commands concurrently.
    async fn spdk_read_async(&mut self, offset: i64, len: usize) -> IOResult<BytesMut> {
        use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest};

        let dma_pool = &self.dma_pool;
        let io_channel = &self.io_channel;
        let inflight = &self.inflight;
        let io_timeout_us = self.io_timeout_us;
        let name = &self.name;
        let bs = self.block_size as i64;
        let buf_cap = dma_pool.buf_capacity();
        let pool_depth = dma_pool.depth();

        let mut result = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut cur_off = offset;
        while remaining > 0 {
            // Plan a batch of up to pool_depth chunks
            let remaining_in_range = remaining;
            let batch_n = remaining_in_range.div_ceil(buf_cap).min(pool_depth);
            let mut batch: Vec<(
                usize, // buf_idx
                i64,   // aligned_off
                usize, // head_skip
                usize, // chunk_data
                usize, // aligned_len
                Arc<IoCompletion>,
            )> = Vec::with_capacity(batch_n);

            for _ in 0..batch_n {
                let aligned_off = cur_off & !(bs - 1);
                let head_skip = (cur_off - aligned_off) as usize;
                let chunk_data = remaining.min(buf_cap - head_skip);
                let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs as usize);

                let (buf_idx, buf_ptr) = dma_pool.acquire().await;
                let completion = IoCompletion::new();
                inflight.fetch_add(1, std::sync::atomic::Ordering::Release);
                let req = IoRequest {
                    op: IoOp::Read {
                        ns: self.ns,
                        qpair: io_channel.qpair,
                        buf: buf_ptr,
                        offset: aligned_off as u64,
                        num_bytes: aligned_len as u64,
                    },
                    completion: completion.clone(),
                    bdev_inflight: inflight.clone(),
                };
                if io_channel.poller_tx.send(req).is_err() {
                    inflight.fetch_sub(1, std::sync::atomic::Ordering::Release);
                    return err_box!("SPDK poller thread is gone");
                }
                batch.push((
                    buf_idx,
                    aligned_off,
                    head_skip,
                    chunk_data,
                    aligned_len,
                    completion,
                ));
                cur_off += chunk_data as i64;
                remaining -= chunk_data;
            }

            // Wake poller once after batch submit
            if io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                if let Err(e) = io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                         I/O may be delayed until timeout.",
                        name, e
                    );
                }
            }

            // Await all completions in this batch (in order)
            for (buf_idx, _aligned_off, head_skip, chunk_data, _aligned_len, completion) in &batch {
                let rc: i32 = completion.wait_async(io_timeout_us).await;
                if rc != 0 {
                    return err_box!(
                        "NVMe read failed on '{}' at offset={}: {}",
                        name,
                        _aligned_off,
                        Self::nvme_error_str(rc)
                    );
                }
                // Copy from DMA buf to heap (DMA buffers can be reused after)
                let buf_ptr = dma_pool.bufs[*buf_idx].as_ptr();
                let slice = unsafe {
                    std::slice::from_raw_parts((buf_ptr as *const u8).add(*head_skip), *chunk_data)
                };
                result.extend_from_slice(slice);
                dma_pool.release(*buf_idx);
            }
        }
        Ok(result)
    }

    /// Async write with pipeline: processes chunks in batches of pool depth.
    /// Within each batch: submits all RMW reads in parallel, then all writes in parallel.
    async fn spdk_write_async(&mut self, offset: i64, data: &[u8]) -> IOResult<()> {
        use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest};

        if !self.writable {
            return err_box!("bdev '{}' not opened for writing", self.name);
        }

        let len = data.len();
        if len == 0 {
            return Ok(());
        }

        let dma_pool = &self.dma_pool;
        let io_channel = &self.io_channel;
        let inflight = &self.inflight;
        let io_timeout_us = self.io_timeout_us;
        let name = &self.name;
        let bs = self.block_size as i64;
        let buf_cap = dma_pool.buf_capacity();
        let pool_depth = dma_pool.depth();

        let mut written = 0usize;
        let mut cur_off = offset;
        while written < len {
            let remaining = len - written;
            let batch_n = remaining.div_ceil(buf_cap).min(pool_depth);

            // Plan batch chunks
            struct BatchChunk {
                buf_idx: usize,
                aligned_off: i64,
                head_skip: usize,
                chunk_data: usize,
                aligned_len: usize,
                needs_rmw: bool,
                completion: Option<Arc<IoCompletion>>,
            }
            let mut chunks: Vec<BatchChunk> = Vec::with_capacity(batch_n);
            let mut bytes_in_batch = 0usize;
            for _ in 0..batch_n {
                let aligned_off = cur_off & !(bs - 1);
                let head_skip = (cur_off - aligned_off) as usize;
                let chunk_data = (len - written - bytes_in_batch).min(buf_cap - head_skip);
                let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs as usize);
                let needs_rmw = head_skip > 0 || (chunk_data + head_skip) < aligned_len;

                let (buf_idx, _buf_ptr) = dma_pool.acquire().await;
                chunks.push(BatchChunk {
                    buf_idx,
                    aligned_off,
                    head_skip,
                    chunk_data,
                    aligned_len,
                    needs_rmw,
                    completion: None,
                });
                bytes_in_batch += chunk_data;
                cur_off += chunk_data as i64;
            }
            let cur_off_saved = cur_off;
            let written_saved = written;

            // Phase 1: Submit all RMW reads in parallel
            let mut rmw_count = 0;
            for i in 0..chunks.len() {
                if !chunks[i].needs_rmw {
                    continue;
                }
                let completion = IoCompletion::new();
                inflight.fetch_add(1, std::sync::atomic::Ordering::Release);
                let req = IoRequest {
                    op: IoOp::Read {
                        ns: self.ns,
                        qpair: io_channel.qpair,
                        buf: dma_pool.bufs[chunks[i].buf_idx].as_ptr(),
                        offset: chunks[i].aligned_off as u64,
                        num_bytes: chunks[i].aligned_len as u64,
                    },
                    completion: completion.clone(),
                    bdev_inflight: inflight.clone(),
                };
                if io_channel.poller_tx.send(req).is_err() {
                    inflight.fetch_sub(1, std::sync::atomic::Ordering::Release);
                    return err_box!("SPDK poller thread is gone");
                }
                chunks[i].completion = Some(completion);
                rmw_count += 1;
            }

            // Wake poller once for RMW reads
            if rmw_count > 0 && io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                if let Err(e) = io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}.",
                        name, e
                    );
                }
            }

            // Await all RMW reads
            for chunk in &chunks {
                if let Some(ref completion) = chunk.completion {
                    let rc: i32 = completion.wait_async(io_timeout_us).await;
                    if rc != 0 {
                        return err_box!(
                            "Read-modify-write: read failed on '{}' at offset={}: {}",
                            name,
                            chunk.aligned_off,
                            Self::nvme_error_str(rc)
                        );
                    }
                }
            }

            // Phase 2: Copy user data into DMA buffers
            let mut batch_offset = written_saved;
            for chunk in &chunks {
                let buf_ptr = dma_pool.bufs[chunk.buf_idx].as_ptr();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data[batch_offset..].as_ptr(),
                        (buf_ptr as *mut u8).add(chunk.head_skip),
                        chunk.chunk_data,
                    );
                }
                batch_offset += chunk.chunk_data;
            }

            // Phase 3: Submit all writes in parallel
            let mut write_completions: Vec<(usize, Arc<IoCompletion>)> =
                Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                let completion = IoCompletion::new();
                inflight.fetch_add(1, std::sync::atomic::Ordering::Release);
                let req = IoRequest {
                    op: IoOp::Write {
                        ns: self.ns,
                        qpair: io_channel.qpair,
                        buf: dma_pool.bufs[chunk.buf_idx].as_ptr(),
                        offset: chunk.aligned_off as u64,
                        num_bytes: chunk.aligned_len as u64,
                    },
                    completion: completion.clone(),
                    bdev_inflight: inflight.clone(),
                };
                if io_channel.poller_tx.send(req).is_err() {
                    inflight.fetch_sub(1, std::sync::atomic::Ordering::Release);
                    return err_box!("SPDK poller thread is gone");
                }
                write_completions.push((chunk.buf_idx, completion));
            }

            // Wake poller once for writes
            if io_channel.poller_is_sleeping.load(Ordering::Acquire) {
                if let Err(e) = io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}.",
                        name, e
                    );
                }
            }

            // Await all writes
            for (buf_idx, completion) in &write_completions {
                let rc: i32 = completion.wait_async(io_timeout_us).await;
                if rc != 0 {
                    return err_box!(
                        "NVMe write failed on '{}' at offset={}: {}",
                        name,
                        0i64,
                        Self::nvme_error_str(rc)
                    );
                }
                dma_pool.release(*buf_idx);
            }

            cur_off = cur_off_saved;
            written = written_saved + bytes_in_batch;
        }

        Ok(())
    }

    /// Async read_region: same as read_region but async.
    pub async fn async_read_region(&mut self, len: i32) -> IOResult<DataSlice> {
        if len <= 0 {
            return Ok(DataSlice::Empty);
        }
        let chunk = (len as i64).min(self.size - self.pos);
        if chunk <= 0 {
            return err_box!(
                "offset exceeds bdev length, length={}, offset={}",
                self.size,
                self.pos
            );
        }
        let buf = self.spdk_read_async(self.pos, chunk as usize).await?;
        self.pos += chunk;
        Ok(DataSlice::Buffer(buf))
    }

    /// Async write_region: same as write_region but async.
    pub async fn async_write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        let data: &[u8] = match region {
            DataSlice::Empty => return Ok(()),
            DataSlice::Buffer(bytes) => bytes,
            DataSlice::IOSlice(_) => {
                return err_box!(
                    "DataSlice::IOSlice not supported for SPDK bdev '{}'.",
                    self.name
                );
            }
            DataSlice::MemSlice(bytes) => bytes.as_slice(),
            DataSlice::Bytes(bytes) => bytes,
        };

        if data.is_empty() {
            return Ok(());
        }

        self.check_bounds(data.len() as i64)?;
        self.spdk_write_async(self.pos, data).await?;
        self.pos += data.len() as i64;
        Ok(())
    }

    fn spdk_flush(&self) -> IOResult<()> {
        use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest};
        let completion = IoCompletion::new();
        self.inflight
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let req = IoRequest {
            op: IoOp::Flush {
                ns: self.ns,
                qpair: self.io_channel.qpair,
            },
            completion: completion.clone(),
            bdev_inflight: self.inflight.clone(),
        };
        if self.io_channel.poller_tx.send(req).is_err() {
            return err_box!("SPDK poller thread is gone");
        }
        if self.io_channel.poller_is_sleeping.load(Ordering::Acquire) {
            if let Err(e) = self.io_channel.eventfd.write(1) {
                warn!(
                    "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                     I/O may be delayed until timeout.",
                    self.name, e
                );
            }
        }
        let rc = completion.wait(self.io_timeout_us);
        if rc != 0 {
            return err_box!(
                "NVMe flush failed on '{}': {}",
                self.name,
                Self::nvme_error_str(rc)
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BlockIO implementation — the core trait
// ---------------------------------------------------------------------------

impl BlockIO for SpdkBdev {
    /// Read up to len bytes from current position. Returns DataSlice::Buffer.
    fn read_region(&mut self, _enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let chunk = (len as i64).min(self.size - self.pos);
        if chunk <= 0 {
            return err_box!(
                "offset exceeds bdev length, length={}, offset={}",
                self.size,
                self.pos
            );
        }

        let buf = self.spdk_read(self.pos, chunk as usize)?;
        self.pos += chunk;
        Ok(DataSlice::Buffer(buf))
    }

    /// Write DataSlice at current position. Handles all except IOSlice (fd not for SPDK).
    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        let data: &[u8] = match region {
            DataSlice::Empty => return Ok(()),

            DataSlice::Buffer(bytes) => bytes,

            DataSlice::IOSlice(_) => {
                return err_box!(
                    "DataSlice::IOSlice not supported for SPDK bdev '{}'.
                     IOSlice wraps a kernel fd which cannot be used with userspace SPDK.",
                    self.name
                );
            }

            DataSlice::MemSlice(bytes) => bytes.as_slice(),

            DataSlice::Bytes(bytes) => bytes,
        };

        if data.is_empty() {
            return Ok(());
        }

        self.check_bounds(data.len() as i64)?;
        self.spdk_write(self.pos, data)?;
        self.pos += data.len() as i64;
        Ok(())
    }

    /// Write all bytes from buf at current position.
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        self.check_bounds(buf.len() as i64)?;
        self.spdk_write(self.pos, buf)?;
        self.pos += buf.len() as i64;
        Ok(())
    }

    /// Read exactly buf.len() bytes into buf.
    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        self.check_bounds(buf.len() as i64)?;
        let data = self.spdk_read(self.pos, buf.len())?;
        buf.copy_from_slice(&data);
        self.pos += buf.len() as i64;
        Ok(())
    }

    /// Flush writes to NVMe device.
    fn flush(&mut self) -> IOResult<()> {
        self.spdk_flush()
    }

    /// Seek to absolute offset.
    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        if pos == self.pos {
            return Ok(pos);
        }

        if pos < 0 || pos > self.size {
            return err_box!(
                "seek position {} out of range for bdev '{}' (size={})",
                pos,
                self.name,
                self.size
            );
        }

        self.pos = pos;
        Ok(self.pos)
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn len(&self) -> i64 {
        self.size
    }

    fn path(&self) -> &str {
        &self.name
    }

    /// Resize not supported — NVMe namespaces have fixed capacity.
    fn resize(&mut self, _truncate: bool, _off: i64, _len: i64, _mode: i32) -> IOResult<()> {
        err_box!(
            "resize not supported for SPDK bdev '{}'.
             NVMe namespaces have fixed capacity ({})",
            self.name,
            self.size
        )
    }
}

impl Display for SpdkBdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkBdev({}, size={}, block_size={}, pos={}, writable={})",
            self.name, self.size, self.block_size, self.pos, self.writable
        )
    }
}

impl Drop for SpdkBdev {
    fn drop(&mut self) {
        // Wait for in-flight I/O.
        let max_wait = if self.io_timeout_us > 0 {
            std::time::Duration::from_micros(self.io_timeout_us * 2)
        } else {
            std::time::Duration::from_secs(60)
        };
        let deadline = std::time::Instant::now() + max_wait;
        let mut logged = false;
        loop {
            let count = self.inflight.load(std::sync::atomic::Ordering::Acquire);
            if count == 0 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                error!(
                    "SpdkBdev '{}': {} in-flight I/O(s) still pending after {}s. \
                         Leaking DMA buffers to prevent use-after-free.",
                    self.name,
                    count,
                    max_wait.as_secs()
                );
                // Poison DMA pool so buffers are no-ops on drop.
                // TODO: leak qpair and handle on timeout because reusing a qpair with orphaned callbacks causes use after free.
                break;
            }
            if !logged {
                warn!(
                    "SpdkBdev '{}': waiting for {} in-flight I/O(s) to complete before drop",
                    self.name, count
                );
                logged = true;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Return qpair to pool and release handle.
        if let Some(env) = crate::io::spdk_env::SpdkEnv::global_including_shutdown() {
            // Unregister qpair from poller before returning it to pool to avoid use-after-free
            let unregistered = env.unregister_qpair_from_poller(self.io_channel.qpair);
            if unregistered {
                env.release_qpair(self.ctrlr, self.io_channel.qpair);
            } else {
                error!(
                    "SpdkBdev '{}': qpair not unregistered, leaking to prevent UAF",
                    self.name
                );
            }
            env.release_handle();
        } else {
            unsafe {
                crate::io::spdk_ffi::curvine_spdk_free_io_qpair(self.io_channel.qpair);
            }
        }
        debug!(
            "SpdkBdev '{}' closed (qpair returned to pool, DMA buffers freed)",
            self.name
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::spdk_env::{SpdkConf, SpdkEnv};

    fn ensure_spdk_init() {
        if SpdkEnv::global().is_some() {
            return;
        }
        let mut conf = SpdkConf::default();
        conf.enabled = true;
        conf.app_name = "curvine-test".to_string();
        conf.hugepage_mb = 64;
        conf.no_huge = true;
        conf.reactor_mask =
            std::env::var("SPDK_REACTOR_MASK").unwrap_or_else(|_| "0x1".to_string());
        let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let trsvcid = std::env::var("SPDK_TARGET_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4420);
        let subnqn = std::env::var("SPDK_TARGET_NQN")
            .unwrap_or_else(|_| "nqn.2024-01.io.curvine:test".into());
        let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or_else(|_| "tcp".into());
        conf.targets = vec![crate::io::spdk_env::NvmeTarget {
            traddr,
            trsvcid,
            subnqn,
            trtype,
            adrfam: "ipv4".to_string(),
            ..Default::default()
        }];
        SpdkEnv::init_global(conf).expect("SPDK init for tests");
    }

    #[test]
    fn dma_buf_alloc_aligned() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(1000, 512).unwrap();
        // 1000 rounded up to 512 alignment = 1024
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.block_size(), 512);
    }
    #[test]
    fn dma_buf_as_slice_roundtrip() {
        ensure_spdk_init();
        let mut buf = DmaBuf::alloc(4096, 4096).unwrap();
        assert_eq!(buf.capacity(), 4096);
        // Write a pattern and read it back
        buf.as_slice_mut()[0] = 0xAB;
        buf.as_slice_mut()[4095] = 0xCD;
        assert_eq!(buf.as_slice()[0], 0xAB);
        assert_eq!(buf.as_slice()[4095], 0xCD);
    }
    #[test]
    fn dma_buf_fixed_capacity() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(4096, 512).unwrap();
        // Buffer is fixed-size — capacity and pointer never change
        assert_eq!(buf.capacity(), 4096);
        assert!(!buf.as_ptr().is_null());
    }
    #[test]
    fn dma_buf_invalid_block_size() {
        // block_size = 0
        assert!(DmaBuf::alloc(1024, 0).is_err());
        // block_size not power of two
        assert!(DmaBuf::alloc(1024, 3).is_err());
    }
    #[test]
    fn dma_buf_drop_does_not_leak() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(1024 * 1024, 4096).unwrap();
        drop(buf);
    }
}
