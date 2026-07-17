// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::io::{IOError, IOResult};
use io_uring::squeue::Entry;
use io_uring::types::{Fd, Fixed};
use io_uring::{opcode, IoUring};
use std::os::unix::io::RawFd;

/// Wrapper around `io_uring::IoUring` that provides helper methods for
/// preparing and submitting common file I/O operations.
pub struct IoUringEnv {
    pub(crate) ring: IoUring,
    #[allow(dead_code)]
    pub(crate) queue_depth: u32,
    sqpoll_enabled: bool,
    /// Index of the registered file descriptor, or `None` if no file is registered.
    fixed_file_index: Option<u32>,
}

impl IoUringEnv {
    /// Create a new io_uring instance with the given queue depth.
    /// Returns an error if the kernel does not support io_uring (< 5.6)
    /// or if ring creation fails for any reason.
    pub fn new(queue_depth: u32) -> IOResult<Self> {
        let ring = IoUring::new(queue_depth).map_err(|e| {
            IOError::create(format!(
                "Failed to create io_uring with queue_depth={}: {}",
                queue_depth, e
            ))
        })?;
        Ok(Self { ring, queue_depth, sqpoll_enabled: false, fixed_file_index: None })
    }

    /// Create a new io_uring instance with SQPOLL enabled.
    ///
    /// SQPOLL mode runs a kernel thread that polls the submission queue,
    /// eliminating the `io_uring_enter()` syscall for most submissions.
    /// This reduces latency by ~22-29% for single-request patterns.
    ///
    /// - `idle_ms`: How long the kernel thread sleeps when idle before requiring
    ///   a wakeup. 100ms is recommended for production.
    /// - `cpu_core`: Optional CPU core to pin the kernel polling thread to.
    ///   When `None`, the kernel chooses automatically.
    ///
    /// # Requirements
    /// - Linux 5.13+ (for SQPOLL support)
    /// - Root or `CAP_SYS_NICE` capability
    ///
    /// # Errors
    /// Returns an error if SQPOLL is not available (wrong kernel version,
    /// missing capabilities, etc.). The caller should fall back to `new()`.
    pub fn new_with_sqpoll(queue_depth: u32, idle_ms: u32, cpu_core: Option<u32>) -> IOResult<Self> {
        let mut builder = IoUring::builder();
        builder.setup_sqpoll(idle_ms);
        if let Some(core) = cpu_core {
            builder.setup_sqpoll_cpu(core);
        }
        let ring = builder.build(queue_depth).map_err(|e| {
            IOError::create(format!(
                "Failed to create SQPOLL io_uring (need Linux 5.13+ and root/CAP_SYS_NICE, idle_ms={}): {}",
                idle_ms, e
            ))
        })?;
        Ok(Self { ring, queue_depth, sqpoll_enabled: true, fixed_file_index: None })
    }

    /// Returns `true` if this ring was created with SQPOLL enabled.
    pub fn sqpoll_enabled(&self) -> bool {
        self.sqpoll_enabled
    }

    /// Submit all pending SQEs and wait for `count` completions.
    pub fn submit_and_wait(&mut self, count: usize) -> IOResult<()> {
        self.ring.submit_and_wait(count).map_err(|e| {
            IOError::create(format!("io_uring submit_and_wait failed: {}", e))
        })?;
        Ok(())
    }

    /// Submit all pending SQEs without waiting for completions.
    pub fn submit(&mut self) -> IOResult<usize> {
        self.ring.submit().map_err(|e| {
            IOError::create(format!("io_uring submit failed: {}", e))
        })
    }

    /// Prepare a WRITE SQE. The caller must ensure `buf` remains valid
    /// until the corresponding CQE is consumed.
    ///
    /// # Safety
    /// The caller must ensure `buf` points to valid memory of at least `len` bytes
    /// and remains valid until the CQE is consumed.
    pub unsafe fn prep_write(
        &self,
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Entry {
        opcode::Write::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    }

    /// Prepare a READ SQE. The caller must ensure `buf` remains valid
    /// until the corresponding CQE is consumed.
    ///
    /// # Safety
    /// The caller must ensure `buf` points to valid mutable memory of at least `len` bytes
    /// and remains valid until the CQE is consumed.
    pub unsafe fn prep_read(
        &self,
        fd: RawFd,
        buf: *mut u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Entry {
        opcode::Read::new(Fd(fd), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    }

    /// Prepare an FSYNC SQE.
    pub fn prep_fsync(&self, fd: RawFd, user_data: u64) -> Entry {
        opcode::Fsync::new(Fd(fd)).build().user_data(user_data)
    }

    // --- Fixed file operations (Phase 3) ---

    /// Register a file descriptor at the given index for use with fixed-file operations.
    ///
    /// Once registered, use [`prep_write_fixed`](Self::prep_write_fixed),
    /// [`prep_read_fixed`](Self::prep_read_fixed), and
    /// [`prep_fsync_fixed`](Self::prep_fsync_fixed) with the same index.
    ///
    /// This avoids per-I/O kernel fd lookup overhead.
    pub fn register_file(&mut self, index: u32, fd: RawFd) -> IOResult<()> {
        let mut fds = vec![io_uring::register::SKIP_FILE; index as usize + 1];
        fds[index as usize] = fd;
        self.ring.submitter().register_files(&fds).map_err(|e| {
            IOError::create(format!("Failed to register fixed file at index {}: {}", index, e))
        })?;
        self.fixed_file_index = Some(index);
        Ok(())
    }

    /// Returns the fixed file index, if a file has been registered.
    pub fn fixed_file_index(&self) -> Option<u32> {
        self.fixed_file_index
    }

    /// Prepare a WRITE SQE using a pre-registered fixed file.
    ///
    /// # Safety
    /// The caller must ensure `buf` points to valid memory of at least `len` bytes
    /// and remains valid until the CQE is consumed.
    pub unsafe fn prep_write_fixed(
        &self,
        index: u32,
        buf: *const u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Entry {
        opcode::Write::new(Fixed(index), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    }

    /// Prepare a READ SQE using a pre-registered fixed file.
    ///
    /// # Safety
    /// The caller must ensure `buf` points to valid mutable memory of at least `len` bytes
    /// and remains valid until the CQE is consumed.
    pub unsafe fn prep_read_fixed(
        &self,
        index: u32,
        buf: *mut u8,
        len: u32,
        offset: u64,
        user_data: u64,
    ) -> Entry {
        opcode::Read::new(Fixed(index), buf, len)
            .offset(offset)
            .build()
            .user_data(user_data)
    }

    /// Prepare an FSYNC SQE using a pre-registered fixed file.
    pub fn prep_fsync_fixed(&self, index: u32, user_data: u64) -> Entry {
        opcode::Fsync::new(Fixed(index)).build().user_data(user_data)
    }

    /// Push an SQE into the submission queue.
    pub fn push(&mut self, sqe: &Entry) -> IOResult<()> {
        // SAFETY: The SQE is a Copy type. The safety of buffer pointers
        // within the SQE is the caller's responsibility.
        unsafe {
            self.ring.submission().push(sqe).map_err(|e| {
                IOError::create(format!("io_uring submission queue full: {}", e))
            })?;
        }
        Ok(())
    }

    /// Get the next CQE from the completion queue, if any.
    pub fn next_cqe(&mut self) -> Option<io_uring::cqueue::Entry> {
        self.ring.completion().next()
    }
}
