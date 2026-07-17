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
use io_uring::types::Fd;
use io_uring::{opcode, IoUring};
use std::os::unix::io::RawFd;

/// Wrapper around `io_uring::IoUring` that provides helper methods for
/// preparing and submitting common file I/O operations.
pub struct IoUringEnv {
    pub(crate) ring: IoUring,
    #[allow(dead_code)]
    pub(crate) queue_depth: u32,
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
        Ok(Self { ring, queue_depth })
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
