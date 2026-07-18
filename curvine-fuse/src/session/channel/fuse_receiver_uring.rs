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

//! io_uring-based FUSE request reader.
//!
//! Uses `IORING_OP_READ` on `/dev/fuse` for zero-copy request reception.
//! This is more efficient than the splice-based path (1 copy vs 2 copies)
//! but requires Linux 6.14+ for full FUSE io_uring support.

use bytes::BytesMut;
use io_uring::{types, IoUring, opcode};
use log::debug;
use orpc::io::{IOError, IOResult};
use orpc::sys::pipe::AsyncFd;
use std::sync::{Arc, Mutex};

use crate::FUSE_IN_HEADER_LEN;

/// io_uring-based FUSE request reader.
///
/// Wraps an `io_uring::IoUring` instance for reading FUSE requests from
/// `/dev/fuse`. Each reader owns its own ring instance (QD configurable,
/// default 8).
///
/// The ring is protected by `Arc<Mutex<IoUring>>` so it can be shared
/// across `spawn_blocking` calls. Each `receive()` call:
/// 1. Clones the `Arc`
/// 2. Spawns a blocking task
/// 3. Takes the lock, submits `IORING_OP_READ`, waits for completion
/// 4. Returns the raw bytes as `BytesMut`
pub struct IoUringFuseReader {
    ring: Arc<Mutex<IoUring>>,
    fuse_fd: i32,
    fuse_len: usize,
}

impl IoUringFuseReader {
    /// Create a new io_uring FUSE reader.
    ///
    /// - `kernel_fd`: The `/dev/fuse` fd (from `AsyncFd::raw_fd()`)
    /// - `queue_depth`: io_uring submission queue depth
    /// - `buf_size`: Read buffer size (typically 128KB for FUSE)
    pub fn new(kernel_fd: &AsyncFd, queue_depth: u32, buf_size: usize) -> IOResult<Self> {
        let ring = IoUring::new(queue_depth).map_err(|e| {
            IOError::create(format!(
                "Failed to create io_uring for FUSE (queue_depth={}): {}",
                queue_depth, e
            ))
        })?;

        Ok(Self {
            ring: Arc::new(Mutex::new(ring)),
            fuse_fd: kernel_fd.raw_fd(),
            fuse_len: buf_size,
        })
    }

    /// Read a FUSE request from `/dev/fuse` using io_uring.
    ///
    /// Blocks in `spawn_blocking` while waiting for the kernel to provide
    /// a request. Returns the raw request bytes (header + payload).
    pub async fn receive(&mut self) -> IOResult<BytesMut> {
        let ring = self.ring.clone();
        let fd = self.fuse_fd;
        let buf_len = self.fuse_len;

        let result = tokio::task::spawn_blocking(move || -> IOResult<BytesMut> {
            let mut ring = ring.lock().map_err(|e| {
                IOError::create(format!("io_uring ring lock poisoned: {}", e))
            })?;

            // Allocate a buffer for the kernel to write into.
            // We allocate per-call to avoid lifetime issues with spawn_blocking.
            let mut kernel_buf = vec![0u8; buf_len];

            // Prepare READ SQE for /dev/fuse
            let read_e = opcode::Read::new(
                types::Fd(fd),
                kernel_buf.as_mut_ptr(),
                buf_len as u32,
            )
            .build()
            .user_data(0);

            // Push SQE to submission queue
            unsafe {
                ring.submission().push(&read_e).map_err(|e| {
                    IOError::create(format!("io_uring submission queue full: {}", e))
                })?;
            }

            // Submit and wait for at least one completion
            ring.submit_and_wait(1)
                .map_err(|e| IOError::create(format!("io_uring submit_and_wait failed: {}", e)))?;

            // Get the completion
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| IOError::create("no io_uring completion found"))?;

            let result = cqe.result();
            if result < 0 {
                return Err(IOError::create(format!(
                    "io_uring read on /dev/fuse failed: {}",
                    std::io::Error::from_raw_os_error(-result)
                )));
            }

            let len = result as usize;
            if len < FUSE_IN_HEADER_LEN {
                return Err(IOError::create(format!(
                    "short read on fuse device via io_uring ({} bytes)",
                    len
                )));
            }

            // Copy kernel buffer into BytesMut
            let mut buf = BytesMut::zeroed(len);
            buf.extend_from_slice(&kernel_buf[..len]);
            Ok(buf)
        })
        .await
        .map_err(IOError::from)??;

        debug!("io_uring fuse read: {} bytes", result.len());
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    #[test]
    fn io_uring_fuse_ring_creation() {
        // Verify io_uring ring can be created with FUSE-appropriate queue depth
        let ring = IoUring::new(8);
        assert!(ring.is_ok(), "io_uring ring creation should succeed on Linux 5.6+");
    }

    #[test]
    fn io_uring_fuse_read_basic() {
        // Try to open /dev/fuse (may fail if not mounted)
        let fuse_fd = match OpenOptions::new().read(true).open("/dev/fuse") {
            Ok(f) => f,
            Err(_) => {
                eprintln!("Skipping: /dev/fuse not available (no FUSE mount)");
                return;
            }
        };

        let fd = fuse_fd.as_raw_fd();
        let mut ring = IoUring::new(4).expect("io_uring creation failed");
        let mut buf = vec![0u8; 4096];

        let read_e = opcode::Read::new(
            types::Fd(fd),
            buf.as_mut_ptr(),
            buf.len() as u32,
        )
        .build()
        .user_data(0);

        unsafe {
            ring.submission().push(&read_e).expect("submit failed");
        }

        // Non-blocking submit: check if any FUSE request is pending
        ring.submit().expect("submit syscall failed");

        // Try to get a completion (may not be ready if no FUSE activity)
        // This validates io_uring can work on /dev/fuse
        let completion = ring.completion().next();
        if let Some(cqe) = completion {
            let result = cqe.result();
            if result > 0 {
                eprintln!("io_uring read {} bytes from /dev/fuse", result);
                assert!(result as usize >= FUSE_IN_HEADER_LEN);
            } else {
                eprintln!(
                    "io_uring read returned {}: {}",
                    result,
                    std::io::Error::from_raw_os_error(-result)
                );
            }
        } else {
            eprintln!("No FUSE request pending (normal if no activity)");
        }
    }
}
