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

use crate::io::block_io::BlockIO;
use crate::io::io_uring_env::IoUringEnv;
use crate::io::{IOError, IOResult};
use crate::sys::DataSlice;
use bytes::BytesMut;
use std::fmt::{Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};

/// io_uring-backed block device. Implements [`BlockIO`] using async I/O
/// via the Linux io_uring interface (kernel 5.6+).
///
/// Each `IoUringBdev` owns its own `IoUring` ring instance. This avoids
/// lock contention and matches the pattern used in Curvine where each
/// block writer/reader operates on a single file.
///
/// # Safety
///
/// The `Send` impl is safe because:
/// - Each `IoUringBdev` is used from a single thread at a time
///   (via `spawn_blocking` or direct calls).
/// - The io_uring shared memory region is kernel-managed and does not
///   require Rust-level synchronization.
pub struct IoUringBdev {
    env: IoUringEnv,
    file: fs::File,
    #[allow(dead_code)]
    fd: RawFd,
    path: String,
    pos: i64,
    len: i64,
}

// SAFETY: See struct doc comment above.
unsafe impl Send for IoUringBdev {}

impl IoUringBdev {
    /// Open a file for io_uring I/O.
    ///
    /// - `sqpoll_idle_ms`: SQPOLL idle timeout in ms. 0 = standard io_uring.
    /// - `sqpoll_cpu`: Optional CPU core to pin the SQPOLL kernel thread to.
    pub fn new(path: impl AsRef<str>, is_write: bool, sqpoll_idle_ms: u32, sqpoll_cpu: Option<u32>) -> IOResult<Self> {
        let path_str = path.as_ref().to_string();
        let file = if is_write {
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path_str)
        } else {
            OpenOptions::new().read(true).open(&path_str)
        }
        .map_err(|e| IOError::with_msg(e, &format!("Failed to open {}", path_str)))?;

        let fd = file.as_raw_fd();
        let len = file.metadata().map(|m| m.len() as i64).unwrap_or(0);
        let queue_depth = 8;
        let mut env = if sqpoll_idle_ms > 0 {
            IoUringEnv::new_with_sqpoll(queue_depth, sqpoll_idle_ms, sqpoll_cpu)?
        } else {
            IoUringEnv::new(queue_depth)?
        };

        // Phase 3: Register fd at index 0 for fixed-file I/O.
        // This avoids per-I/O kernel fd lookup overhead.
        env.register_file(0, fd)?;

        Ok(Self {
            env,
            file,
            fd,
            path: path_str,
            pos: 0,
            len,
        })
    }

    /// Open a file for writing with an initial offset.
    pub fn with_write_offset(
        path: impl AsRef<str>,
        _truncate: bool,
        offset: i64,
        sqpoll_idle_ms: u32,
        sqpoll_cpu: Option<u32>,
    ) -> IOResult<Self> {
        let mut bdev = Self::new(path, true, sqpoll_idle_ms, sqpoll_cpu)?;
        if offset > 0 {
            bdev.seek(offset)?;
        }
        Ok(bdev)
    }

    /// Open a file for reading at the given offset.
    pub fn with_read(path: impl AsRef<str>, offset: u64, sqpoll_idle_ms: u32, sqpoll_cpu: Option<u32>) -> IOResult<Self> {
        let mut bdev = Self::new(path, false, sqpoll_idle_ms, sqpoll_cpu)?;
        if offset > 0 {
            bdev.seek(offset as i64)?;
        }
        Ok(bdev)
    }

    /// Synchronize the file length from metadata.
    fn update_len(&mut self) {
        if let Ok(meta) = self.file.metadata() {
            self.len = meta.len() as i64;
        }
    }
}

impl BlockIO for IoUringBdev {
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        // SAFETY: buf is valid for the duration of this call and the submit_and_wait
        // ensures the CQE is consumed before the buffer goes out of scope.
        let sqe = unsafe {
            self.env
                .prep_write_fixed(0, buf.as_ptr(), buf.len() as u32, self.pos as u64, 0)
        };
        self.env.push(&sqe)?;
        self.env.submit_and_wait(1)?;

        let cqe = self
            .env
            .next_cqe()
            .ok_or_else(|| IOError::create("No CQE for write"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(IOError::create(format!(
                "io_uring write failed at pos={}: {}",
                self.pos,
                io::Error::from_raw_os_error(-result)
            )));
        }
        self.pos += buf.len() as i64;
        self.update_len();
        Ok(())
    }

    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        // SAFETY: buf is valid mutable memory for the duration of this call
        // and submit_and_wait ensures the CQE is consumed before reuse.
        let sqe = unsafe {
            self.env
                .prep_read_fixed(0, buf.as_mut_ptr(), buf.len() as u32, self.pos as u64, 0)
        };
        self.env.push(&sqe)?;
        self.env.submit_and_wait(1)?;

        let cqe = self
            .env
            .next_cqe()
            .ok_or_else(|| IOError::create("No CQE for read"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(IOError::create(format!(
                "io_uring read failed at pos={}: {}",
                self.pos,
                io::Error::from_raw_os_error(-result)
            )));
        }
        let read_len = result as usize;
        if read_len < buf.len() {
            return Err(IOError::create(format!(
                "Short read at pos={}: expected {} bytes, got {}",
                self.pos,
                buf.len(),
                read_len
            )));
        }
        self.pos += buf.len() as i64;
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        let sqe = self.env.prep_fsync_fixed(0, 0);
        self.env.push(&sqe)?;
        self.env.submit_and_wait(1)?;

        let cqe = self
            .env
            .next_cqe()
            .ok_or_else(|| IOError::create("No CQE for fsync"))?;
        let result = cqe.result();
        if result < 0 {
            return Err(IOError::create(format!(
                "io_uring fsync failed: {}",
                io::Error::from_raw_os_error(-result)
            )));
        }
        Ok(())
    }

    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        if pos == self.pos {
            return Ok(pos);
        }
        // io_uring uses offset-based I/O, so seek is pure position tracking.
        // We allow seeking past end for writes (matching LocalFile behavior).
        self.pos = pos;
        Ok(self.pos)
    }

    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        match region {
            DataSlice::Empty => Ok(()),
            DataSlice::Buffer(bytes) => self.write_all(bytes),
            DataSlice::Bytes(bytes) => self.write_all(bytes),
            DataSlice::MemSlice(bytes) => self.write_all(bytes.as_slice()),
            DataSlice::IOSlice(_) => {
                Err(IOError::create("IOSlice not supported by io_uring backend"))
            }
        }
    }

    fn read_region(&mut self, _enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let chunk = (len as i64).min(self.len - self.pos);
        if chunk <= 0 {
            return Err(IOError::create(format!(
                "No data to read: file_len={}, pos={}",
                self.len, self.pos
            )));
        }
        let mut buf = BytesMut::with_capacity(chunk as usize);
        unsafe {
            buf.set_len(chunk as usize);
        }
        self.read_all(&mut buf)?;
        Ok(DataSlice::Buffer(buf))
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn resize(&mut self, truncate: bool, _off: i64, len: i64, _mode: i32) -> IOResult<()> {
        // Phase 2: implement via IORING_OP_FTRUNCATE
        if truncate {
            self.file.set_len(len as u64).map_err(|e| {
                IOError::with_msg(e, &format!("Failed to truncate {}", self.path))
            })?;
            self.len = len;
        }
        Ok(())
    }
}

impl Display for IoUringBdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IoUringBdev({})", self.path)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::block_io::BlockIO;

    #[test]
    fn io_uring_bdev_write_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test_block_uring.bin");
        let path_str = path.to_str().unwrap();
        let data = b"Hello from io_uring bdev! This is a test of the BlockIO interface.";
        let mut read_buf = vec![0u8; data.len()];

        // Write
        let mut writer = IoUringBdev::with_write_offset(path_str, true, 0, 0, None)?;
        assert_eq!(writer.pos(), 0);
        writer.write_all(data)?;
        writer.flush()?;
        assert_eq!(writer.pos(), data.len() as i64);
        drop(writer);

        // Read
        let mut reader = IoUringBdev::with_read(path_str, 0, 0, None)?;
        assert_eq!(reader.len(), data.len() as i64);
        reader.read_all(&mut read_buf)?;
        assert_eq!(&read_buf, data);
        assert_eq!(reader.pos(), data.len() as i64);

        Ok(())
    }

    #[test]
    fn io_uring_bdev_seek() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test_seek.bin");
        let path_str = path.to_str().unwrap();
        let data = b"0123456789ABCDEF";

        let mut writer = IoUringBdev::with_write_offset(path_str, true, 0, 0, None)?;
        writer.write_all(data)?;
        writer.flush()?;

        // Seek to middle and write
        writer.seek(8)?;
        assert_eq!(writer.pos(), 8);
        let new_data = b"XXXX";
        writer.write_all(new_data)?;
        writer.flush()?;

        // Read back and verify
        let mut reader = IoUringBdev::with_read(path_str, 0, 0, None)?;
        let mut full = vec![0u8; data.len()];
        reader.read_all(&mut full)?;
        assert_eq!(&full[0..8], b"01234567");
        assert_eq!(&full[8..12], b"XXXX");
        assert_eq!(&full[12..], b"CDEF");

        Ok(())
    }

    #[test]
    fn io_uring_bdev_write_region() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test_region.bin");
        let path_str = path.to_str().unwrap();

        let mut writer = IoUringBdev::with_write_offset(path_str, true, 0, 0, None)?;
        let data = b"region data";
        let region = DataSlice::Buffer(BytesMut::from(data.as_slice()));
        writer.write_region(&region)?;
        writer.flush()?;

        let mut reader = IoUringBdev::with_read(path_str, 0, 0, None)?;
        let region_out = reader.read_region(false, data.len() as i32)?;
        assert_eq!(region_out.len(), data.len());
        assert_eq!(&region_out.as_slice()[..], data);

        Ok(())
    }

    #[test]
    fn io_uring_sqpoll_write_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test_sqpoll.bin");
        let path_str = path.to_str().unwrap();
        let data = b"SQPOLL mode write+read roundtrip test";
        let mut read_buf = vec![0u8; data.len()];

        // Write with SQPOLL (100ms idle, auto CPU)
        let mut writer = IoUringBdev::with_write_offset(path_str, true, 0, 100, None)?;
        assert!(writer.env.sqpoll_enabled());
        writer.write_all(data)?;
        writer.flush()?;
        drop(writer);

        // Read with SQPOLL
        let mut reader = IoUringBdev::with_read(path_str, 0, 100, None)?;
        assert!(reader.env.sqpoll_enabled());
        reader.read_all(&mut read_buf)?;
        assert_eq!(&read_buf, data);

        Ok(())
    }

    #[test]
    fn io_uring_fixed_file_write_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("test_fixed.bin");
        let path_str = path.to_str().unwrap();
        let data = b"Fixed file registration test data";
        let mut read_buf = vec![0u8; data.len()];

        // Write using fixed file (fd registered at index 0)
        let mut writer = IoUringBdev::with_write_offset(path_str, true, 0, 0, None)?;
        assert!(writer.env.fixed_file_index().is_some());
        assert_eq!(writer.env.fixed_file_index(), Some(0));
        writer.write_all(data)?;
        writer.flush()?;
        drop(writer);

        // Read using fixed file
        let mut reader = IoUringBdev::with_read(path_str, 0, 0, None)?;
        assert!(reader.env.fixed_file_index().is_some());
        reader.read_all(&mut read_buf)?;
        assert_eq!(&read_buf, data);

        Ok(())
    }
}
