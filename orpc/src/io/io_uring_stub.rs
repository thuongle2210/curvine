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
use crate::io::{IOError, IOResult};
use crate::sys::DataSlice;

/// Stub `IoUringBdev` used when the `io-uring` feature is disabled
/// or when the kernel does not support io_uring. All I/O methods
/// return an error indicating io_uring is unavailable.
pub struct IoUringBdev;

impl IoUringBdev {
    pub fn new(_path: impl AsRef<str>, _is_write: bool, _sqpoll_idle_ms: u32, _sqpoll_cpu: Option<u32>) -> IOResult<Self> {
        Err(IOError::create("io_uring not available (feature disabled or unsupported kernel)"))
    }

    pub fn with_write_offset(
        _path: impl AsRef<str>,
        _truncate: bool,
        _offset: i64,
        _sqpoll_idle_ms: u32,
        _sqpoll_cpu: Option<u32>,
    ) -> IOResult<Self> {
        Err(IOError::create("io_uring not available (feature disabled or unsupported kernel)"))
    }

    pub fn with_read(_path: impl AsRef<str>, _offset: u64, _sqpoll_idle_ms: u32, _sqpoll_cpu: Option<u32>) -> IOResult<Self> {
        Err(IOError::create("io_uring not available (feature disabled or unsupported kernel)"))
    }
}

impl BlockIO for IoUringBdev {
    fn write_all(&mut self, _buf: &[u8]) -> IOResult<()> {
        Err(IOError::create("io_uring not available"))
    }

    fn read_all(&mut self, _buf: &mut [u8]) -> IOResult<()> {
        Err(IOError::create("io_uring not available"))
    }

    fn flush(&mut self) -> IOResult<()> {
        Err(IOError::create("io_uring not available"))
    }

    fn seek(&mut self, _pos: i64) -> IOResult<i64> {
        Err(IOError::create("io_uring not available"))
    }

    fn write_region(&mut self, _region: &DataSlice) -> IOResult<()> {
        Err(IOError::create("io_uring not available"))
    }

    fn read_region(&mut self, _enable_send_file: bool, _len: i32) -> IOResult<DataSlice> {
        Err(IOError::create("io_uring not available"))
    }

    fn pos(&self) -> i64 {
        0
    }

    fn len(&self) -> i64 {
        0
    }

    fn path(&self) -> &str {
        ""
    }

    fn resize(&mut self, _truncate: bool, _off: i64, _len: i64, _mode: i32) -> IOResult<()> {
        Err(IOError::create("io_uring not available"))
    }
}
