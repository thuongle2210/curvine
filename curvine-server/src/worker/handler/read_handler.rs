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

use crate::worker::block::BlockStore;
use crate::worker::handler::ReadContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockReadResponse, DataHeaderProto};
use curvine_common::state::StorageType;
use curvine_common::FsResult;
use log::{debug, info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::io::{BlockDevice, BlockIO};
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::DataSlice;
use orpc::sys::{CacheManager, ReadAheadTask};
use orpc::{err_box, ternary, try_option_mut};
use std::mem;

pub struct ReadHandler {
    pub(crate) store: BlockStore,
    pub(crate) os_cache: CacheManager,
    pub(crate) context: Option<ReadContext>,
    pub(crate) file: Option<BlockDevice>,
    pub(crate) last_task: Option<ReadAheadTask>,
    pub(crate) io_slow_us: u64,
    pub(crate) enable_send_file: bool,
    pub(crate) async_enabled: bool,
    pub(crate) metrics: &'static WorkerMetrics,
    /// Prefetched data for next sequential read chunk (pos, data).
    prefetch_buf: Option<(i64, DataSlice)>,
    /// Background prefetch task handle.
    prefetch_task: Option<tokio::task::JoinHandle<(i64, orpc::io::IOResult<DataSlice>)>>,
}

impl ReadHandler {
    pub const MAX_READ_AHEAD: i64 = 16 * 1024 * 1024;

    pub fn new(store: BlockStore) -> Self {
        let metrics = Worker::get_metrics();
        let conf = Worker::get_conf();
        Self {
            store,
            os_cache: CacheManager::with_place(),
            context: None,
            file: None,
            last_task: None,
            io_slow_us: conf.worker.io_slow_us(),
            enable_send_file: conf.worker.enable_send_file,
            async_enabled: conf.worker.async_enabled,
            metrics,
            prefetch_buf: None,
            prefetch_task: None,
        }
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let mut context = ReadContext::from_req(msg)?;
        let meta = self.store.get_block(context.block_id)?;

        if context.off > meta.len {
            return err_box!(
                "The length of the requested data exceeds the maximum length of the block file, \
            request off {}, file len {}",
                context.off,
                meta.len
            );
        }

        if context.chunk_size <= 0 {
            return err_box!("chunk_size must be greater than 0");
        }

        if context.enable_read_ahead && context.read_ahead_len > Self::MAX_READ_AHEAD {
            return err_box!(
                "The pre-read size exceeds the maximum value allowed by the system.\
                 The current value is {}. The maximum allowed value is: {}",
                context.read_ahead_len,
                Self::MAX_READ_AHEAD
            );
        }

        // Check short-circuit before open. SPDK has no filesystem path.
        let is_short_circuit =
            context.short_circuit && meta.storage_type() != StorageType::SpdkDisk;
        let (label, path, file) = if is_short_circuit {
            let path = meta.get_block_file()?;
            ("local", path, None)
        } else {
            let file = meta.create_reader(context.off as u64)?;
            ("remote", file.path().to_string(), Some(file))
        };

        self.os_cache = CacheManager::new(
            context.enable_read_ahead,
            context.read_ahead_len,
            context.drop_cache_len,
            context.chunk_size as i64,
        );

        let log_msg = format!(
            "Read {}-block start req_id: {}, path: {:?}, chunk_size: {}, read len: {}, read_ahead: {}-{}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            ByteUnit::byte_to_string(context.len as u64),
            self.os_cache.enable,
            self.os_cache.read_ahead_len
        );

        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: ternary!(is_short_circuit, Some(path), None),
            storage_type: meta.storage_type().into(),
        };

        let _ = mem::replace(&mut self.file, file);
        context.bdev_offset = meta.bdev_offset;
        let _ = self.context.replace(context);

        self.metrics.read_blocks.with_label_values(&[label]).inc();
        info!("{}", log_msg);

        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn check_context(context: &ReadContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    pub fn read(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);

        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            let abs_offset = if file.supports_short_circuit() {
                header.offset
            } else {
                context.bdev_offset + header.offset
            };
            if abs_offset != file.pos() {
                file.seek(abs_offset)?;
            }
        }

        let spend = TimeSpent::new();
        // OS page cache read-ahead is only available for local files
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        }
        // SPDK bypasses kernel — sendfile unavailable
        let enable_send_file = self.enable_send_file && file.supports_send_file();
        let region = file.read_region(enable_send_file, context.chunk_size)?;
        // Post-read read-ahead for next chunk
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        }
        let used = spend.used_us();

        if used >= self.io_slow_us {
            warn!(
                "Slow read data from disk cost: {}us (threshold={}us), path: {} ",
                used,
                self.io_slow_us,
                file.path()
            );
        }
        self.metrics.read_bytes.inc_by(region.len() as i64);
        self.metrics.read_time_us.inc_by(used as i64);
        self.metrics.read_count.inc();

        Ok(msg.success_with_data(None, region))
    }

    pub async fn async_read(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);

        let needs_seek = if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            let abs_offset = if file.supports_short_circuit() {
                header.offset
            } else {
                context.bdev_offset + context.off + header.offset
            };
            Some(abs_offset)
        } else {
            None
        };

        // Check prefetch cache: if the requested offset matches cached data, skip I/O
        if let Some((prefetch_pos, prefetched)) = self.prefetch_buf.take() {
            let current_pos = match needs_seek {
                Some(off) => off,
                None => file.pos(),
            };
            if current_pos == prefetch_pos && !prefetched.is_empty() {
                // Prefetch hit — use cached data, no I/O needed
                let used = 0u64;
                let region = prefetched;

                // Advance file position past prefetched data
                file.seek(current_pos + region.len() as i64)?;

                self.metrics.read_bytes.inc_by(region.len() as i64);
                self.metrics.read_time_us.inc_by(used as i64);
                self.metrics.read_count.inc();
                return Ok(msg.success_with_data(None, region));
            }
        }

        // Handle seek
        if let Some(abs_offset) = needs_seek {
            if abs_offset != file.pos() {
                file.seek(abs_offset)?;
            }
        }

        let spend = TimeSpent::new();
        // OS page cache read-ahead is only available for local files
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        }
        // SPDK bypasses kernel — sendfile unavailable
        let enable_send_file = self.enable_send_file && file.supports_send_file();
        let region = file
            .async_read_region(enable_send_file, context.chunk_size)
            .await?;
        // Post-read read-ahead for next chunk
        if file.supports_read_ahead() {
            if let Some(local) = file.as_local_mut() {
                self.last_task = local.read_ahead(&self.os_cache, self.last_task.take());
            }
        } else if file.supports_async() {
            // Prefetch next chunk for SPDK async reads (overlaps I/O with network send)
            let next_pos = file.pos();
            let remaining = file.len() - next_pos;
            if remaining > 0 {
                let chunk_size = context.chunk_size.min(remaining as i32);
                match file.async_read_region(enable_send_file, chunk_size).await {
                    Ok(prefetch_data) if !prefetch_data.is_empty() => {
                        self.prefetch_buf = Some((next_pos, prefetch_data));
                    }
                    _ => { /* prefetch miss is benign */ }
                }
            }
        }
        let used = spend.used_us();

        if used >= self.io_slow_us {
            warn!(
                "Slow read data from disk cost: {}us (threshold={}us), path: {} ",
                used,
                self.io_slow_us,
                file.path()
            );
        }
        self.metrics.read_bytes.inc_by(region.len() as i64);
        self.metrics.read_time_us.inc_by(used as i64);
        self.metrics.read_count.inc();

        Ok(msg.success_with_data(None, region))
    }

    // Reading is completed and the file is closed.
    pub fn complete(&mut self, msg: &Message) -> FsResult<Message> {
        let _block_id = match &self.context {
            Some(v) => {
                //Remote reading
                Self::check_context(v, msg)?;
                v.block_id
            }

            None => {
                // Local short circuit reading, block information is in the header.
                // let c = ReadContext::from_req(msg)?;
                // c.block_id
                -1
            }
        };

        let file = self.file.take();
        drop(file);
        self.prefetch_buf = None;
        self.prefetch_task = None;

        info!("Read block end for req_id {}", msg.req_id());
        Ok(msg.success())
    }
}

impl MessageHandler for ReadHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        let sync = if !self.async_enabled {
            true
        } else if msg.request_status() != RequestStatus::Running {
            true
        } else {
            self.file.as_ref().is_none_or(|f| !f.supports_async())
        };
        info!(
            "is_sync: async_enabled={}, status={:?}, file_supports_async={:?}, result={}",
            self.async_enabled,
            msg.request_status(),
            self.file.as_ref().map(|f| f.supports_async()),
            sync
        );
        sync
    }

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.read(msg),

            RequestStatus::Complete => self.complete(msg),

            _ => err_box!("Unsupported request type"),
        }
    }

    async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        let result = match request_status {
            RequestStatus::Running => self.async_read(&msg).await,
            _ => self.handle(&msg),
        };
        match result {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}
