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
use crate::worker::handler::WriteContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::{BlockWriteResponse, DataHeaderProto};
use curvine_common::state::{ExtendedBlock, FileAllocMode};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::io::{BlockDevice, BlockIO};
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, ternary, try_option_mut};
use std::mem;

use bytes::Bytes as TBytes;

pub struct WriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<WriteContext>,
    pub(crate) file: Option<BlockDevice>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
    pub(crate) async_enabled: bool,
    /// Deferred write: one chunk buffered ahead. Data from RPC N is written
    /// during processing of RPC N+1, overlapping I/O with network RTT.
    pending_write: Option<(i64, Vec<u8>)>,
}

impl WriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let conf = Worker::get_conf();
        let metrics = Worker::get_metrics();
        Self {
            store,
            context: None,
            file: None,
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
            async_enabled: conf.worker.async_enabled,
            pending_write: None,
        }
    }

    pub fn resize(file: &mut BlockDevice, ctx: &WriteContext) -> FsResult<()> {
        let opts = if let Some(opts) = &ctx.block.alloc_opts {
            opts
        } else {
            return Ok(());
        };
        opts.validate()?;
        if opts.len > ctx.block_size {
            return err_box!(
                "Invalid resize operation: allocation size {} > block size {}",
                opts.len,
                ctx.block_size
            );
        }
        // SPDK: NVMe namespace has fixed capacity, just validate fits.
        if !file.supports_short_circuit() {
            return Ok(());
        }

        // Remove KEEP_SIZE flag to fix length mismatch at finalization.
        let mut mode = opts.mode;
        mode.remove(FileAllocMode::KEEP_SIZE);
        file.resize(opts.truncate, opts.off, opts.len, mode.bits())?;

        if opts.len != file.len() {
            return err_box!(
                "invalid resize file {} operation: resize {} != actual {}, opts={:?}",
                file.path(),
                opts.len,
                file.len(),
                opts
            );
        }

        Ok(())
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = WriteContext::from_req(msg)?;
        if context.off > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
                context.block_size
            );
        }

        let open_block = ExtendedBlock {
            len: context.block_size,
            ..context.block.clone()
        };

        let meta = self.store.open_block(&open_block)?;
        let mut file = meta.create_writer(context.off, false)?;
        // check file resize
        Self::resize(&mut file, &context)?;

        let is_short_circuit = context.short_circuit && file.supports_short_circuit();
        let (label, path, file) = if is_short_circuit {
            ("local", file.path().to_string(), None)
        } else {
            ("remote", file.path().to_string(), Some(file))
        };

        let log_msg = format!(
            "Write {}-block start req_id: {}, path: {:?}, chunk_size: {}, off: {}, block_size: {}",
            label,
            context.req_id,
            path,
            context.chunk_size,
            context.off,
            ByteUnit::byte_to_string(context.block_size as u64)
        );

        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(is_short_circuit, Some(path), None),
            off: context.off,
            block_size: context.block_size,
            storage_type: meta.storage_type().into(),
            pipeline_status: None,
        };

        let _ = mem::replace(&mut self.file, file);
        let _ = self.context.replace(context);

        self.metrics.write_blocks.with_label_values(&[label]).inc();

        info!("{}", log_msg);
        Ok(Builder::success(msg).proto_header(response).build())
    }

    fn check_context(context: &WriteContext, msg: &Message) -> FsResult<()> {
        if context.req_id != msg.req_id() {
            return err_box!(
                "Request id mismatch, expected {}, actual {}",
                context.req_id,
                msg.req_id()
            );
        }
        Ok(())
    }

    pub fn write(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        // msg.header
        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            // Flush operation should not trigger seek or boundary check
            if !header.flush {
                if header.offset < 0 || header.offset >= context.block_size {
                    return err_box!(
                        "Invalid seek offset: {}, block length: {}",
                        header.offset,
                        context.block_size
                    );
                }
                // SPDK: file.pos()/seek() = absolute bdev offset
                let abs_offset = if file.supports_short_circuit() {
                    header.offset
                } else {
                    (file.len() - context.block_size) + header.offset
                };
                file.seek(abs_offset)?;
            }
        }

        // Write existing data blocks.
        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            // SPDK: pos() = absolute bdev offset
            let write_limit = if file.supports_short_circuit() {
                // Local files: pos() is relative to file start
                context.block_size
            } else {
                // SPDK bdevs: pos() is absolute, len() is the upper bound
                file.len()
            };
            if file.pos() + data_len > write_limit {
                return err_box!(
                    "Write range [{}, {}) exceeds block size {}",
                    file.pos(),
                    file.pos() + data_len,
                    context.block_size
                );
            }

            let spend = TimeSpent::new();
            file.write_region(&msg.data)?;

            let used = spend.used_us();
            if used >= self.io_slow_us {
                warn!(
                    "Slow write data from disk cost: {}us (threshold={}us), path: {} ",
                    used,
                    self.io_slow_us,
                    file.path()
                );
            }
            self.metrics.write_bytes.inc_by(msg.data_len() as i64);
            self.metrics.write_time_us.inc_by(used as i64);
            self.metrics.write_count.inc();
        }

        Ok(msg.success())
    }

    fn commit_block(&self, block: &ExtendedBlock, commit: bool) -> FsResult<()> {
        if commit {
            self.store.finalize_block(block)?;
        } else {
            self.store.abort_block(block)?;
        }
        Ok(())
    }

    pub fn complete(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        if let Some(context) = self.context.take() {
            Self::check_context(&context, msg)?;
        }
        let context = WriteContext::from_req(msg)?;

        // flush and close the file.
        let file = self.file.take();
        if let Some(mut file) = file {
            file.flush()?;
            drop(file);
        }

        if context.block.len > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.block.len,
                context.block_size
            );
        }

        // Submit block.
        self.commit_block(&context.block, commit)?;
        self.is_commit = true;

        info!(
            "write block end for req_id {}, is commit: {}, off: {}, len: {}",
            msg.req_id(),
            commit,
            context.off,
            context.block.len
        );

        Ok(msg.success())
    }
}

impl WriteHandler {
    /// Async version of write() — defers the actual write by one RPC,
    /// overlapping I/O with the next request's network round-trip.
    async fn async_write(&mut self, msg: &Message) -> FsResult<Message> {
        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);
        Self::check_context(context, msg)?;

        // Parse current request offset
        let mut current_offset = None;
        if msg.header_len() > 0 {
            let header: DataHeaderProto = msg.parse_header()?;
            if !header.flush {
                if header.offset < 0 || header.offset >= context.block_size {
                    return err_box!(
                        "Invalid seek offset: {}, block length: {}",
                        header.offset,
                        context.block_size
                    );
                }
                let abs_offset = if file.supports_short_circuit() {
                    header.offset
                } else {
                    (file.len() - context.block_size) + header.offset
                };
                current_offset = Some(abs_offset);
            }
        }

        // Extract data bytes from current message
        let current_data = if msg.data_len() > 0 {
            let data_bytes = match &msg.data {
                orpc::sys::DataSlice::Empty => Vec::new(),
                orpc::sys::DataSlice::Buffer(b) => b.to_vec(),
                orpc::sys::DataSlice::MemSlice(v) => v.as_slice().to_vec(),
                orpc::sys::DataSlice::Bytes(b) => b.to_vec(),
                orpc::sys::DataSlice::IOSlice(..) => {
                    return err_box!("IOSlice not supported in async write");
                }
            };
            data_bytes
        } else {
            Vec::new()
        };

        let data_len = current_data.len() as i64;
        if data_len > 0 {
            let write_limit = if file.supports_short_circuit() {
                context.block_size
            } else {
                file.len()
            };

            // Validate bounds before starting write
            if let Some(off) = current_offset {
                if off + data_len > write_limit {
                    return err_box!(
                        "Write range [{}, {}) exceeds block size {}",
                        off,
                        off + data_len,
                        context.block_size
                    );
                }
            }
        }

        // Write the PREVIOUSLY pending data (from the RPC before this one)
        if let Some((prev_offset, prev_data)) = self.pending_write.take() {
            if !prev_data.is_empty() {
                let spend = TimeSpent::new();
                file.seek(prev_offset)?;
                let slice = orpc::sys::DataSlice::bytes(bytes::Bytes::from(prev_data));
                file.async_write_region(&slice).await?;

                let used = spend.used_us();
                if used >= self.io_slow_us {
                    warn!(
                        "Slow write data from disk cost: {}us (threshold={}us), path: {} ",
                        used,
                        self.io_slow_us,
                        file.path()
                    );
                }
                self.metrics.write_bytes.inc_by(data_len);
                self.metrics.write_time_us.inc_by(used as i64);
                self.metrics.write_count.inc();
            }
        }

        // Buffer CURRENT data for the NEXT RPC's deferred write
        if data_len > 0 {
            self.pending_write = Some((current_offset.unwrap_or(0), current_data));
        }

        // Return ack immediately — data will be written on next call
        Ok(msg.success())
    }

    /// Flush the deferred pending write (data from the previous async_write RPC).
    async fn flush_pending_async(&mut self) -> FsResult<()> {
        if let Some((offset, data)) = self.pending_write.take() {
            if !data.is_empty() {
                if let Some(file) = self.file.as_mut() {
                    file.seek(offset)?;
                    let slice = orpc::sys::DataSlice::bytes(TBytes::from(data));
                    file.async_write_region(&slice).await?;
                }
            }
        }
        Ok(())
    }
}

impl MessageHandler for WriteHandler {
    type Error = FsError;

    fn is_sync(&self, msg: &Message) -> bool {
        if !self.async_enabled {
            return true;
        }
        if msg.request_status() != RequestStatus::Running {
            return true;
        }
        // Only use async path when the backing device supports it (e.g. SPDK).
        // LocalFile falls through to blocking I/O and must stay on the blocking pool.
        self.file.as_ref().is_none_or(|f| !f.supports_async())
    }

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),

            RequestStatus::Running => self.write(msg),

            RequestStatus::Complete => self.complete(msg, true),

            RequestStatus::Cancel => self.complete(msg, false),

            _ => err_box!("Unsupported request type"),
        }
    }

    async fn async_handle(&mut self, msg: Message) -> FsResult<Message> {
        let request_status = msg.request_status();
        let result = match request_status {
            RequestStatus::Running => self.async_write(&msg).await,
            RequestStatus::Complete | RequestStatus::Cancel => {
                // Flush deferred write before closing file
                self.flush_pending_async().await?;
                self.handle(&msg)
            }
            _ => self.handle(&msg),
        };
        match result {
            Ok(v) => Ok(v),
            Err(e) => Ok(msg.error_ext(&e)),
        }
    }
}
