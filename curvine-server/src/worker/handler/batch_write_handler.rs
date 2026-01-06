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
use curvine_common::fs::RpcCode;
use curvine_common::proto::{
    BatchFilesWriteRequest, BatchFilesWriteResponse, BlockWriteRequest, BlockWriteResponse,
    BlocksBatchCommitRequest, BlocksBatchCommitResponse, BlocksBatchWriteRequest,
    BlocksBatchWriteResponse, DataHeaderProto, WriteBlocksBatchRequest, WriteBlocksBatchResponse,
    WriteCommitRequest, WriteCommitsBatchRequest, WriteCommitsBatchResponse,
};
use curvine_common::state::ExtendedBlock;
use curvine_common::utils::ProtoUtils;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::{ByteUnit, TimeSpent};
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::sys::{DataSlice, RawVec};
use orpc::{err_box, ternary, try_option_mut};
use rand::seq::index;
use std::{mem, result};

pub struct BatchWriteHandler {
    pub(crate) store: BlockStore,
    pub(crate) context: Option<Vec<WriteContext>>,
    pub(crate) file: Option<Vec<LocalFile>>,
    pub(crate) is_commit: bool,
    pub(crate) io_slow_us: u64,
    pub(crate) metrics: &'static WorkerMetrics,
}

impl BatchWriteHandler {
    pub fn new(store: BlockStore) -> Self {
        let conf = Worker::get_conf();
        let metrics = Worker::get_metrics();
        println!("DEBUG: at BatchWriteHandler::new()");
        Self {
            store,
            context: Some(Vec::new()),
            file: Some(Vec::new()),
            is_commit: false,
            io_slow_us: conf.worker.io_slow_us(),
            metrics,
        }
    }

    pub fn resize(file: &mut LocalFile, ctx: &WriteContext) -> FsResult<()> {
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
        if opts.len == 0 {
            return Ok(());
        }

        file.resize(opts.truncate, opts.off, opts.len, opts.mode.bits())?;
        Ok(())
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = WriteContext::from_req(msg)?;
        println!(
            "DEBUG, at BatchWriterHandler, open with context={:?}",
            context
        );
        // println!("DEBUG, at BatchWriterHandler, short_circuit: {:?}", context.short_circuit);
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
        println!("DEBUG, at BatchWriterHandler, open with meta={:?}", meta);
        let mut file = meta.create_writer(context.off, false)?;
        println!("DEBUG, at BatchWriterHandler, open with file={:?}", file);
        // check file resize
        Self::resize(&mut file, &context)?;
        println!("DEBUG, at BatchWriterHandler, open with file={:?}", file);

        let (label, path, file) = if context.short_circuit {
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
        println!("DEBUG, at BatchWriterHandler, log_msg: {:?}", log_msg);
        let response = BlockWriteResponse {
            id: meta.id,
            path: ternary!(context.short_circuit, Some(path), None),
            off: context.off,
            block_size: context.block_size,
            storage_type: meta.storage_type().into(),
        };
        println!(
            "DEBUG, at BatchWriterHandler, reponse before: {:?}",
            response
        );

        if let Some(files) = &mut self.file {
            if let Some(file) = file {
                files.push(file);
            }
        }
        println!(
            "DEBUG, at BatchWriterHandler, before update context: {:?}",
            self.context
        );
        if let Some(contexts) = &mut self.context {
            contexts.push(context);
        }
        println!(
            "DEBUG, at BatchWriterHandler, after update context: {:?}",
            self.context
        );

        self.metrics.write_blocks.with_label_values(&[label]).inc();

        info!("{}", log_msg);

        println!(
            "DEBUG, at BatchWriterHandler, reponse after: {:?}",
            response
        );
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

    pub fn write(&mut self, msg: &Message, index: usize) -> FsResult<Message> {
        println!(
            "DEBUG, at BatchWriterHandler, before write action, context: {:?}",
            self.context
        );
        println!("DEBUG: at BatchWRiteHandler::write");
        let file = &mut self.file.as_mut().unwrap()[index];
        let context = &mut self.context.as_mut().unwrap()[index];
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
                file.seek(header.offset)?;
            }
        }

        // Write existing data blocks.
        let data_len = msg.data_len() as i64;
        if data_len > 0 {
            if file.pos() + data_len > context.block_size {
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

    pub fn complete(&mut self, msg: &Message, commit: bool, index: usize) -> FsResult<Message> {
        println!(
            "DEBUG: BatchWriteHandler, complete, self.is_commit={:?}",
            self.is_commit
        );
        if self.is_commit {
            println!(
                "DEBUT, BatchWriteHandler, complete, msg.data: {:?}",
                msg.data
            );
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                println!("short circuit complete");
                Ok(msg.success())
            };
        }

        println!(
            "DEBUT, BatchWriteHandler, complete, before check context: {:?}",
            self.context
        );
        if let Some(context) = self.context.take() {
            Self::check_context(&context[index], msg)?;
        }
        let context = WriteContext::from_req(msg)?;

        println!(
            "DEBUG: at BatchWriteHandler of worker, context= {:?}",
            context
        );
        // flush and close the file.
        let file = self.file.take();
        if let Some(mut file) = file {
            file[index].flush()?;
            drop(file);
        }
        println!("DEBUG: context.block.len = {:?}", context.block.len);
        println!("DEBUG: context.block_size = {:?}", context.block_size);
        println!("DEBUG: context.block = {:?}", context.block);
        if context.block.len > context.block_size {
            return err_box!(
                "Invalid write offset: {}, block size: {}",
                context.off,
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

    pub fn open_batch(&mut self, msg: &Message) -> FsResult<Message> {
        println!(
            "DEBUG: open_batch - handler instance: {:p}, context before: {:?}",
            self, self.context
        );
        let header: BlocksBatchWriteRequest = msg.parse_header()?;
        println!("DEBUG at open_batch: {:?}", header);
        let mut responses = Vec::with_capacity(header.blocks.len());

        // reserve size of files and contexts
        // self.file.as_mut().unwrap().reserve(header.blocks.len());
        // self.context.as_mut().unwrap().reserve(header.blocks.len());
        self.file = Some(Vec::with_capacity(header.blocks.len()));
        self.context = Some(Vec::with_capacity(header.blocks.len()));

        println!(
            "DEBUG at BatchWriteHandler, at open_batch, self.file= {:?}",
            self.file
        );
        println!(
            "DEBUG at BatchWriteHandler, at open_batch, self.context= {:?}",
            self.context
        );

        for (i, block_proto) in header.blocks.into_iter().enumerate() {
            let unique_req_id = msg.req_id() + i as i64;

            // Create a single BlockWriteRequest from the block
            let header = BlockWriteRequest {
                block: block_proto,
                off: header.off,
                block_size: header.block_size,
                short_circuit: header.short_circuit,
                client_name: header.client_name.clone(),
                chunk_size: header.chunk_size,
            };

            // Create single request message for each block
            let single_msg = Builder::new()
                .code(msg.code())
                .request(RequestStatus::Open)
                .req_id(unique_req_id)
                .seq_id(msg.seq_id())
                .proto_header(header)
                .build();

            let response = self.open(&single_msg)?;
            println!("DEBUG, at BatchWriteHandler, response={:?}", response);
            let block_response: BlockWriteResponse = response.parse_header()?;
            responses.push(block_response);
        }

        println!("DEBUG: at open_batch, reponses = {:?}", responses);
        let batch_response = BlocksBatchWriteResponse { responses };

        println!(
            "DEBUG at BatchWriteHandler, at the end of open_batch, context= {:?}",
            self.context
        );
        println!(
            "DEBUG: open_batch - after adding contexts: {:?}",
            self.context
        );

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
        println!(
            "DEBUG: complete_batch - handler instance: {:p}, context at start: {:?}",
            self, self.context
        );
        println!("DEBUG at BatchWriteHandler, at complete_batch, at the starting step, self.context: {:?}", self.context);
        // Parse the flattened batch request
        let header: BlocksBatchCommitRequest = msg.parse_header()?;
        println!(
            "DEBUG at BatchWriteHandler, at complete_batch, header = {:?}",
            header
        );
        let mut results = Vec::new();

        if self.is_commit {
            return if !msg.data.is_empty() {
                err_box!("The block has been committed and data cannot be written anymore.")
            } else {
                Ok(msg.success())
            };
        }

        println!("DEBUG at BatchWriteHandler, at complete_batch, before resolve complete batch");
        // Take and check existing context (same as complete)
        println!(
            "DEBUG at BatchWriteHandler, at complete_batch, self.context: {:?}",
            self.context
        );

        // Process each block independently
        for (i, block_proto) in header.blocks.into_iter().enumerate() {
            if let Some(context) = self.context.take() {
                if context.len() > 1 {
                    Self::check_context(&context[i], msg)?;
                }
            }

            // Flush and close the file (same as complete)
            let file = self.file.take();
            if let Some(mut file) = file {
                if file.len() > 1 {
                    file[i].flush()?;
                    drop(file);
                }
            }

            // Create context manually for each block from block_proto
            let unique_req_id = msg.req_id() + i as i64;
            let context = WriteContext {
                block: ProtoUtils::extend_block_from_pb(block_proto),
                req_id: unique_req_id,
                chunk_size: header.block_size as i32,
                short_circuit: false,
                off: header.off,
                block_size: header.block_size,
            };

            println!(
                "DEBUG at BatchWriteHandler, at complete_batch, context[{}] = {:?}",
                i, context
            );

            // Validate block length (same as complete)
            if context.block.len > context.block_size {
                return err_box!(
                    "Invalid write offset: {}, block size: {}",
                    context.off,
                    context.block_size
                );
            }

            // Commit the block
            self.commit_block(&context.block, commit)?;
            results.push(true);
        }

        self.is_commit = true;
        println!(
            "DEBUG at BatchWriteHandler, at complete_batch, results = {:?}",
            results
        );
        let batch_response = BlocksBatchCommitResponse { results };

        Ok(Builder::success(msg).proto_header(batch_response).build())
    }

    pub fn write_files_batch_for_remote(&mut self, msg: &Message) -> FsResult<Message> {
        let header: BatchFilesWriteRequest = msg.parse_header()?;
        let mut results = Vec::new();

        for (i, file_data) in header.files.iter().enumerate() {
            // Convert bytes to DataSlice
            let data_slice = DataSlice::Bytes(bytes::Bytes::from(file_data.clone().content));

            // Create a temporary message for each file
            let single_msg = Builder::new()
                .code(RpcCode::WriteBlock)
                .request(RequestStatus::Running)
                .req_id(header.req_id + i as i64)
                .seq_id(header.seq_id)
                .data(data_slice)
                .build();

            // Reuse existing write method
            let response = self.write(&single_msg, i)?;
            results.push(response.is_success());
        }

        let batch_response = BatchFilesWriteResponse { results };
        println!(
            "DEBUG: BatchWriteHandler, at write_files_batch_for_remote, batch_response: {:?}",
            batch_response
        );
        Ok(Builder::success(msg).proto_header(batch_response).build())
    }
}

impl MessageHandler for BatchWriteHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            // batch operations
            RequestStatus::OpenBatch => self.open_batch(msg),
            RequestStatus::RunningBatch => self.write_files_batch_for_remote(msg),
            RequestStatus::CompleteBatch => self.complete_batch(msg, true),
            RequestStatus::CancelBatch => self.complete_batch(msg, false),

            _ => err_box!("Unsupported request type"),
        }
    }
}

// pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
//     // Parse batch files request
//     let header: BatchFilesWriteRequest = msg.parse_header()?;
//     let mut results = Vec::new();

//     println!("DEBUG: at WriteHanlder::write_batch: {:?}", header);
//     for file_data in header.files {
//         // Write each file to the block
//         let data = DataSlice::Bytes(bytes::Bytes::from(file_data.content));

//         // Use existing write logic
//         let write_result = self.write_data_direct(data);
//         results.push(write_result.is_ok());
//     }
//     let response = BatchFilesWriteResponse { results };
//     Ok(Builder::success(msg).proto_header(response).build())
// }

// // Helper method to write data directly without context
// fn write_data_direct(&mut self, data: DataSlice) -> FsResult<()> {
//     if let Some(ref mut data_file) = data {
//         println!("DEBUG at BatchWriteHandler with file: {:?}", data_file);
//         file.write_all(data_file.as_slice())?;
//         Ok(())
//     } else {
//         err_box!("No file open for writing")
//     }
// }

// Handle batch write requests for multiple files
// pub fn write_batch(&mut self, msg: &Message) -> FsResult<Message> {
//     let file = try_option_mut!(self.file);
//     let context = try_option_mut!(self.context);
//     Self::check_context(context, msg)?;

//     // Parse batch data header
//     if msg.header_len() > 0 {
//         let header: DataHeaderProto = msg.parse_header()?;
//         if !header.flush {
//             if header.offset < 0 || header.offset >= context.block_size {
//                 return err_box!(
//                     "Invalid seek offset: {}, block length: {}",
//                     header.offset,
//                     context.block_size
//                 );
//             }
//             file.seek(header.offset)?;
//         }
//     }

//     // Deserialize and write multiple files
//     let data = msg.data.clone();
//     let slice = data.as_slice();

//     let mut offset = 0;

//     // Read file count
//     if data.len() < 4 {
//         return err_box!("Invalid batch data: missing file count");
//     }
//     let file_count = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]) as usize;
//     offset += 4;

//     let mut total_written = 0;

//     for i in 0..file_count {
//         // Read path length
//         if offset + 4 > data.len() {
//             return err_box!("Invalid batch data: missing path length for file {}", i);
//         }
//         let path_len = u32::from_le_bytes([
//             slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]
//         ]) as usize;
//         offset += 4;

//         // Read path
//         if offset + path_len > data.len() {
//             return err_box!("Invalid batch data: missing path for file {}", i);
//         }
//         let path = std::str::from_utf8(&slice[offset..offset+path_len])
//         .map_err(|e| curvine_common::error::FsError::common(format!("Invalid path encoding: {}", e)))?;
//         offset += path_len;

//         // Read content length
//         if offset + 4 > data.len() {
//             return err_box!("Invalid batch data: missing content length for file {}", i);
//         }
//         let content_len = u32::from_le_bytes([
//             slice[offset], slice[offset+1], slice[offset+2], slice[offset+3]
//         ]) as usize;
//         offset += 4;

//         // Read content
//         if offset + content_len > data.len() {
//             return err_box!("Invalid batch data: missing content for file {}", i);
//         }
//         let content = &slice[offset..offset+content_len];
//         offset += content_len;

//         // Write content to file
//         if file.pos() + content.len() as i64 > context.block_size {
//             return err_box!(
//                 "Batch write exceeds block size: pos={}, len={}, block_size={}",
//                 file.pos(),
//                 content.len(),
//                 context.block_size
//             );
//         }

//         let spend = TimeSpent::new();
//         let raw_vec = RawVec::from_slice(content);
//         let content_slice = DataSlice::MemSlice(raw_vec);
//         file.write_region(&content_slice)?;
//         let used = spend.used_us();

//         if used >= self.io_slow_us {
//             warn!(
//                 "Slow batch write from disk cost: {}us (threshold={}us), path: {} ",
//                 used,
//                 self.io_slow_us,
//                 path
//             );
//         }

//         self.metrics.write_bytes.inc_by(content.len() as i64);
//         self.metrics.write_time_us.inc_by(used as i64);
//         self.metrics.write_count.inc();
//         total_written += content.len();

//         info!(
//             "Batch wrote file {}: {} bytes, path: {}",
//             i, content.len(), path
//         );
//     }

//     info!(
//         "Batch write completed: {} files, {} total bytes",
//         file_count, total_written
//     );

//     Ok(msg.success())
// }
// pub fn complete_batch(&mut self, msg: &Message, commit: bool) -> FsResult<Message> {
//     // Parse the flattened batch request
//     let header: BlocksBatchCommitRequest = msg.parse_header()?;
//     let mut results = Vec::new();

//     println!("DEBUG: at BatchWriteHandler,complete_batch, header {:?}", header);
//     // Iterate over blocks directly, not nested requests
//     for (i, block_proto) in header.blocks.into_iter().enumerate() {

//         let unique_req_id = msg.req_id() + i as i64;

//         println!("DEBUG, Loop complete block at i= {:?} with commit= {:?} and unique_req_id={:?}",i, commit, unique_req_id);

//         // Create single request message for each block
//         let single_msg = Builder::new()
//             .code(msg.code())
//             .request(if commit { RequestStatus::Complete } else { RequestStatus::Cancel })
//             .req_id(unique_req_id)
//             .seq_id(msg.seq_id())
//             .proto_header(WriteCommitRequest {
//                 block: block_proto,
//                 pos: header.off,
//                 block_size: header.block_size,
//                 cancel: !commit,
//             })
//             .build();

//         println!("DEBUG, at BatchWriteHandler, at complete batch, single_msg: {:?}", single_msg);
//         let _response = self.complete(&single_msg, commit)?;
//         println!("DEBUG, at BatchWriteHandler: at complete batch, _response: {:?}, ", _response);
//         results.push(true);
//     }

//     let batch_response = BlocksBatchCommitResponse {
//         results,
//     };

//     Ok(Builder::success(msg).proto_header(batch_response).build())
// }
