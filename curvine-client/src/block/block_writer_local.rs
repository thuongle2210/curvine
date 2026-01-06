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

use crate::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{DataSlice, RawPtr};
use orpc::{err_box, try_option};
use std::io::{BufWriter, Write};
use std::sync::Arc;

pub struct BlockWriterLocal {
    rt: Arc<Runtime>,
    fs_context: Arc<FsContext>,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    file: Option<RawPtr<LocalFile>>,
    buffered_file: Option<RawPtr<BufWriter<LocalFile>>>,
    block_size: i64,
    seq_id: i32,
    req_id: i64,
    is_buffered: bool,
}

impl BlockWriterLocal {
    pub async fn new(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_block(
                &block,
                pos,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await?;

        let path = try_option!(write_context.path);
        let file = LocalFile::with_write_offset(path, false, pos)?;

        let writer = Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            block,
            worker_address,
            file: Some(RawPtr::from_owned(file)),
            buffered_file: None,
            block_size,
            seq_id,
            req_id,
            is_buffered: false,
        };

        Ok(writer)
    }

    pub async fn new_buffered(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let block_size = fs_context.block_size();
        let client = fs_context.block_client(&worker_address).await?;
        let write_context = client
            .write_block(
                &block,
                pos,
                block_size,
                req_id,
                seq_id,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await?;

        let path = try_option!(write_context.path);
        let file: LocalFile = LocalFile::with_write_offset(path, false, pos)?;
        let buffered_file = BufWriter::new(file);

        let writer = Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            block,
            worker_address,
            file: None,
            buffered_file: Some(RawPtr::from_owned(buffered_file)),
            block_size,
            seq_id,
            req_id,
            is_buffered: true,
        };

        Ok(writer)
    }

    pub async fn sync_all(&mut self) -> FsResult<()> {
        if self.is_buffered {
            let buffered_file = self.buffered_file.clone();
            self.rt
                .spawn_blocking(move || {
                    buffered_file.unwrap().as_mut().flush()?;
                    Ok(())
                })
                .await?
        } else {
            self.flush().await
        }
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        // let file = self.file.clone();
        // self.rt
        //     .spawn_blocking(move || {
        //         file.unwrap().as_mut().write_all(chunk.as_slice())?;
        //         Ok::<(), FsError>(())
        //     })
        //     .await??;

        if self.is_buffered {
            let buffered_file = self.buffered_file.clone();
            self.rt
                .spawn_blocking(move || {
                    buffered_file
                        .unwrap()
                        .as_mut()
                        .write_all(chunk.as_slice())?;
                    Ok::<(), FsError>(())
                })
                .await??;
        } else {
            let file = self.file.clone();
            self.rt
                .spawn_blocking(move || {
                    file.unwrap().as_mut().write_all(chunk.as_slice())?;
                    Ok::<(), FsError>(())
                })
                .await??;
            if self.pos() > self.block.len {
                self.block.len = self.pos();
            }
        }

        Ok(())
    }

    // Block write data.
    pub fn blocking_write(&mut self, chunk: DataSlice) -> FsResult<()> {
        self.file.as_mut().unwrap().write_all(chunk.as_slice())?;
        if self.pos() > self.block.len {
            self.block.len = self.pos();
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> FsResult<()> {
        let file = self.file.clone();
        self.rt
            .spawn_blocking(move || {
                file.unwrap().as_mut().flush()?;
                Ok(())
            })
            .await?
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        // self.flush().await?;
        // let next_seq_id = self.next_seq_id();
        // let client = self.fs_context.block_client(&self.worker_address).await?;
        // client
        //     .write_commit(
        //         &self.block,
        //         self.pos(),
        //         self.block_size,
        //         self.req_id,
        //         next_seq_id,
        //         false,
        //     )
        //     .await
        if self.is_buffered {
            self.sync_all().await?;
        } else {
            self.flush().await?;
        }

        let next_seq_id = self.next_seq_id();
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit(
                &self.block,
                self.pos(),
                self.block_size,
                self.req_id,
                next_seq_id,
                false,
            )
            .await
    }

    pub async fn cancel(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit(
                &self.block,
                self.pos(),
                self.block_size,
                self.req_id,
                next_seq_id,
                true,
            )
            .await
    }

    pub fn remaining(&self) -> i64 {
        self.block_size - self.pos()
    }

    pub fn pos(&self) -> i64 {
        self.file.clone().unwrap().pos()
    }

    pub fn len(&self) -> i64 {
        self.block.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        } else if pos > self.block_size {
            return err_box!(
                "Seek position {} exceeds block capacity {}",
                pos,
                self.block_size
            );
        }

        let file = self.file.clone();
        self.rt
            .spawn_blocking(move || {
                file.unwrap().as_mut().seek(pos)?;
                Ok(())
            })
            .await?
    }
}
