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


use crate::block::block_writer::BatchWriterAdapter::{BatchLocal, BatchRemote};
use crate::block::block_writer::WriterAdapter::{Local, Remote};


use crate::block::{
    BatchBlockWriterLocal, BatchBlockWriterRemote, BlockWriterLocal, BlockWriterRemote,
};
use crate::file::FsContext;
use bytes::buf::Writer;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{
    BlockLocation, CommitBlock, ExtendedBlock, LocatedBlock, WorkerAddress,
};
use curvine_common::FsResult;
use futures::future::try_join_all;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{DataSlice, RawPtr};
use orpc::{err_box, err_ext};
use std::collections::HashMap;
use std::sync::Arc;


use std::io::{BufWriter, Write};
enum WriterAdapter {
    Local(BlockWriterLocal),
    Remote(BlockWriterRemote),
    // BatchLocal(BatchBlockWriterLocal),
    // BatchRemote(BatchBlockWriterRemote),
}


impl std::fmt::Debug for WriterAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterAdapter::Local(writer) => {
                write!(f, "WriterAdapter::Local()")
            }
            WriterAdapter::Remote(writer) => {
                write!(f, "WriterAdapter::Remote()")
            }
        }
    }
}
impl WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(f) => f.worker_address(),
            Remote(f) => f.worker_address(),
        }
    }


    async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.write(buf).await,
            Remote(f) => f.write(buf).await,
        }
    }


    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.blocking_write(buf.clone()),
            Remote(f) => rt.block_on(f.write(buf.clone())),
        }
    }


    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.flush().await,
            Remote(f) => f.flush().await,
        }
    }


    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.complete().await,
            Remote(f) => f.complete().await,
        }
    }


    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.cancel().await,
            Remote(f) => f.cancel().await,
        }
    }


    fn remaining(&self) -> i64 {
        match self {
            Local(f) => f.remaining(),
            Remote(f) => f.remaining(),
        }
    }


    fn pos(&self) -> i64 {
        match self {
            Local(f) => f.pos(),
            Remote(f) => f.pos(),
        }
    }


    // Add seek support for WriterAdapter
    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            Local(f) => f.seek(pos).await,
            Remote(f) => f.seek(pos).await,
        }
    }


    fn len(&self) -> i64 {
        match self {
            Local(f) => f.len(),
            Remote(f) => f.len(),
        }
    }


    async fn new(
        fs_context: Arc<FsContext>,
        located_block: &LocatedBlock,
        worker_addr: &WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);


        let adapter = if short_circuit {
            let writer = BlockWriterLocal::new(
                    fs_context,
                    located_block.block.clone(),
                    worker_addr.clone(),
                    pos,
                )
                .await?;
            Local(writer)
        } else {
            // Remote writers don't support buffering
            let writer = BlockWriterRemote::new(
                &fs_context,
                located_block.block.clone(),
                worker_addr.clone(),
                pos,
            )
            .await?;
            Remote(writer)
        };


        Ok(adapter)
    }
}
enum BatchWriterAdapter {
    BatchLocal(BatchBlockWriterLocal),
    BatchRemote(BatchBlockWriterRemote),
}


impl std::fmt::Debug for BatchWriterAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchWriterAdapter::BatchLocal(writer) => {
                write!(f, "BatchWriterAdapter::BatchLocal({writer:?})")
            }
            BatchWriterAdapter::BatchRemote(writer) => {
                write!(f, "BatchWriterAdapter::BatchRemote()")
            }
        }
    }
}
impl BatchWriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            BatchLocal(f) => f.worker_address(),
            BatchRemote(f) => f.worker_address(),
        }
    }


    async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.write(files).await,
            BatchRemote(f) => f.write(files).await,
        }
    }


    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            BatchLocal(f) => Ok(()),
            BatchRemote(f) => Ok(()),
        }
    }


    async fn flush(&mut self) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.flush().await,
            BatchRemote(f) => f.flush().await,
        }
    }


    async fn complete(&mut self) -> FsResult<()> {
        match self {
            BatchLocal(f) => f.complete().await,
            BatchRemote(f) => f.complete().await,
        }
    }


    // Create new WriterAdapter
    async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: &Vec<LocatedBlock>,
        worker_addr: &WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);


        println!(
            "DEBUG, at WriterAdapter::new_batch() short_circuit: {:?}",
            short_circuit
        );


        let blocks: Vec<ExtendedBlock> = located_blocks.iter().map(|lb| lb.block.clone()).collect();
        println!("at WriterAdapter blocks= {:?}", blocks);
        let adapter = if short_circuit {
            println!("DEBUG, at WriterAdapter::new_batch(): choose BatchBlockWriterLocal");
            let writer =
                BatchBlockWriterLocal::new_batch(fs_context, blocks, worker_addr.clone(), 0)
                    .await?;
            BatchLocal(writer)
        } else {
            println!("DEBUG, at WriterAdapter::new_batch(): choose BatchBlockWriterRemote");
            let writer =
                BatchBlockWriterRemote::new_batch(&fs_context, blocks, worker_addr.clone(), 0)
                    .await?;
            BatchRemote(writer)
        };


        println!(
            "DEBUG: at WriterAdapter, at new_batch, adapter: {:?}",
            adapter
        );
        Ok(adapter)
    }
}


pub struct BlockWriter {
    inners: Vec<WriterAdapter>,
    locate: LocatedBlock,
    fs_context: Arc<FsContext>,
}


impl BlockWriter {
    pub async fn new(
        fs_context: Arc<FsContext>,
        locate: LocatedBlock,
        pos: i64,
    ) -> FsResult<Self> {
        if locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }


        let mut inners = Vec::with_capacity(locate.locs.len());
        for addr in &locate.locs {
            let adapter =
                WriterAdapter::new(fs_context.clone(), &locate, addr, pos).await?;
            inners.push(adapter);
        }


        let writer = Self {
            inners,
            locate,
            fs_context,
        };
        Ok(writer)
    }


    pub async fn write(&mut self, chunk: DataSlice) -> FsResult<()> {
        let chunk = chunk.freeze();
        let futures = self.inners.iter_mut().map(|writer| {
            let chunk_clone = chunk.clone();
            async move {
                writer
                    .write(chunk_clone)
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            }
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }


    pub fn blocking_write(&mut self, rt: &Runtime, chunk: DataSlice) -> FsResult<()> {
        if self.inners.len() == 1 {
            if let Err(e) = self.inners[0].blocking_write(rt, chunk) {
                self.fs_context
                    .add_failed_worker(self.inners[0].worker_address());
                Err(e)
            } else {
                Ok(())
            }
        } else {
            rt.block_on(self.write(chunk))?;
            Ok(())
        }
    }


    pub async fn flush(&mut self) -> FsResult<()> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .flush()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }


    pub async fn complete(&mut self) -> FsResult<CommitBlock> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .complete()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(self.to_commit_block())
    }


    pub async fn cancel(&mut self) -> FsResult<()> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .cancel()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }


    pub fn remaining(&self) -> i64 {
        self.inners[0].remaining()
    }


    pub fn has_remaining(&self) -> bool {
        self.inners[0].remaining() > 0
    }


    pub fn pos(&self) -> i64 {
        self.inners[0].pos()
    }


    pub fn len(&self) -> i64 {
        self.inners[0].len()
    }


    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }


    // Get block ID for cache management
    pub fn block_id(&self) -> i64 {
        self.locate.block.id
    }


    // Implement seek support for random writes
    pub async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative position: {}", pos);
        }


        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .seek(pos)
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }


    pub fn to_commit_block(&self) -> CommitBlock {
        let locs = self
            .locate
            .locs
            .iter()
            .map(|x| BlockLocation {
                worker_id: x.worker_id,
                storage_type: self.locate.block.storage_type,
            })
            .collect();


        CommitBlock {
            block_id: self.locate.block.id,
            block_len: self.len(),
            locations: locs,
        }
    }
}


pub struct BatchBlockWriter {
    inners: Vec<BatchWriterAdapter>,
    fs_context: Arc<FsContext>,
    located_blocks: Vec<LocatedBlock>,
    file_lengths: Vec<i64>,
}
impl BatchBlockWriter {
    /// Create multiple BlockWriters for batch operations  
    pub async fn new(
        fs_context: Arc<FsContext>,
        located_blocks: Vec<LocatedBlock>, // all blocks have the same worker information
        pos: i64,
    ) -> FsResult<Self> {
        if located_blocks.is_empty() {
            return err_box!("No blocks provided");
        }


        // Get the first block to extract worker information
        let first_locate = located_blocks.get(0).unwrap();


        if first_locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }


        // Create adapters for each worker (same workers for all blocks)
        let mut inners = Vec::with_capacity(first_locate.locs.len());
        for addr in &first_locate.locs {
            // Create a batch adapter that can handle multiple blocks
            let adapter =
                BatchWriterAdapter::new(fs_context.clone(), &located_blocks, addr, pos).await?;
            inners.push(adapter);
        }
        println!("inners at BatchBlockWriter: {:?}", inners);


        Ok(Self {
            inners,
            fs_context,
            located_blocks: located_blocks,
            file_lengths: Vec::new(),
        })
    }


    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        // Store individual file lengths
        for (_, content) in files {
            self.file_lengths.push(content.len() as i64);
        }
        // Write each file separately to all writers with index
        let futures = self.inners.iter_mut().map(|writer| {
            async move {
                writer
                    .write(files) // Pass index here
                    .await
                    .map_err(|e| (writer.worker_address().clone(), e))
            }
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        println!(
            "DEBUG at BatchBlockWriter, at write_all, self.inners: {:?}",
            self.inners
        );


        Ok(())
    }


    pub async fn flush(&mut self) -> FsResult<()> {
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .flush()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }
        Ok(())
    }


    /// Complete all writers and return commit blocks  
    pub async fn complete(&mut self) -> FsResult<Vec<CommitBlock>> {
        println!(
            "DEBUG at BatchBlockWriter, at complete, self.inners: {:?}",
            self.inners
        );
        let futures = self.inners.iter_mut().map(|writer| async move {
            writer
                .complete()
                .await
                .map_err(|e| (writer.worker_address().clone(), e))
        });


        if let Err((worker_addr, e)) = try_join_all(futures).await {
            self.fs_context.add_failed_worker(&worker_addr);
            return Err(e);
        }


        println!("DEBUG at BatchBlockWriter, end_complete");
        Ok(self.to_commit_blocks())
    }


    pub fn to_commit_blocks(&self) -> Vec<CommitBlock> {
        let mut commit_blocks = Vec::with_capacity(self.located_blocks.len());


        for (i, located_block) in self.located_blocks.iter().enumerate() {
            println!(
                "DEBUG: at BatchBlockWriter, located_block before update length: {:?}",
                located_block
            );
            let mut commit_block = CommitBlock::from(located_block);


            println!(
                "DEBUG: at BatchBlockWriter::to_commit_blocks, self.file_lengths: {:?}",
                self.file_lengths
            );
            // Use actual file length instead of 0
            if let Some(&length) = self.file_lengths.get(i) {
                commit_block.block_len = length;
            }
            println!(
                "DEBUG: at BatchBlockWriter, located_block after length: {:?}",
                commit_block
            );


            commit_blocks.push(commit_block);
        }


        commit_blocks
    }
}