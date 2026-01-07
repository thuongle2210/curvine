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


use crate::block::batch_block_writer::BatchWriterAdapter::{BatchLocal, BatchRemote};


use crate::block::{
    BatchBlockWriterLocal, BatchBlockWriterRemote
};
use crate::file::FsContext;
use curvine_common::fs::Path;
use curvine_common::state::{
    CommitBlock, ExtendedBlock, LocatedBlock, WorkerAddress,
};
use curvine_common::FsResult;
use futures::future::try_join_all;
use orpc::err_box;
use std::sync::Arc;


enum BatchWriterAdapter {
    BatchLocal(BatchBlockWriterLocal),
    BatchRemote(BatchBlockWriterRemote),
}


impl std::fmt::Debug for BatchWriterAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchWriterAdapter::BatchLocal(_writer) => {
                write!(f, "BatchWriterAdapter::BatchLocal()")
            }
            BatchWriterAdapter::BatchRemote(_writer) => {
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
        located_blocks: &[LocatedBlock],
        worker_addr: &WorkerAddress,
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
                BatchBlockWriterLocal::new(fs_context, blocks, worker_addr.clone(), 0)
                    .await?;
            BatchLocal(writer)
        } else {
            println!("DEBUG, at WriterAdapter::new_batch(): choose BatchBlockWriterRemote");
            let writer =
                BatchBlockWriterRemote::new(&fs_context, blocks, worker_addr.clone(), 0)
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
    ) -> FsResult<Self> {
        if located_blocks.is_empty() {
            return err_box!("No blocks provided");
        }


        // Get the first block to extract worker information
        let first_locate = located_blocks.first().unwrap();


        if first_locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }


        // Create adapters for each worker (same workers for all blocks)
        let mut inners = Vec::with_capacity(first_locate.locs.len());
        for addr in &first_locate.locs {
            // Create a batch adapter that can handle multiple blocks
            let adapter =
                BatchWriterAdapter::new(fs_context.clone(), &located_blocks, addr).await?;
            inners.push(adapter);
        }
        println!("inners at BatchBlockWriter: {:?}", inners);


        Ok(Self {
            inners,
            fs_context,
            located_blocks,
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