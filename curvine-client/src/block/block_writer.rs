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

use crate::block::block_writer::WriterAdapter::{Local, Remote, BatchLocal, BatchRemote};
use crate::block::{BatchBlockWriterLocal,BatchBlockWriterRemote, BlockWriterLocal, BlockWriterRemote};
use crate::file::FsContext;
use bytes::buf::Writer;
use curvine_common::state::{BlockLocation, CommitBlock, LocatedBlock, WorkerAddress, ExtendedBlock};
use curvine_common::FsResult;
use futures::future::try_join_all;
use orpc::err_box;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{DataSlice, RawPtr};
use std::collections::HashMap;
use std::sync::Arc;
use curvine_common::fs::Path;  

use std::io::{BufWriter, Write};
enum WriterAdapter {
    Local(BlockWriterLocal),
    Remote(BlockWriterRemote),
    BatchLocal(BatchBlockWriterLocal),
    BatchRemote(BatchBlockWriterRemote)
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
            WriterAdapter::BatchLocal(writer) => {
                write!(f, "WriterAdapter::BatchLocal({writer:?})")
            }
            WriterAdapter::BatchRemote(writer) => {
                write!(f, "WriterAdapter::BatchRemote()")
            }
        }
    }
}
impl WriterAdapter {
    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(f) => f.worker_address(),
            Remote(f) => f.worker_address(),
            BatchLocal(f) => f.worker_address(),
            BatchRemote(f) =>   f.worker_address(),
        }
    }

    async fn write(&mut self, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.write(buf).await,
            Remote(f) => f.write(buf).await,
            BatchLocal(f) =>  Ok(()),
            BatchRemote(f) =>  Ok(()),

        }
    }

    async fn write_batch_local(&mut self, buf: DataSlice, index: i32) -> FsResult<()> {
        match self {
            Local(f) => Ok(()),
            Remote(f) => Ok(()),
            BatchLocal(f) =>  f.write(buf, index).await,
            BatchRemote(f) =>  Ok(()),
        }
    }

    async fn write_batch(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        match self {
            Local(f) => Ok(()),
            Remote(f) => Ok(()),
            BatchLocal(f) =>  f.write_v2(files).await,
            BatchRemote(f) => f.write(files).await,
        }
    }
    // async fn write_batch(&mut self, buf: Option<RawPtr<BufWriter<LocalFile>>>) -> FsResult<()> {
    //     match self {
    //         Local(f) => Ok(()),
    //         Remote(f) => Ok(()),
    //         BatchLocal(f) => f.write(buf).await,
    //     }
    // }

    fn blocking_write(&mut self, rt: &Runtime, buf: DataSlice) -> FsResult<()> {
        match self {
            Local(f) => f.blocking_write(buf.clone()),
            Remote(f) => rt.block_on(f.write(buf.clone())),
            BatchLocal(f) => Ok(()),
            BatchRemote(f) => Ok(())
        }
    }

    async fn flush(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.flush().await,
            Remote(f) => f.flush().await,
            BatchLocal(f) => f.flush().await,
            BatchRemote(f) => f.flush().await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.complete().await,
            Remote(f) => f.complete().await,
            BatchLocal(f) => f.complete().await,
            BatchRemote(f) => f.complete().await,
        }
    }

    async fn cancel(&mut self) -> FsResult<()> {
        match self {
            Local(f) => f.cancel().await,
            Remote(f) => f.cancel().await,
            BatchLocal(f) => Ok(()),
            BatchRemote(f) => Ok(()),
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(f) => f.remaining(),
            Remote(f) => f.remaining(),
            BatchLocal(f) => 0 as i64,
            BatchRemote(f) => 0 as i64,
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(f) => f.pos(),
            Remote(f) => f.pos(),
            BatchLocal(f) => 0 as i64,
            BatchRemote(f) => 0 as i64,
        }
    }

    // Add seek support for WriterAdapter
    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match self {
            Local(f) => f.seek(pos).await,
            Remote(f) => f.seek(pos).await,
            BatchLocal(f) => Ok(()),
            BatchRemote(f) => Ok(()),
        }
    }

    fn len(&self) -> i64 {
        match self {
            Local(f) => f.len(),
            Remote(f) => f.len(),
            BatchLocal(f) => 0 as i64,
            BatchRemote(f) => 0 as i64,
        }
    }

    // fn block_id(&self) -> i64 {  
    //     match self {  
    //         Local(f) => 0 as i64,  // Access block ID from BlockWriterLocal  
    //         Remote(f) => 0 as i64, // Access block ID from BlockWriterRemote  
    //         BatchLocal(f) => f,
    //     }  
    // }  

    // Create new WriterAdapter
    async fn new_batch(  
    fs_context: Arc<FsContext>,  
    located_blocks: &Vec<LocatedBlock>,  
    worker_addr: &WorkerAddress,  
    pos: i64,  
) -> FsResult<Self> {  
    let conf = &fs_context.conf.client;  
    let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);  
    
    println!("DEBUG, at WriterAdapter::new_batch() short_circuit: {:?}", short_circuit);
    // let adapter = if short_circuit {    
    //     // Extract ExtendedBlock from each LocatedBlock for batch processing  
    //     let blocks: Vec<ExtendedBlock> = located_blocks  
    //         .iter()  
    //         .map(|lb| lb.block.clone())  
    //         .collect();  
            
    //     let writer = BatchBlockWriterLocal::new_batch(    
    //         fs_context,    
    //         blocks,    
    //         worker_addr.clone(),    
    //         0  
    //     )    
    //     .await?;  
    //     BatchLocal(writer)    
    // } 
    // else {    
    //     // For remote workers, use the first block (single file approach)  
    //     let first_block = located_blocks  
    //         .first()  
    //         .ok_or_else(|| err_box!("No blocks provided"));  
            
    //     let blocks: Vec<ExtendedBlock> = located_blocks  
    //         .iter()  
    //         .map(|lb| lb.block.clone())  
    //         .collect();

    //     let writer = BlockWriterRemote::new(    
    //         &fs_context,    
    //         blocks.first(),    
    //         worker_addr.clone(),    
    //         pos,    
    //     )    
    //     .await?;    
    //     Remote(writer)    
    // };    


    let blocks: Vec<ExtendedBlock> = located_blocks  
        .iter()  
        .map(|lb| lb.block.clone())  
        .collect();  

    println!("at WriterAdapter blocks= {:?}", blocks);
    let adapter = if short_circuit { 
        println!("DEBUG, at WriterAdapter::new_batch(): choose BatchBlockWriterLocal");
        let writer = BatchBlockWriterLocal::new_batch(    
            fs_context,    
            blocks,    
            worker_addr.clone(),    
            0  
        )    
        .await?;  
        BatchLocal(writer)
    } else {
        println!("DEBUG, at WriterAdapter::new_batch(): choose BatchBlockWriterRemote");
        let writer = BatchBlockWriterRemote::new_batch(
            &fs_context,    
            blocks,    
            worker_addr.clone(),    
            0  
        )    
        .await?;  
        BatchRemote(writer)
    };
    
    // will change to local or remote by short circuit configuration in the next time
    // let writer = BatchBlockWriterLocal::new_batch(    
    //     fs_context,    
    //     blocks,    
    //     worker_addr.clone(),    
    //     0  
    // )    
    // .await?;  

    // try with BatchLocal and BatchRemote
    // let adapter = BatchLocal(writer);  
    
    println!("DEBUG: at WriterAdapter, at new_batch, adapter: {:?}", adapter);
    Ok(adapter)  
}

    async fn new(
        fs_context: Arc<FsContext>,
        located_block: &LocatedBlock,
        worker_addr: &WorkerAddress,
        pos: i64,
        use_buffered: bool,
    ) -> FsResult<Self> {
        let conf = &fs_context.conf.client;
        let short_circuit = conf.short_circuit && fs_context.is_local_worker(worker_addr);

        let adapter = if short_circuit {  
            let writer = if use_buffered {  
                // Use buffered Local writer  
                BlockWriterLocal::new_buffered(  
                    fs_context,  
                    located_block.block.clone(),  
                    worker_addr.clone(),  
                    pos,  
                )  
                .await?  
            } else {  
                // Use regular Local writer  
                BlockWriterLocal::new(  
                    fs_context,  
                    located_block.block.clone(),  
                    worker_addr.clone(),  
                    pos,  
                )  
                .await?  
            };  
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

pub struct BlockWriter {
    inners: Vec<WriterAdapter>,
    locate: LocatedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockWriter {
    pub async fn new(fs_context: Arc<FsContext>, locate: LocatedBlock, pos: i64,  use_buffered: bool) -> FsResult<Self> {
        if locate.locs.is_empty() {
            return err_box!("There is no available worker");
        }

        let mut inners = Vec::with_capacity(locate.locs.len());
        for addr in &locate.locs {
            let adapter = WriterAdapter::new(fs_context.clone(), &locate, addr, pos, use_buffered).await?;
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
    inners: Vec<WriterAdapter>, 
    buffer: BufWriter<Vec<u8>>,   
    fs_context: Arc<FsContext>,
    file_blocks: Vec<(Path, LocatedBlock)>, 
    located_blocks: Vec<LocatedBlock>,
    file_lengths: Vec<i64>,
}
impl BatchBlockWriter {  
    /// Create multiple BlockWriters for batch operations  
    pub async fn new_batch(    
        fs_context: Arc<FsContext>,    
        located_blocks: Vec<LocatedBlock>,  // all blocks have the same worker information  
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
            let adapter = WriterAdapter::new_batch(  
                fs_context.clone(),   
                &located_blocks,   
                addr,   
                pos  
            ).await?;  
            inners.push(adapter);  
        }  
        println!("inners at BatchBlockWriter: {:?}", inners);
        
        Ok(Self {    
            inners,    
            buffer: BufWriter::new(Vec::new()),    
            fs_context,
            file_blocks: Vec::new(),
            located_blocks: located_blocks,
            file_lengths: Vec::new(),
        })    
    }
      
    pub fn serialize_files_for_remote(files: &[(&Path, &str)]) -> FsResult<DataSlice> {  
        let mut buffer = Vec::new();  
        
        // Write file count  
        buffer.extend_from_slice(&(files.len() as u32).to_le_bytes());  
        
        for (path, content) in files {  
            let path_bytes = path.full_path().as_bytes();  
            buffer.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());  
            buffer.extend_from_slice(path_bytes);  
            
            let content_bytes = content.as_bytes();  
            buffer.extend_from_slice(&(content_bytes.len() as u32).to_le_bytes());  
            buffer.extend_from_slice(content_bytes);  
        }  
        
        Ok(DataSlice::Bytes(bytes::Bytes::from(buffer)))
    }

    // /// Write data to all writers without flushing  
    // pub async fn write_all(&mut self, files: &[(&Path, &str)]) -> FsResult<()> { 
    //     // Clear previous mappings  
    //     self.file_blocks.clear();  
          
    //     // Store individual file lengths  
    //     for (_, content) in files {  
    //         self.file_lengths.push(content.len() as i64);  
    //     }
        
    //     // Serialize all files into a single DataSlice  
    //     let serialized_data = Self::serialize_files_for_remote(files)?;  
        
    //     // Write to all writers (BatchBlockWriterLocal handles buffering internally)  
    //     let futures = self.inners.iter_mut().map(|writer| {  
    //         let data_clone = serialized_data.clone();  
    //         async move {  
    //             writer  
    //                 .write(data_clone)  
    //                 .await  
    //                 .map_err(|e| (writer.worker_address().clone(), e))  
    //         }  
    //     });  
          
    //     if let Err((worker_addr, e)) = try_join_all(futures).await {  
    //         self.fs_context.add_failed_worker(&worker_addr);  
    //         return Err(e);  
    //     }  
          
    //     println!("Written {} files to all writers", files.len());  
    //     Ok(())  
    // } 

    /// Write data to all writers without flushing  
    /// consider for remote
    /// ********************* remmember it
    // pub async fn write_all(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {     
    //     self.file_blocks.clear();      
        
    //     // Store individual file lengths      
    //     for (_, content) in files {      
    //         self.file_lengths.push(content.len() as i64);      
    //     }
        
    //     // Concatenate all file contents (no metadata)      
    //     let mut all_content = Vec::new();      
    //     let mut boundaries = Vec::new();    
    //     let mut current_offset = 0;    
        
    //     for (_, content) in files {      
    //         let content_len = content.len();    
    //         boundaries.push(current_offset);    
    //         all_content.extend_from_slice(content.as_bytes());    
    //         current_offset += content_len;    
    //     }    
        
    //     // Add boundary footer AFTER all content    
    //     println!("DEBUG: at BatchBlockWriter write_all, boundaries: {:?}", boundaries);  
    //     all_content.extend_from_slice(&(boundaries.len() as u32).to_le_bytes());    
    //     for &boundary in &boundaries {    
    //         all_content.extend_from_slice(&(boundary as u32).to_le_bytes());    
    //     }    

    //     all_content.extend_from_slice(&[0xFF]); // Clear delimiter  
    //     for &boundary in &boundaries {  
    //         all_content.extend_from_slice(&(boundary as u32).to_le_bytes());  
    //     }  
    //     all_content.push(boundaries.len() as u8); // Block count as single byte at end
        
    //     let data = DataSlice::Bytes(bytes::Bytes::from(all_content));    
        
    //     // Debug: Verify the data structure  
    //     println!("DEBUG: Total data length: {}, boundaries count: {}",   
    //             data.len(), boundaries.len());  
        
    //     // Write to all writers      
    //     let futures = self.inners.iter_mut().map(|writer| {      
    //         let data_clone = data.clone();      
    //         async move {      
    //             writer      
    //                 .write(data_clone)      
    //                 .await      
    //                 .map_err(|e| (writer.worker_address().clone(), e))      
    //         }      
    //     });      
        
    //     if let Err((worker_addr, e)) = try_join_all(futures).await {      
    //         self.fs_context.add_failed_worker(&worker_addr);      
    //         return Err(e);      
    //     }      
        
    //     Ok(())      
    // }

    // write all for local, it's done

    /// remember to merge with write to remote
    // pub async fn write_all(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {  
    //     for (index, (path, content)) in files.iter().enumerate() {
    //         let data = DataSlice::Bytes(bytes::Bytes::copy_from_slice(content.as_bytes()));
            
    //         // Store individual file lengths      
    //         for (_, content) in files {      
    //             self.file_lengths.push(content.len() as i64);      
    //         }
    //         // Write each file separately to all writers with index  
    //         let futures = self.inners.iter_mut().map(|writer| {  
    //             let data_clone = data.clone();
    //             let index = index as i64;  // Assuming write_batch_local expects i64 index
    //             async move {  
    //                 writer  
    //                     .write_batch_local(data_clone, index as i32)  // Pass index here
    //                     .await  
    //                     .map_err(|e| (writer.worker_address().clone(), e))  
    //             }  
    //         });  
            
    //         if let Err((worker_addr, e)) = try_join_all(futures).await {  
    //             self.fs_context.add_failed_worker(&worker_addr);  
    //             return Err(e);  
    //         }  
    //     }  
    //     Ok(())  
    // }


    pub async fn write_all(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {   
        // Store individual file lengths      
        for (_, content) in files {      
            self.file_lengths.push(content.len() as i64);      
        }
        // Write each file separately to all writers with index  
        let futures = self.inners.iter_mut().map(|writer| {  
        async move {  
            writer  
                .write_batch(files)  // Pass index here
                .await  
                .map_err(|e| (writer.worker_address().clone(), e))  
        }  
        });  
        
        if let Err((worker_addr, e)) = try_join_all(futures).await {  
            self.fs_context.add_failed_worker(&worker_addr);  
            return Err(e);  
        }  
        println!("DEBUG at BatchBlockWriter, at write_all, self.inners: {:?}", self.inners);
        
        Ok(())  
    }


    /// Reconstruct commit blocks from the original located_blocks  
    // pub fn to_commit_blocks(&self) -> Vec<CommitBlock> {  
    //     // You need to store the original located_blocks in the struct 
    //     println!("located_blocks at to_commit_blocks: {:?}", self.located_blocks); 
    //     self.located_blocks  
    //         .iter()  
    //         .map(CommitBlock::from)  // Uses existing From trait  
    //         .collect()  
    // }  
    // /// Flush all buffered data to writers at once  
    // pub async fn flush_all(&mut self) -> FsResult<()> {  
    //     // First flush our internal buffer  
    //     self.buffer.flush().map_err(|e| {  
    //         curvine_common::error::FsError::from(e)  
    //     })?;  
          
    //     // Now write all stored data to actual writers  
    //     for (i, (path, content)) in self.file_data.iter().enumerate() {  
    //         if let Some(writer) = self.writers.get_mut(i) {  
    //             writer.write(DataSlice::from_bytes(content)).await?;  
    //         }  
    //     }  
          
    //     // Single flush operation for all writers  
    //     let futures: Vec<_> = self.writers.iter_mut()  
    //         .map(|writer| writer.flush())  
    //         .collect();  
    //     try_join_all(futures).await?;  
          
    //     println!("Flushed all {} files in one operation", self.file_data.len());  
    //     Ok(())  
    // } 


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
        println!("DEBUG at BatchBlockWriter, at complete, self.inners: {:?}", self.inners);
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

        // Collect commit blocks like FsWriterBase does  
        // let mut commits = Vec::new();  
        // for writer in &self.inners {  
        //     let commit = writer.to_commit_block();  
        //     self.add_commit(commit.clone())?;  // Update block lengths  
        //     commits.push(commit);  
        // }  
        Ok(self.to_commit_blocks())
    }  

    pub fn to_commit_blocks(&self) -> Vec<CommitBlock> {    
        let mut commit_blocks = Vec::new();  
        
        for (i, located_block) in self.located_blocks.iter().enumerate() { 
            println!("DEBUG: at BatchBlockWriter, located_block before update length: {:?}", located_block); 
            let mut commit_block = CommitBlock::from(located_block);  
            
            println!("DEBUG: at BatchBlockWriter::to_commit_blocks, self.file_lengths: {:?}", self.file_lengths);
            // Use actual file length instead of 0  
            if let Some(&length) = self.file_lengths.get(i) {  
                commit_block.block_len = length;  
            }  
            println!("DEBUG: at BatchBlockWriter, located_block after length: {:?}", commit_block); 
            
            commit_blocks.push(commit_block);  
        }  
        
        commit_blocks  
    }

    // fn add_commit(&mut self, commit: CommitBlock) -> FsResult<()> {  
    //     // Update the corresponding located_block's length  
    //     if let Some(lb) = self.located_blocks.iter_mut()  
    //         .find(|b| b.id == commit.block_id) {  
    //         lb.block.len = commit.block_len;  
    //     }  
    //     self.commit_blocks.insert(commit.block_id, commit);  
    //     Ok(())  
    // }   
}