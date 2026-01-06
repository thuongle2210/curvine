use crate::file::FsContext;
use curvine_common::error::FsError;
use curvine_common::fs::Path;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use orpc::common::Utils;
use orpc::io::LocalFile;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::{DataSlice, RawPtr};
use orpc::{err_box, try_option};
use std::io::{BufWriter, Write};
use std::sync::Arc;

pub struct BatchBlockWriterLocal {
    rt: Arc<Runtime>,
    fs_context: Arc<FsContext>,
    blocks: Vec<ExtendedBlock>,
    worker_address: WorkerAddress,
    files: Vec<RawPtr<LocalFile>>,
    block_size: i64,
    req_id: i64,
    pos: i64,
    block_index: usize,
}

impl std::fmt::Debug for BatchBlockWriterLocal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchBlockWriterLocal")
            .field("rt", &self.rt)
            .field("blocks", &self.blocks)
            .field("worker_address", &self.worker_address)
            .field("buffered_file", &self.files)
            .field("block_size", &self.block_size)
            .field("req_id", &self.req_id)
            .finish()
    }
}

impl BatchBlockWriterLocal {
    pub async fn new_batch(
        fs_context: Arc<FsContext>,
        blocks: Vec<ExtendedBlock>,
        worker_address: WorkerAddress,
        pos: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let block_size = fs_context.block_size();
        println!(
            "DEBUG: at BatchBlockWriterLocal, block_size= {:?}",
            block_size
        );
        let client = fs_context.block_client(&worker_address).await?;

        // SINGLE RPC call to setup multiple blocks
        let write_context = client
            .write_blocks_batch(
                &blocks,
                0,
                block_size,
                req_id,
                0 as i32,
                fs_context.write_chunk_size() as i32,
                true,
            )
            .await?;

        println!("write_context: {:?}", write_context);

        // Create multiple files, one for each block context
        let mut files = Vec::new();
        for context in &write_context.contexts {
            let path = try_option!(&context.path);
            let file = LocalFile::with_write_offset(path, false, pos)?;
            files.push(RawPtr::from_owned(file));
        }

        Ok(Self {
            rt: fs_context.clone_runtime(),
            fs_context,
            blocks,
            worker_address,
            files,
            block_size,
            req_id,
            pos: 0,
            block_index: 0,
        })
    }

    // Write to multiple blocks without RPCs
    // pub async fn write_to_blocks(&mut self, block_data: Vec<(usize, DataSlice)>) -> FsResult<()> {
    //     for (block_idx, data) in block_data {
    //         if let Some(file) = self.files.get_mut(block_idx) {
    //             let file_clone = file.clone();
    //             self.rt
    //                 .spawn_blocking(move || {
    //                     use std::io::Write;
    //                     file_clone.write_all(data.as_slice())?;
    //                     Ok::<(), FsError>(())
    //                 })
    //                 .await??;
    //         }
    //     }
    //     Ok(())
    // }

    // SINGLE RPC call to complete all blocks
    pub async fn complete(&mut self) -> FsResult<()> {
        println!("DEBUG at BatchBlockWriterLocal, at complete start game");
        // flush before commit
        self.flush().await?;
        let client = self.fs_context.block_client(&self.worker_address).await?;
        client
            .write_commit_batch(
                &self.blocks,
                self.pos,
                self.fs_context.block_size(),
                Utils::req_id(),
                0,
                false,
            )
            .await
    }

    // Flush all buffered files
    pub async fn flush(&mut self) -> FsResult<()> {
        for file in &mut self.files {
            let file_clone = file.clone();
            println!("DEBUG: at LocalFile, we flush file: {:?}", file_clone);
            self.rt
                .spawn_blocking(move || {
                    file_clone.as_mut().flush()?;
                    Ok::<(), FsError>(())
                })
                .await??;
        }
        Ok(())
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }

    // pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
    //     // Convert DataSlice to bytes and write to internal buffer
    //     let bytes = match data {
    //         DataSlice::Empty => return Ok(()),
    //         DataSlice::Bytes(bytes) => bytes.to_vec(),
    //         DataSlice::Buffer(buf) => buf.to_vec(),
    //         DataSlice::IOSlice(slice) => Vec::new(),
    //         DataSlice::MemSlice(slice) => Vec::new(),
    //     };

    //     // // Update position for all blocks (they share the same file)
    //     // for i in 0..self.block_positions.len() {
    //     //     self.block_positions[i] += write_len;
    //     // }

    //     // // Update block lengths to match current position
    //     // for block in &mut self.blocks {
    //     //     if self.block_positions[0] > block.len {
    //     //         block.len = self.block_positions[0];
    //     //     }
    //     // }

    //     // Get current file position after write

    // // Write to internal BufWriter (no actual I/O yet)
    // println!("DEBUG: at BatchBlockWriterLocal write, writing {} bytes", bytes.len());
    // self.buffered_file.as_mut().unwrap().write_all(&bytes).map_err(|e| {
    //     curvine_common::error::FsError::from(e)
    // })?;

    //     let current_pos = bytes.len() as i64;
    //     self.pos += current_pos;
    //     println!("DEBUG: at BatchBlockWriterLocal write, current_pos = {:?}", current_pos);
    //     for block in &mut self.blocks {
    //         if current_pos > block.len {
    //             block.len = current_pos;
    //         }
    //     }

    //     Ok(())
    // }
    // pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
    //     let bytes = match data {
    //         DataSlice::Bytes(bytes) => bytes.to_vec(),
    //         _ => return Ok(()),
    //     };

    //     if bytes.len() < 5 {
    //         return Ok(());
    //     }

    //     // Get block count from last byte
    //     let block_count = bytes[bytes.len() - 1] as usize;

    //     // Find delimiter (0xFF) to locate boundary start
    //     let mut boundary_start = bytes.len() - 1 - (block_count * 4);
    //     while boundary_start > 0 && bytes[boundary_start] != 0xFF {
    //         boundary_start -= 1;
    //     }

    //     if boundary_start == 0 || bytes[boundary_start] != 0xFF {
    //         return err_box!("Invalid footer format - delimiter not found");
    //     }

    //     println!("DEBUG: extracted boundary_start: {:?}", boundary_start);
    //     boundary_start += 1; // Skip delimiter

    //     // Extract boundaries
    //     let mut boundaries = Vec::new();
    //     for i in 0..block_count {
    //         let start = boundary_start + (i * 4);
    //         if start + 4 <= bytes.len() - 1 {
    //             let boundary = u32::from_le_bytes([
    //                 bytes[start], bytes[start+1],
    //                 bytes[start+2], bytes[start+3]
    //             ]) as usize;
    //             boundaries.push(boundary);
    //         }
    //     }

    //     let mut totals = 0;
    //     // Calculate this block's length and write only its data
    //     if self.block_index < boundaries.len() {
    //         let start = boundaries[self.block_index];
    //         let end = if self.block_index + 1 < boundaries.len() {
    //             boundaries[self.block_index + 1]
    //         } else {
    //             boundary_start - 1
    //         };

    //         let block_len = (end - start) as i64;

    //         // Update block length
    //         for block in &mut self.blocks {
    //             println!("DEBUG: updating block length to {}", block_len);
    //             block.len = block_len;
    //             totals += block_len;
    //         }
    //     }

    //     // FIX: Write only this block's data (not the entire buffer)
    //     let block_data = &bytes[..totals as usize];
    //     println!("DEBUG: writing block {} data: {} bytes", self.block_index, block_data.len());
    //     self.buffered_file.as_mut().unwrap().write_all(block_data)?;

    //     Ok(())
    // }

    pub async fn write(&mut self, data: DataSlice, index: i32) -> FsResult<()> {
        println!(
            "DEBUG at BatchBlockWriter, with data: {:?}, index: {:?}",
            data, index
        );
        // Convert DataSlice to bytes (same as BlockWriterLocal)
        let bytes = match data {
            DataSlice::Empty => return Ok(()),
            DataSlice::Bytes(bytes) => bytes.to_vec(),
            DataSlice::Buffer(buf) => buf.to_vec(),
            DataSlice::IOSlice(slice) => Vec::new(),
            DataSlice::MemSlice(slice) => Vec::new(),
        };

        if bytes.is_empty() {
            return Ok(());
        }

        // Write to current file using spawn_blocking (like BlockWriterLocal)
        let mut file = self.files[index as usize].clone();
        let bytes_clone = bytes.clone();

        self.rt
            .spawn_blocking(move || {
                file.as_mut().write_all(&bytes_clone)?;
                Ok::<(), FsError>(())
            })
            .await??;

        let current_pos = bytes.len() as i64;
        // Update block length for current block
        if current_pos > self.blocks[index as usize].len {
            self.blocks[index as usize].len = current_pos;
        }
        Ok(())
    }

    pub async fn write_v2(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
        println!("DEBUG at BatchBlockWriter, with files: {:?}", files);

        for (index, file) in files.iter().enumerate() {
            let local_file = self.files[index as usize].clone();
            let current_pos = file.1.len() as i64;
            let content_owned = file.1.to_string();
            let handle = self.rt.spawn_blocking(move || {
                let bytes_content = bytes::Bytes::copy_from_slice(content_owned.as_bytes());
                local_file.as_mut().write_all(&bytes_content)?;
                Ok::<(), FsError>(())
            });
            handle.await??;

            // update length of block
            if current_pos > self.blocks[index as usize].len {
                self.blocks[index as usize].len = current_pos;
            }
        }

        Ok(())
    }

    // pub async fn write(&mut self, data: DataSlice) -> FsResult<()> {
    //     let bytes = match data {
    //         DataSlice::Bytes(bytes) => bytes.to_vec(),
    //         _ => return Ok(()),
    //     };

    //     if bytes.len() < 5 {
    //         return Ok(());
    //     }

    //     // Get block count from last byte
    //     let block_count = bytes[bytes.len() - 1] as usize;

    //     // Find delimiter (0xFF) to locate boundary start
    //     let mut boundary_start = bytes.len() - 1 - (block_count * 4);
    //     while boundary_start > 0 && bytes[boundary_start] != 0xFF {
    //         boundary_start -= 1;
    //     }

    //     if boundary_start == 0 || bytes[boundary_start] != 0xFF {
    //         return err_box!("Invalid footer format - delimiter not found");
    //     }

    //     println!("DEBUG: extracted boundary_start: {:?}", boundary_start);
    //     // FIX: Write ALL content (40 bytes), not just this block's portion
    //     let content_end = boundary_start;
    //     if content_end > 0 {
    //         let content = &bytes[0..content_end]; // All 40 bytes of content

    //         // Write all content to this block
    //         self.buffered_file.as_mut().unwrap().write_all(content)?;

    //         // Update block length to total content size
    //         let total_len = content.len() as i64;
    //         for block in &mut self.blocks {
    //             println!("DEBUG: updating block length to {}", total_len);
    //             block.len = total_len;
    //         }
    //     }

    //     Ok(())
    // }
    // pub fn file_pos(&self) -> FsResult<i64> {
    //     match &self.buffered_file {
    //         Some(buffered) => {
    //             let file = buffered.get_ref();
    //             Ok(file.pos())
    //         }
    //         None => err_box!("No file initialized")
    //     }
    // }
}
