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
    pub async fn new(
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
        })
    }

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

    pub async fn write(&mut self, files: &[(&Path, &str)]) -> FsResult<()> {
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
