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

use crate::BlockMeta;
use curvine_core_error::{err_box, CommonResult};
use curvine_model::{ExtendedBlock, StorageType};
use curvine_runtime::common::ByteUnit;

/// Snapshot of a single data directory's free-space ratio, for the
/// `disk_free_ratio` worker metric.
#[derive(Debug, Clone)]
pub struct DirFreeRatio {
    pub dir_id: u32,
    pub dir_path: String,
    pub storage_type: StorageType,
    /// `available_space / total_space` (FS) or `(capacity - used) / capacity`
    /// (SPDK), in `[0.0, 1.0]`. Pre-reserved, pre-guard.
    pub free_ratio: f64,
}

/// Proof required to release a `Quarantined` SPDK extent. Must match the
/// current metadata exactly (block id, state, storage type, offset, size)
/// or the release is rejected and the extent stays quarantined.
#[derive(Debug, Clone, Copy)]
pub struct QuarantineReleaseProof {
    pub block_id: i64,
    pub bdev_offset: i64,
    pub size: i64,
}

/// Result of attempting to release a quarantined block with a proof.
/// Distinguishes two very different "didn't release" cases so callers can
/// retry each correctly: `NotYetQuarantined` is expected to resolve almost
/// immediately (the block is still catching up internally) and must never
/// be treated as permanently unmatchable; `Stale` means the resource this
/// proof targets has already been resolved through some other path (the
/// block is gone, or exists under a different identity/storage type) and
/// is always safe to give up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineReleaseOutcome {
    /// Proof matched exactly; the block was released and its extent freed.
    Released,
    /// The block exists with the same identity (offset/size/storage type)
    /// but hasn't transitioned to `Quarantined` yet. Expected to be
    /// transient (bounded by lock contention, not "forever") in the normal
    /// case; reaching an unusually high retry count here indicates the
    /// quarantine transition itself may be stuck, not that the proof is
    /// stale.
    NotYetQuarantined,
    /// The block doesn't exist, or exists with a different identity/state/
    /// storage type than this proof describes. Whatever this proof would
    /// have released has already been resolved elsewhere; safe to drop.
    Stale,
}

// block data collection.
pub trait Dataset {
    // Total capacity
    fn capacity(&self) -> i64;

    fn capacity_str(&self) -> String {
        ByteUnit::byte_to_string(self.capacity() as u64)
    }

    // Total available capacity
    fn available(&self) -> i64;

    fn fs_used(&self) -> i64;

    fn num_blocks(&self) -> usize;

    fn num_blocks_to_delete(&self) -> usize;
    fn increment_blocks_to_delete(&self);
    fn decrement_blocks_to_delete(&self);

    fn available_str(&self) -> String {
        ByteUnit::byte_to_string(self.available() as u64)
    }

    fn get_block(&self, id: i64) -> Option<&BlockMeta>;

    // Create a new block.
    fn open_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    // Submit block
    fn finalize_block(&mut self, block: &ExtendedBlock) -> CommonResult<BlockMeta>;

    // Cancel a block
    fn abort_block(&mut self, block: &ExtendedBlock) -> CommonResult<()>;

    /// Abort with an uncertain write/flush outcome; default falls back to `abort_block`.
    fn abort_block_uncertain(&mut self, block: &ExtendedBlock) -> CommonResult<()> {
        self.abort_block(block)
    }

    /// Release a quarantined block only if `proof` exactly matches current
    /// metadata. Default: nothing to release (no quarantine concept for
    /// this storage type), reported as `Stale` since there's nothing a
    /// caller should keep retrying.
    fn release_quarantined_block_with_proof(
        &mut self,
        proof: &QuarantineReleaseProof,
    ) -> CommonResult<QuarantineReleaseOutcome> {
        let _ = proof;
        Ok(QuarantineReleaseOutcome::Stale)
    }

    // Delete a block.
    fn remove_block(&mut self, block: &ExtendedBlock) -> CommonResult<()>;

    fn get_block_check(&self, id: i64) -> CommonResult<&BlockMeta> {
        match self.get_block(id) {
            None => err_box!("block {} not exists", id),
            Some(v) => Ok(v),
        }
    }
}
