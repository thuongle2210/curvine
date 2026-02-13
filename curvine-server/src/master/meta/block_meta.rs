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

use curvine_common::proto::BlockMetaProto;
use curvine_common::{
    state::{BlockLocation, CommitBlock, FileAllocOpts, WorkerAddress},
    utils::ProtoUtils,
};
use serde::{Deserialize, Serialize};

#[allow(unused)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize, Serialize)]
#[repr(i8)]
pub enum BlockState {
    Complete = 0,
    Committed = 1,
    Writing = 2,
    Recovering = 3,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockMeta {
    pub(crate) id: i64,
    pub(crate) len: i64,
    pub(crate) replicas: u16,
    // The pre-assigned worker id is required when deleting.
    pub(crate) locs: Option<Vec<BlockLocation>>,
    pub(crate) alloc_opts: Option<FileAllocOpts>,
}

impl BlockMeta {
    pub fn new(id: i64, len: i64) -> Self {
        Self {
            id,
            len,
            replicas: 1,
            locs: None,
            alloc_opts: None,
        }
    }

    // Pre-allocated worker block
    pub fn with_pre(id: i64, workers: &[WorkerAddress]) -> Self {
        let locs = workers
            .iter()
            .map(|x| BlockLocation::with_id(x.worker_id))
            .collect();
        Self {
            id,
            len: 0,
            replicas: 1,
            locs: Some(locs),
            alloc_opts: None,
        }
    }

    pub fn with_alloc(id: i64, alloc_opts: FileAllocOpts) -> Self {
        Self {
            id,
            len: alloc_opts.len,
            replicas: 0,
            locs: None,
            alloc_opts: Some(alloc_opts),
        }
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn commit(&mut self, commit: &CommitBlock) {
        self.len = self.len.max(commit.block_len);
        let _ = self.locs.take();
        let _ = self.alloc_opts.take();
    }

    pub fn assign_worker(&mut self, workers: &[WorkerAddress]) -> bool {
        if self.alloc_opts.is_none() || self.locs.is_some() {
            false
        } else {
            let locs = workers
                .iter()
                .map(|x| BlockLocation::with_id(x.worker_id))
                .collect();
            self.locs = Some(locs);
            true
        }
    }
    pub fn matching_block(a: Option<&BlockMeta>, b: Option<&CommitBlock>) -> bool {
        let id_a = a.map(|x| x.id);
        let id_b = b.map(|x| x.block_id);
        id_a == id_b
    }
    pub fn block_meta_to_pb(meta: BlockMeta) -> BlockMetaProto {
        BlockMetaProto {
            id: meta.id,
            len: meta.len,
            replicas: meta.replicas as u32,
            locs: meta
                .locs
                .map(|vec| {
                    vec.into_iter()
                        .map(ProtoUtils::block_location_to_pb)
                        .collect()
                })
                .unwrap_or_default(),
            alloc_opts: meta.alloc_opts.map(ProtoUtils::file_alloc_opts_to_pb),
        }
    }

    pub fn block_meta_from_pb(proto: BlockMetaProto) -> BlockMeta {
        Self {
            id: proto.id,
            len: proto.len,
            replicas: proto.replicas as u16,
            locs: if proto.locs.is_empty() {
                None
            } else {
                Some(
                    proto
                        .locs
                        .into_iter()
                        .map(ProtoUtils::block_location_from_pb)
                        .collect(),
                )
            },
            alloc_opts: proto.alloc_opts.map(ProtoUtils::file_alloc_opts_from_pb),
        }
    }
}
