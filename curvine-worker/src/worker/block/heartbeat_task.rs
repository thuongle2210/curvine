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

use crate::worker::block::{BlockStore, MasterClient};
use crate::worker::storage::{Dataset, QuarantineReleaseOutcome, QuarantineReleaseProof};
use crate::worker::Worker;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_model::ProtoUtils;
use curvine_model::{BlockReportInfo, HeartbeatStatus, WorkerCommand};
use curvine_rpc::server::ServerState;
use curvine_runtime::runtime::{GroupExecutor, LoopTask};
use curvine_runtime::sync::StateCtl;
use dashmap::{DashMap, DashSet};
use log::{error, warn};
use std::sync::{Arc, Mutex};

/// A quarantine-release proof still waiting for an exact metadata match,
/// plus how many heartbeat ticks it has been retried.
pub(crate) struct PendingQuarantineProof {
    pub(crate) proof: QuarantineReleaseProof,
    pub(crate) attempts: u32,
    /// Set once this proof has crossed `stuck_warn_retries` while still
    /// `NotYetQuarantined`, so the one-time warning log and the
    /// `spdk_quarantine_proof_stuck` gauge only fire/increment once per
    /// proof, and so the gauge can be correctly decremented whenever this
    /// proof eventually leaves the pending queue (released or dropped).
    pub(crate) warned: bool,
    /// Set once a persistent store `Err` has been logged for this proof,
    /// so repeated `Err` results on later ticks don't re-log every tick
    /// (which would otherwise flood logs for as long as the underlying
    /// store error persists, since `store_err_max_retries` defaults to
    /// unlimited). `attempts`/the retry cap are still tracked and applied
    /// normally regardless of this flag -- only the per-tick log line is
    /// suppressed after the first occurrence.
    pub(crate) err_warned: bool,
}

impl PendingQuarantineProof {
    /// Returns `true` only the first time this is called for a given
    /// proof; `false` on every subsequent call. Used to gate the
    /// persistent-store-`Err` log line to a single occurrence per proof
    /// instead of once per heartbeat tick.
    fn should_log_store_err(&mut self) -> bool {
        if self.err_warned {
            false
        } else {
            self.err_warned = true;
            true
        }
    }
}

pub struct HeartbeatTask {
    pub(crate) executor: Arc<GroupExecutor>,
    pub(crate) worker_ctl: StateCtl,
    pub(crate) client: MasterClient,
    pub(crate) store: BlockStore,
    pub(crate) report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
    pub(crate) pending_deletes: Arc<DashSet<i64>>,
    /// Quarantine release proofs that didn't yet match (quarantine not
    /// visible to this dataset generation, or a transient store error),
    /// retried every tick. See `Dataset::release_quarantined_block_with_proof`.
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    pub(crate) pending_quarantine_proofs: Arc<Mutex<Vec<PendingQuarantineProof>>>,
    /// Give up on a persistent store `Err` after this many retries. `0` =
    /// unlimited. `Stale` proofs are always dropped immediately (no retry
    /// needed -- the answer can't change on a later tick), so this no
    /// longer caps anything other than transient store errors.
    /// See `WorkerConf::spdk_quarantine_proof_max_retries`.
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    pub(crate) quarantine_proof_store_err_max_retries: u32,
    /// Give up on a `NotYetQuarantined` proof after this many retries. `0`
    /// = unlimited. Expected to be much larger than the store-err cap,
    /// since reaching it means the quarantine transition itself is stuck,
    /// not that the proof is merely stale (which drops immediately, see
    /// above). See `WorkerConf::spdk_quarantine_proof_stuck_max_retries`.
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    pub(crate) quarantine_proof_stuck_max_retries: u32,
    /// Emit a one-time warning + raise `spdk_quarantine_proof_stuck` once a
    /// `NotYetQuarantined` proof has been retried this many ticks, as an
    /// early, non-terminal signal before `quarantine_proof_stuck_max_retries`
    /// is ever reached (or forever, if that cap is disabled). `0` =
    /// disabled. See `WorkerConf::spdk_quarantine_proof_stuck_warn_retries`.
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    pub(crate) quarantine_proof_stuck_warn_retries: u32,
}

impl HeartbeatTask {
    // Asynchronously delete the block file.
    pub(crate) fn delete_block_task(
        executor: Arc<GroupExecutor>,
        store: BlockStore,
        cmds: Vec<WorkerCommand>,
        report_blocks: Arc<DashMap<i64, BlockReportInfo>>,
        pending_deletes: Arc<DashSet<i64>>,
    ) {
        for cmd in cmds {
            match cmd {
                WorkerCommand::DeleteBlock(c) => {
                    for block in c.blocks {
                        if !pending_deletes.insert(block) {
                            continue;
                        }

                        if let Err(e) = store
                            .write()
                            .map(|state| state.increment_blocks_to_delete())
                        {
                            pending_deletes.remove(&block);
                            error!("failed to mark block {} deleting: {}", block, e);
                            continue;
                        }

                        let store1 = store.clone();
                        let report_blocks1 = report_blocks.clone();
                        let pending_deletes1 = pending_deletes.clone();
                        let res = executor.spawn(move || match store1.async_remove_block(block) {
                            Ok(Some(meta)) => {
                                report_blocks1
                                    .insert(block, BlockReportInfo::with_deleted(block, meta.len));
                            }
                            Ok(None) => {
                                report_blocks1
                                    .insert(block, BlockReportInfo::with_deleted(block, 0));
                            }
                            Err(e) => {
                                warn!("async_remove_block {}: {}", block, e);
                                pending_deletes1.remove(&block);
                            }
                        });

                        if let Err(e) = res {
                            pending_deletes.remove(&block);
                            match store.write() {
                                Ok(state) => state.decrement_blocks_to_delete(),
                                Err(err) => error!(
                                    "failed to clear deleting state for block {}: {}",
                                    block, err
                                ),
                            }
                            warn!("{}", e);
                        }
                    }
                }
            }
        }
    }

    pub fn get_report_blocks(&self) -> Vec<BlockReportInfo> {
        let mut vec = vec![];
        let blocks = self
            .report_blocks
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>();

        for block in blocks {
            if let Some(v) = self.report_blocks.remove(&block) {
                vec.push(v.1);
            }
        }
        vec
    }

    pub fn put_missing_report(&self, blocks: Vec<BlockReportInfo>) {
        for block in blocks {
            self.report_blocks.insert(block.id, block);
        }
    }

    fn acknowledge_reports(&self, blocks: &[BlockReportInfo]) {
        for block in blocks {
            if block.status == curvine_model::BlockReportStatus::Deleted {
                self.pending_deletes.remove(&block.id);
            }
        }
    }

    /// Drain SPDK terminal-completion events and retry any proof that
    /// didn't match last tick (the quarantine may not have been visible yet
    /// when the event arrived — see `pending_quarantine_proofs`).
    #[cfg(feature = "spdk")]
    fn process_quarantine_terminal_events(&self) {
        let new_proofs: Vec<QuarantineReleaseProof> = curvine_storage_spdk::SpdkEnv::global()
            .map(|env| {
                env.drain_terminal_events()
                    .into_iter()
                    .map(|event| QuarantineReleaseProof {
                        block_id: event.block_id,
                        bdev_offset: event.bdev_offset,
                        size: event.size,
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self::retry_pending_quarantine_proofs(
            &self.store,
            &self.pending_quarantine_proofs,
            new_proofs,
            self.quarantine_proof_store_err_max_retries,
            self.quarantine_proof_stuck_max_retries,
            self.quarantine_proof_stuck_warn_retries,
        );
    }

    #[cfg(not(feature = "spdk"))]
    fn process_quarantine_terminal_events(&self) {}

    /// Core retry logic, independent of the SPDK event source so it can be
    /// unit tested without a real device. Extends `pending` with
    /// `new_proofs`, then retries every pending proof once:
    ///
    /// - `Released`: drop, done. If this proof had previously been flagged
    ///   as stuck (`p.warned`), clears the `spdk_quarantine_proof_stuck`
    ///   gauge for it.
    /// - `NotYetQuarantined`: the block exists with the same identity but
    ///   hasn't transitioned to `Quarantined` yet. Expected to resolve
    ///   almost immediately; capped at `stuck_max_retries` since reaching it
    ///   means the transition itself is stuck, not that the proof is merely
    ///   stale. Separately, once this proof has been retried
    ///   `stuck_warn_retries` times (expected to be well below
    ///   `stuck_max_retries`) without yet hitting that final cap, a
    ///   one-time warning is logged and `spdk_quarantine_proof_stuck` is
    ///   raised -- purely an early, non-terminal observability signal, not
    ///   a change in retry behavior.
    /// - `Stale`: the block's identity no longer matches at all; whatever
    ///   this proof would have released has already been resolved
    ///   elsewhere. This is a settled fact the moment it's observed --
    ///   re-checking on a later tick cannot change the answer -- so it is
    ///   dropped unconditionally on first observation, with no retry/cap
    ///   needed. If this proof had previously been flagged as stuck,
    ///   clears the gauge for it.
    /// - `Err`: a transient store-level failure (e.g. lock contention), not
    ///   a definitive answer about the proof itself, so it genuinely is
    ///   worth retrying a bounded number of times before giving up. Capped
    ///   at `store_err_max_retries`. If dropped while previously flagged as
    ///   stuck, clears the gauge for it. The warning log for this case is
    ///   emitted only once per proof (see `PendingQuarantineProof::err_warned`),
    ///   not every tick, since `store_err_max_retries` defaults to
    ///   unlimited and a persistent error would otherwise flood logs.
    ///
    /// `0` for any of the three thresholds means disabled/unlimited
    /// (preserves infinite retry / no early warning).
    #[cfg_attr(not(feature = "spdk"), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn retry_pending_quarantine_proofs(
        store: &BlockStore,
        pending: &Mutex<Vec<PendingQuarantineProof>>,
        new_proofs: Vec<QuarantineReleaseProof>,
        store_err_max_retries: u32,
        stuck_max_retries: u32,
        stuck_warn_retries: u32,
    ) {
        let mut pending = match pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        pending.extend(new_proofs.into_iter().map(|proof| PendingQuarantineProof {
            proof,
            attempts: 0,
            warned: false,
            err_warned: false,
        }));
        if pending.is_empty() {
            return;
        }

        pending.retain_mut(
            |p| match store.release_quarantined_block_with_proof(&p.proof) {
                Ok(QuarantineReleaseOutcome::Released) => {
                    log::info!(
                        "released quarantined SPDK extent: block_id={} offset={} size={}",
                        p.proof.block_id,
                        p.proof.bdev_offset,
                        p.proof.size
                    );
                    if p.warned {
                        if let Ok(metrics) = Worker::get_metrics() {
                            metrics.spdk_quarantine_proof_stuck.dec();
                        }
                    }
                    false
                }
                Ok(QuarantineReleaseOutcome::NotYetQuarantined) => {
                    p.attempts += 1;
                    if !p.warned && stuck_warn_retries != 0 && p.attempts >= stuck_warn_retries {
                        p.warned = true;
                        warn!(
                            "block {} has been Writing for {} retries following an uncertain \
                             flush failure; may indicate abort_block_uncertain is stuck \
                             (offset={} size={})",
                            p.proof.block_id, p.attempts, p.proof.bdev_offset, p.proof.size
                        );
                        if let Ok(metrics) = Worker::get_metrics() {
                            metrics.spdk_quarantine_proof_stuck.inc();
                        }
                    }
                    if stuck_max_retries != 0 && p.attempts >= stuck_max_retries {
                        log::error!(
                            "block {} still Writing after {} retries following an uncertain \
                             flush failure; abort_block_uncertain may be stuck or failing, \
                             giving up on this quarantine release proof (offset={} size={})",
                            p.proof.block_id,
                            p.attempts,
                            p.proof.bdev_offset,
                            p.proof.size
                        );
                        if let Ok(metrics) = Worker::get_metrics() {
                            metrics.spdk_quarantine_proof_dropped.inc();
                            if p.warned {
                                metrics.spdk_quarantine_proof_stuck.dec();
                            }
                        }
                        false
                    } else {
                        true
                    }
                }
                Ok(QuarantineReleaseOutcome::Stale) => {
                    // Stale is a definitive, settled fact the moment it's
                    // observed (the block is gone, or exists under a
                    // different identity) -- re-checking on a later tick
                    // cannot make it "more stale." Unlike NotYetQuarantined
                    // (which genuinely needs to wait for a transition in
                    // progress) or a transient store Err, there is nothing
                    // to gain by retrying, so this drops unconditionally on
                    // first observation instead of accumulating attempts
                    // against a cap.
                    warn!(
                        "giving up on quarantine release proof for block {}: the block no \
                         longer matches (offset={} size={}); the extent it would have \
                         released was already resolved through some other path",
                        p.proof.block_id, p.proof.bdev_offset, p.proof.size
                    );
                    if let Ok(metrics) = Worker::get_metrics() {
                        metrics.spdk_quarantine_proof_dropped.inc();
                        if p.warned {
                            metrics.spdk_quarantine_proof_stuck.dec();
                        }
                    }
                    false
                }
                Err(e) => {
                    p.attempts += 1;
                    // Log only the first time this proof hits a store
                    // error, not every heartbeat tick -- store_err_max_retries
                    // defaults to unlimited, so without this guard a
                    // persistent error would flood logs indefinitely at
                    // the heartbeat interval.
                    if p.should_log_store_err() {
                        warn!(
                            "release_quarantined_block_with_proof failed for block {}: {}",
                            p.proof.block_id, e
                        );
                    }
                    if store_err_max_retries != 0 && p.attempts >= store_err_max_retries {
                        if let Ok(metrics) = Worker::get_metrics() {
                            metrics.spdk_quarantine_proof_dropped.inc();
                            if p.warned {
                                metrics.spdk_quarantine_proof_stuck.dec();
                            }
                        }
                        false
                    } else {
                        true
                    }
                }
            },
        );
    }
}

impl LoopTask for HeartbeatTask {
    type Error = FsError;

    fn run(&self) -> FsResult<()> {
        self.process_quarantine_terminal_events();

        // Perform heartbeat sending.
        let info = match self.store.get_and_check_storages() {
            Ok(info) => info,
            Err(e) => {
                error!("collect worker storage info failed {}", e);
                return Ok(());
            }
        };
        let res = self.client.heartbeat(HeartbeatStatus::Running, info);
        match res {
            Ok(v) => {
                let cmds = ProtoUtils::worker_cmd_from_pb(v.cmds);
                Self::delete_block_task(
                    self.executor.clone(),
                    self.store.clone(),
                    cmds,
                    self.report_blocks.clone(),
                    self.pending_deletes.clone(),
                );
            }

            Err(e) => {
                // Wait for the next try again.
                error!("Send heartbeat failed {}", e);
                return Ok(());
            }
        };

        // Execute block report
        let report_blocks = self.get_report_blocks();
        if report_blocks.is_empty() {
            return Ok(());
        }

        let res = self.client.incr_block_report(&report_blocks);
        match res {
            Ok(v) => {
                self.acknowledge_reports(&report_blocks);
                let cmds = ProtoUtils::worker_cmd_from_pb(v.cmds);
                Self::delete_block_task(
                    self.executor.clone(),
                    self.store.clone(),
                    cmds,
                    self.report_blocks.clone(),
                    self.pending_deletes.clone(),
                );
            }

            Err(e) => {
                error!("report blocks {}", e);
                self.put_missing_report(report_blocks)
            }
        }

        Ok(())
    }

    fn terminate(&self) -> bool {
        let state: ServerState = self.worker_ctl.state();
        state == ServerState::Stop
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingQuarantineProof, QuarantineReleaseProof};
    use crate::worker::block::BlockStore;
    use crate::worker::block::HeartbeatTask;
    use crate::worker::Worker;
    use curvine_config::{ClusterConf, WorkerConf};
    use curvine_core_error::CommonResult;
    use curvine_model::{ExtendedBlock, FileType, StorageType};
    use std::sync::Mutex;

    // `WorkerMetrics` (including `spdk_quarantine_proof_stuck`) is a
    // process-wide singleton (see `worker_metrics.rs`'s own note on this).
    // Tests that assert exact gauge deltas must not run concurrently with
    // each other, or their before/after snapshots can race against one
    // another's increments/decrements. This lock serializes only those
    // tests against each other, without affecting the rest of this file's
    // parallelism.
    static GAUGE_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn test_store(name: &str) -> CommonResult<BlockStore> {
        let _worker = Worker::with_conf(ClusterConf {
            format_worker: true,
            worker: WorkerConf {
                data_dir: vec![format!("[MEM:1KB]../testing/heartbeat-task-{name}")],
                ..WorkerConf::default()
            },
            ..ClusterConf::default()
        })?;
        let dataset = curvine_storage_local::BlockDataset::new_spdk_test_dataset(1, 1 << 30)?;
        Ok(BlockStore::from_dataset_for_test(dataset))
    }

    #[test]
    fn retry_releases_matching_proof_and_removes_from_pending() -> CommonResult<()> {
        let store = test_store("release")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        store.abort_block_uncertain(&block)?;

        let pending = Mutex::new(Vec::new());
        let new_proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };

        HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![new_proof], 0, 0, 0);

        assert!(
            pending.lock().unwrap().is_empty(),
            "a matching proof must be released and removed from the pending queue"
        );
        assert!(store.get_block(1).is_err(), "block must be fully released");
        Ok(())
    }

    #[test]
    fn retry_drops_stale_proof_immediately() -> CommonResult<()> {
        let store = test_store("stale-immediate")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        store.abort_block_uncertain(&block)?;

        // Release it once for real, so a second identical proof is genuinely
        // Stale (the block no longer exists at all).
        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(vec![PendingQuarantineProof {
            proof,
            attempts: 0,
            warned: false,
            err_warned: false,
        }]);
        HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![], 0, 0, 0);
        assert!(pending.lock().unwrap().is_empty(), "first proof releases");

        // A second, duplicate proof for the same now-gone identity: Stale
        // from the very first check. No retry cap is configured (both args
        // are 0), yet it must still be dropped on the very first attempt --
        // Stale is a settled fact the moment it's observed, not something
        // that needs a cap to eventually give up on.
        let stale_proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        HeartbeatTask::retry_pending_quarantine_proofs(
            &store,
            &pending,
            vec![stale_proof],
            0,
            0,
            0,
        );
        assert!(
            pending.lock().unwrap().is_empty(),
            "a stale proof must be dropped on first observation, with no retry needed"
        );
        Ok(())
    }

    #[test]
    fn retry_keeps_not_yet_quarantined_proof_past_the_stale_cap() -> CommonResult<()> {
        let store = test_store("not-yet-quarantined")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        // Not quarantined yet: still Writing, same identity as the proof.

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let stale_cap = 2u32;
        let stuck_cap = 10u32;

        // Retry well past the (tight) stale cap: must NOT be dropped, since
        // this proof is NotYetQuarantined, not Stale.
        for tick in 1..=(stale_cap * 3) {
            HeartbeatTask::retry_pending_quarantine_proofs(
                &store,
                &pending,
                if tick == 1 { vec![proof] } else { vec![] },
                stale_cap,
                stuck_cap,
                0,
            );
            assert!(
                !pending.lock().unwrap().is_empty(),
                "NotYetQuarantined proof must survive past the stale cap (tick {tick})"
            );
        }
        Ok(())
    }

    #[test]
    fn retry_drops_not_yet_quarantined_proof_after_stuck_cap() -> CommonResult<()> {
        let store = test_store("stuck-cap")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        // Never transitions to Quarantined in this test (simulates
        // abort_block_uncertain persistently failing/never running).

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let stuck_cap = 3u32;

        for tick in 1..stuck_cap {
            HeartbeatTask::retry_pending_quarantine_proofs(
                &store,
                &pending,
                if tick == 1 { vec![proof] } else { vec![] },
                0,
                stuck_cap,
                0,
            );
            assert!(
                !pending.lock().unwrap().is_empty(),
                "must still be retried before reaching the stuck cap (tick {tick})"
            );
        }

        HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![], 0, stuck_cap, 0);
        assert!(
            pending.lock().unwrap().is_empty(),
            "a persistently NotYetQuarantined proof must be dropped once it exceeds the stuck cap"
        );
        Ok(())
    }

    #[test]
    fn retry_warns_once_and_raises_gauge_when_crossing_warn_threshold() -> CommonResult<()> {
        let _guard = GAUGE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let store = test_store("stuck-warn")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        // Never transitions to Quarantined in this test.

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let warn_at = 2u32;
        let metrics = Worker::get_metrics()?;
        let gauge_before = metrics.spdk_quarantine_proof_stuck.get();

        // Tick 1: below the warn threshold, gauge must not move yet.
        HeartbeatTask::retry_pending_quarantine_proofs(
            &store,
            &pending,
            vec![proof],
            0,
            0,
            warn_at,
        );
        assert_eq!(
            metrics.spdk_quarantine_proof_stuck.get(),
            gauge_before,
            "gauge must not rise before the warn threshold is reached"
        );

        // Tick 2: reaches the warn threshold, gauge must rise by exactly 1.
        HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![], 0, 0, warn_at);
        assert_eq!(
            metrics.spdk_quarantine_proof_stuck.get(),
            gauge_before + 1,
            "gauge must rise by exactly 1 on the tick the warn threshold is first crossed"
        );

        // Ticks 3-5: still NotYetQuarantined, well past the warn threshold.
        // The gauge must NOT rise again -- the warning/gauge increment is
        // one-time per proof, not re-fired every subsequent tick.
        for _ in 0..3 {
            HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![], 0, 0, warn_at);
            assert_eq!(
                metrics.spdk_quarantine_proof_stuck.get(),
                gauge_before + 1,
                "gauge must stay at +1 on every subsequent tick past the warn threshold"
            );
        }
        assert!(
            !pending.lock().unwrap().is_empty(),
            "no final cap is configured, proof must still be pending"
        );
        Ok(())
    }

    #[test]
    fn retry_gauge_clears_when_warned_proof_finally_releases() -> CommonResult<()> {
        let _guard = GAUGE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let store = test_store("stuck-warn-then-release")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let warn_at = 1u32;
        let metrics = Worker::get_metrics()?;
        let gauge_before = metrics.spdk_quarantine_proof_stuck.get();

        // Crosses the warn threshold immediately (warn_at=1).
        HeartbeatTask::retry_pending_quarantine_proofs(
            &store,
            &pending,
            vec![proof],
            0,
            0,
            warn_at,
        );
        assert_eq!(metrics.spdk_quarantine_proof_stuck.get(), gauge_before + 1);

        // Now the block actually transitions to Quarantined (simulating
        // abort_block_uncertain finally succeeding), and the next retry
        // tick releases it for real.
        store.abort_block_uncertain(&block)?;
        HeartbeatTask::retry_pending_quarantine_proofs(&store, &pending, vec![], 0, 0, warn_at);

        assert!(
            pending.lock().unwrap().is_empty(),
            "proof must be released once the block is actually Quarantined"
        );
        assert_eq!(
            metrics.spdk_quarantine_proof_stuck.get(),
            gauge_before,
            "gauge must clear back down once a previously-warned proof releases"
        );
        Ok(())
    }

    #[test]
    fn retry_gauge_clears_when_warned_proof_hits_final_cap() -> CommonResult<()> {
        let _guard = GAUGE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let store = test_store("stuck-warn-then-drop")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;
        // Never transitions to Quarantined: will eventually hit stuck_cap.

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let warn_at = 1u32;
        let stuck_cap = 2u32;
        let metrics = Worker::get_metrics()?;
        let gauge_before = metrics.spdk_quarantine_proof_stuck.get();

        // Tick 1: crosses the warn threshold.
        HeartbeatTask::retry_pending_quarantine_proofs(
            &store,
            &pending,
            vec![proof],
            0,
            stuck_cap,
            warn_at,
        );
        assert_eq!(metrics.spdk_quarantine_proof_stuck.get(), gauge_before + 1);
        assert!(!pending.lock().unwrap().is_empty());

        // Tick 2: hits the final stuck cap and is dropped. The gauge must
        // be cleared in the same tick, since this proof was warned.
        HeartbeatTask::retry_pending_quarantine_proofs(
            &store,
            &pending,
            vec![],
            0,
            stuck_cap,
            warn_at,
        );
        assert!(
            pending.lock().unwrap().is_empty(),
            "proof must be dropped once it exceeds the stuck cap"
        );
        assert_eq!(
            metrics.spdk_quarantine_proof_stuck.get(),
            gauge_before,
            "gauge must clear back down once a previously-warned proof is dropped"
        );
        Ok(())
    }

    #[test]
    fn retry_warn_threshold_disabled_by_default() -> CommonResult<()> {
        let _guard = GAUGE_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let store = test_store("stuck-warn-disabled")?;
        let block = ExtendedBlock::new(1, 4096, StorageType::SpdkDisk, FileType::File);
        store.open_block(&block)?;

        let proof = QuarantineReleaseProof {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };
        let pending = Mutex::new(Vec::new());
        let metrics = Worker::get_metrics()?;
        let gauge_before = metrics.spdk_quarantine_proof_stuck.get();

        // warn_at=0 means disabled; run several ticks, gauge must never move.
        for tick in 0..10 {
            HeartbeatTask::retry_pending_quarantine_proofs(
                &store,
                &pending,
                if tick == 0 { vec![proof] } else { vec![] },
                0,
                0,
                0,
            );
            assert_eq!(
                metrics.spdk_quarantine_proof_stuck.get(),
                gauge_before,
                "warn_at=0 must never raise the gauge (tick {tick})"
            );
        }
        Ok(())
    }

    #[test]
    fn should_log_store_err_fires_exactly_once_per_proof() {
        let mut p = PendingQuarantineProof {
            proof: QuarantineReleaseProof {
                block_id: 1,
                bdev_offset: 0,
                size: 4096,
            },
            attempts: 0,
            warned: false,
            err_warned: false,
        };

        assert!(
            p.should_log_store_err(),
            "must log on the first occurrence of a store error"
        );
        for i in 0..20 {
            assert!(
                !p.should_log_store_err(),
                "must not log again on subsequent occurrences (iteration {i})"
            );
        }
    }
}
