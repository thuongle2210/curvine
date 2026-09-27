//! RocksDB-backed SPDK block metadata.
use crate::meta_store::BlockMetaStore;
use crate::{BlockMeta, BlockState};
/// Legacy key: block_id (8B). Value: dir_id(4B) | offset(8B) | size(8B) | len(8B) | finalized(1B) | state(1B).
/// Generation key: 0xff | block_id (8B) | generation (8B). Same value shape plus generation in record.
/// Older 29-byte records without state decode from finalized.
/// O(1) per block
use byteorder::{BigEndian, ByteOrder};
use curvine_core_error::{err_box, CommonResult};
use curvine_rocksdb::{DBConf, DBEngine};
use log::{info, warn};
const CF_SPDK_BLOCKS: &str = "spdk_blocks";
const VALUE_SIZE: usize = 30;
const LEGACY_VALUE_SIZE: usize = 29;
const GENERATION_KEY_PREFIX: u8 = 0xff;
pub struct SpdkMetaStore {
    db: DBEngine,
}
#[derive(Debug, Clone)]
pub struct SpdkBlockRecord {
    pub block_id: i64,
    pub generation: i64,
    pub dir_id: u32,
    pub offset: i64,
    pub size: i64,
    pub len: i64,
    pub finalized: bool,
    pub state: BlockState,
}
impl SpdkMetaStore {
    pub fn open(dir: &str, format: bool) -> CommonResult<Self> {
        let conf = DBConf::new(dir).add_cf(CF_SPDK_BLOCKS);
        let db = DBEngine::new(conf, format)?;
        info!("SpdkMetaStore opened at {}", dir);
        Ok(Self { db })
    }
    pub fn put(
        &self,
        block_id: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        finalized: bool,
    ) -> CommonResult<()> {
        let state = if finalized {
            BlockState::Finalized
        } else {
            BlockState::Recovering
        };
        self.put_with_state(block_id, dir_id, offset, size, len, state)
    }

    pub fn put_with_state(
        &self,
        block_id: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        state: BlockState,
    ) -> CommonResult<()> {
        let key = Self::encode_key(block_id);
        let value = Self::encode_value(dir_id, offset, size, len, state);
        self.db.put_cf(CF_SPDK_BLOCKS, key, value)
    }

    pub fn put_generation(
        &self,
        block_id: i64,
        generation: i64,
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        state: BlockState,
    ) -> CommonResult<()> {
        let key = Self::encode_generation_key(block_id, generation);
        let value = Self::encode_value(dir_id, offset, size, len, state);
        self.db.put_cf(CF_SPDK_BLOCKS, key, value)
    }

    pub fn delete_generation(&self, block_id: i64, generation: i64) -> CommonResult<()> {
        let key = Self::encode_generation_key(block_id, generation);
        self.db.delete_cf(CF_SPDK_BLOCKS, key)
    }

    pub fn get_generation(
        &self,
        block_id: i64,
        generation: i64,
    ) -> CommonResult<Option<SpdkBlockRecord>> {
        let key = Self::encode_generation_key(block_id, generation);
        match self.db.get_cf(CF_SPDK_BLOCKS, key)? {
            None => Ok(None),
            Some(v) => Ok(Some(Self::decode_value(block_id, generation, &v)?)),
        }
    }

    pub fn delete(&self, block_id: i64) -> CommonResult<()> {
        let key = Self::encode_key(block_id);
        self.db.delete_cf(CF_SPDK_BLOCKS, key)
    }
    pub fn get(&self, block_id: i64) -> CommonResult<Option<SpdkBlockRecord>> {
        let key = Self::encode_key(block_id);
        match self.db.get_cf(CF_SPDK_BLOCKS, key)? {
            None => Ok(None),
            Some(v) => {
                let rec = Self::decode_value(block_id, 0, &v)?;
                Ok(Some(rec))
            }
        }
    }
    pub fn scan_all(&self) -> CommonResult<Vec<SpdkBlockRecord>> {
        let iter = self.db.scan(CF_SPDK_BLOCKS)?;
        let mut records = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) = match item {
                Ok(kv) => kv,
                Err(e) => return err_box!("RocksDB scan error: {}", e),
            };
            if key_bytes.first().copied() == Some(GENERATION_KEY_PREFIX) {
                continue;
            }
            if key_bytes.len() < 8 {
                warn!(
                    "SpdkMetaStore: skipping short key ({} bytes)",
                    key_bytes.len()
                );
                continue;
            }
            let block_id = BigEndian::read_i64(&key_bytes);
            match Self::decode_value(block_id, 0, &val_bytes) {
                Ok(rec) => records.push(rec),
                Err(e) => {
                    warn!(
                        "SpdkMetaStore: skipping corrupt record for block {}: {}",
                        block_id, e
                    );
                }
            }
        }
        Ok(records)
    }

    pub fn scan_generations(&self) -> CommonResult<Vec<SpdkBlockRecord>> {
        let iter = self.db.scan(CF_SPDK_BLOCKS)?;
        let mut records = Vec::new();
        for item in iter {
            let (key_bytes, val_bytes) = match item {
                Ok(kv) => kv,
                Err(e) => return err_box!("RocksDB scan error: {}", e),
            };
            let Some((block_id, generation)) = Self::decode_generation_key(&key_bytes) else {
                continue;
            };
            match Self::decode_value(block_id, generation, &val_bytes) {
                Ok(rec) => records.push(rec),
                Err(e) => {
                    warn!(
                        "SpdkMetaStore: skipping corrupt generation record for block {} generation {}: {}",
                        block_id, generation, e
                    );
                }
            }
        }
        Ok(records)
    }

    #[inline]
    fn encode_key(block_id: i64) -> [u8; 8] {
        let mut buf = [0u8; 8];
        BigEndian::write_i64(&mut buf, block_id);
        buf
    }

    #[inline]
    fn encode_generation_key(block_id: i64, generation: i64) -> [u8; 17] {
        let mut buf = [0u8; 17];
        buf[0] = GENERATION_KEY_PREFIX;
        BigEndian::write_i64(&mut buf[1..9], block_id);
        BigEndian::write_i64(&mut buf[9..17], generation);
        buf
    }

    #[inline]
    fn decode_generation_key(bytes: &[u8]) -> Option<(i64, i64)> {
        if bytes.len() != 17 || bytes[0] != GENERATION_KEY_PREFIX {
            return None;
        }
        Some((
            BigEndian::read_i64(&bytes[1..9]),
            BigEndian::read_i64(&bytes[9..17]),
        ))
    }

    #[inline]
    fn encode_value(
        dir_id: u32,
        offset: i64,
        size: i64,
        len: i64,
        state: BlockState,
    ) -> [u8; VALUE_SIZE] {
        let mut buf = [0u8; VALUE_SIZE];
        BigEndian::write_u32(&mut buf[0..4], dir_id);
        BigEndian::write_i64(&mut buf[4..12], offset);
        BigEndian::write_i64(&mut buf[12..20], size);
        BigEndian::write_i64(&mut buf[20..28], len);
        buf[28] = if state == BlockState::Finalized { 1 } else { 0 };
        buf[29] = state as u8;
        buf
    }

    fn decode_state(finalized: bool, bytes: &[u8]) -> CommonResult<BlockState> {
        if bytes.len() <= LEGACY_VALUE_SIZE {
            return Ok(if finalized {
                BlockState::Finalized
            } else {
                BlockState::Recovering
            });
        }
        match bytes[29] {
            0 => Ok(BlockState::Finalized),
            1 => Ok(BlockState::Writing),
            2 => Ok(BlockState::Recovering),
            3 => Ok(BlockState::Allocating),
            4 => Ok(BlockState::Finalizing),
            5 => Ok(BlockState::Quarantined),
            6 => Ok(BlockState::Retired),
            value => err_box!("SpdkMetaStore: unknown block state byte {}", value),
        }
    }

    #[inline]
    fn decode_value(block_id: i64, generation: i64, bytes: &[u8]) -> CommonResult<SpdkBlockRecord> {
        if bytes.len() < LEGACY_VALUE_SIZE {
            return err_box!(
                "SpdkMetaStore: value too short for block {} ({} < {})",
                block_id,
                bytes.len(),
                LEGACY_VALUE_SIZE
            );
        }
        let finalized = bytes[28] != 0;
        let state = Self::decode_state(finalized, bytes)?;
        Ok(SpdkBlockRecord {
            block_id,
            generation,
            dir_id: BigEndian::read_u32(&bytes[0..4]),
            offset: BigEndian::read_i64(&bytes[4..12]),
            size: BigEndian::read_i64(&bytes[12..20]),
            len: BigEndian::read_i64(&bytes[20..28]),
            finalized,
            state,
        })
    }
}

impl BlockMetaStore for SpdkMetaStore {
    fn put_block_meta(&self, meta: &BlockMeta) -> CommonResult<()> {
        self.put_with_state(
            meta.id(),
            meta.dir_id(),
            meta.bdev_offset,
            meta.actual_len,
            meta.len(),
            *meta.state(),
        )
    }

    fn remove_block_meta(&self, meta: &BlockMeta) -> CommonResult<()> {
        self.delete(meta.id())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    fn test_dir(name: &str) -> String {
        let d = format!("../testing/spdk_meta_{}", name);
        let _ = std::fs::remove_dir_all(&d);
        d
    }
    #[test]
    fn put_get_delete() {
        let store = SpdkMetaStore::open(&test_dir("pgd"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, true).unwrap();
        store.put(2, 1, 4096, 8192, 6000, false).unwrap();
        let r = store.get(1).unwrap().unwrap();
        assert_eq!(r.offset, 0);
        assert_eq!(r.generation, 0);
        assert!(r.finalized);
        assert_eq!(r.state, BlockState::Finalized);
        store.delete(1).unwrap();
        assert!(store.get(1).unwrap().is_none());
    }
    #[test]
    fn scan_all() {
        let store = SpdkMetaStore::open(&test_dir("scan"), true).unwrap();
        for i in 0..100 {
            store
                .put(i, (i % 3) as u32, i * 4096, 4096, 4096, i % 2 == 0)
                .unwrap();
        }
        assert_eq!(store.scan_all().unwrap().len(), 100);
    }
    #[test]
    fn dir_id_preserved() {
        let store = SpdkMetaStore::open(&test_dir("dir"), true).unwrap();
        store.put(1, 10, 0, 4096, 4096, true).unwrap();
        store.put(2, 20, 4096, 4096, 4096, false).unwrap();
        let records = store.scan_all().unwrap();
        assert_eq!(records.iter().filter(|r| r.dir_id == 10).count(), 1);
        assert_eq!(records.iter().filter(|r| r.dir_id == 20).count(), 1);
    }
    #[test]
    fn update_key() {
        let store = SpdkMetaStore::open(&test_dir("upd"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, false).unwrap();
        store.put(1, 1, 0, 4096, 2048, true).unwrap();
        assert_eq!(store.get(1).unwrap().unwrap().len, 2048);
    }
    #[test]
    fn reopen() {
        let dir = test_dir("reopen");
        {
            let s = SpdkMetaStore::open(&dir, true).unwrap();
            s.put(1, 5, 0, 4096, 4096, true).unwrap();
        }
        let s = SpdkMetaStore::open(&dir, false).unwrap();
        assert_eq!(s.scan_all().unwrap().len(), 1);
    }

    #[test]
    fn quarantined_state_roundtrip() {
        let store = SpdkMetaStore::open(&test_dir("quarantine"), true).unwrap();
        store
            .put_with_state(1, 1, 0, 4096, 4096, BlockState::Quarantined)
            .unwrap();
        let record = store.get(1).unwrap().unwrap();
        assert!(!record.finalized);
        assert_eq!(record.state, BlockState::Quarantined);
    }

    #[test]
    fn retired_state_roundtrip() {
        let store = SpdkMetaStore::open(&test_dir("retired"), true).unwrap();
        store
            .put_with_state(1, 1, 0, 4096, 4096, BlockState::Retired)
            .unwrap();
        let record = store.get(1).unwrap().unwrap();
        assert_eq!(record.state, BlockState::Retired);
    }

    #[test]
    fn generation_records_allow_multiple_extents_per_block() {
        let store = SpdkMetaStore::open(&test_dir("generations"), true).unwrap();
        store
            .put_generation(1, 7, 1, 0, 4096, 4096, BlockState::Finalized)
            .unwrap();
        store
            .put_generation(1, 8, 1, 4096, 4096, 4096, BlockState::Writing)
            .unwrap();

        let gen7 = store.get_generation(1, 7).unwrap().unwrap();
        let gen8 = store.get_generation(1, 8).unwrap().unwrap();
        assert_eq!(gen7.block_id, 1);
        assert_eq!(gen7.generation, 7);
        assert_eq!(gen7.offset, 0);
        assert_eq!(gen7.state, BlockState::Finalized);
        assert_eq!(gen8.generation, 8);
        assert_eq!(gen8.offset, 4096);
        assert_eq!(gen8.state, BlockState::Writing);
    }

    #[test]
    fn generation_scan_is_separate_from_legacy_scan() {
        let store = SpdkMetaStore::open(&test_dir("generation_scan"), true).unwrap();
        store.put(1, 1, 0, 4096, 4096, true).unwrap();
        store
            .put_generation(1, 2, 1, 4096, 4096, 4096, BlockState::Writing)
            .unwrap();

        let legacy = store.scan_all().unwrap();
        let generations = store.scan_generations().unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].generation, 0);
        assert_eq!(generations.len(), 1);
        assert_eq!(generations[0].block_id, 1);
        assert_eq!(generations[0].generation, 2);
    }

    #[test]
    fn delete_generation_removes_only_that_generation() {
        let store = SpdkMetaStore::open(&test_dir("delete_generation"), true).unwrap();
        store
            .put_generation(1, 7, 1, 0, 4096, 4096, BlockState::Finalized)
            .unwrap();
        store
            .put_generation(1, 8, 1, 4096, 4096, 4096, BlockState::Writing)
            .unwrap();

        store.delete_generation(1, 7).unwrap();
        assert!(store.get_generation(1, 7).unwrap().is_none());
        assert!(store.get_generation(1, 8).unwrap().is_some());
    }
}
