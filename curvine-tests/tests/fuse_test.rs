//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#[cfg(target_os = "linux")]
#[test]
fn persist_restore() {
    use std::sync::Arc;

    use curvine_common::fs::{Path, StateReader, StateWriter};
    use curvine_common::state::{CreateFileOptsBuilder, FileStatus, OpenFlags};
    use curvine_fuse::fs::state::NodeState;
    use curvine_fuse::FUSE_ROOT_ID;
    use curvine_tests::Testing;
    use orpc::common::Utils;
    use orpc::runtime::AsyncRuntime;
    use orpc::runtime::RpcRuntime;

    let rt = Arc::new(AsyncRuntime::single());
    let testing: Testing = Testing::default();
    let rt1 = rt.clone();

    rt1.block_on(async move {
        let test_path = Utils::test_file();

        // Create original NodeState and add data
        let fs1 = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state1 = NodeState::new(fs1.clone());

        // Add some nodes
        let status_a = FileStatus::with_name(2, "a".to_string(), true);
        let status_b = FileStatus::with_name(3, "b".to_string(), true);
        let status_c = FileStatus::with_name(4, "c".to_string(), true);
        let a = state1
            .do_lookup(FUSE_ROOT_ID, Some("a"), &status_a)
            .unwrap();
        let b = state1.do_lookup(a.ino, Some("b"), &status_b).unwrap();
        let c = state1.do_lookup(b.ino, Some("c"), &status_c).unwrap();

        // Create dir_handles
        let dir_status_list = vec![
            FileStatus::with_name(10, "file1".to_string(), false),
            FileStatus::with_name(11, "file2".to_string(), false),
        ];
        let dir_handle1 = state1
            .new_dir_handle(a.ino, dir_status_list.clone())
            .await
            .unwrap();
        let dir_handle2 = state1
            .new_dir_handle(b.ino, dir_status_list.clone())
            .await
            .unwrap();

        // Create file handles
        let path = Path::from_str("/a/1.log").unwrap();
        let handle1 = state1
            .new_handle(
                21,
                &path,
                OpenFlags::new_create().value(),
                CreateFileOptsBuilder::with_conf(state1.client_conf())
                    .create_parent(true)
                    .build(),
            )
            .await
            .unwrap();

        let path = Path::from_str("/a/2.log").unwrap();
        let handle2 = state1
            .new_handle(
                22,
                &path,
                OpenFlags::new_create().set_read_write().value(),
                CreateFileOptsBuilder::with_conf(state1.client_conf())
                    .create_parent(true)
                    .build(),
            )
            .await
            .unwrap();

        // Add locks to handle1 for testing
        handle1.add_lock(curvine_common::state::LockFlags::Flock, 100);
        handle1.add_lock(curvine_common::state::LockFlags::Plock, 200);

        // Record original state for comparison
        let original_node_count = state1.node_read().nodes_len();
        let original_id_creator = state1.node_read().current_id();
        let original_fh_creator = state1.current_fh();
        let original_handle1_status = handle1.status().clone();
        let _original_dir_handle1_list = dir_handle1.get_all().to_vec();

        // Persist state
        let mut writer = StateWriter::new(&test_path).unwrap();
        state1.persist(&mut writer).await.unwrap();
        drop(writer);

        // Create new NodeState and restore
        let fs2 = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state2 = NodeState::new(fs2);

        let mut reader = StateReader::new(&test_path).unwrap();
        state2.restore(&mut reader).await.unwrap();

        // Verify node paths
        let path_a = state2.get_path(a.ino).unwrap();
        assert_eq!(path_a.path(), "/a");

        let path_b = state2.get_path(b.ino).unwrap();
        assert_eq!(path_b.path(), "/a/b");

        let path_c = state2.get_path(c.ino).unwrap();
        assert_eq!(path_c.path(), "/a/b/c");

        // Verify node lookup
        let found_a = state2.find_node(FUSE_ROOT_ID, Some("a")).unwrap();
        assert_eq!(found_a.id, a.ino);

        let found_b = state2.find_node(a.ino, Some("b")).unwrap();
        assert_eq!(found_b.id, b.ino);

        let found_c = state2.find_node(b.ino, Some("c")).unwrap();
        assert_eq!(found_c.id, c.ino);

        // Verify handle restoration
        assert_eq!(state2.all_handles().len(), state1.all_handles().len());
        assert!(state2.find_handle(handle1.ino, handle1.fh).is_ok());
        assert!(state2.find_handle(handle2.ino, handle2.fh).is_ok());

        // Verify dir_handle restoration
        assert_eq!(
            state2.all_dir_handles().len(),
            state1.all_dir_handles().len()
        );
        let restored_dir_handle1 = state2
            .find_dir_handle(dir_handle1.ino, dir_handle1.fh)
            .unwrap();
        assert_eq!(restored_dir_handle1.ino, dir_handle1.ino);
        assert_eq!(restored_dir_handle1.fh, dir_handle1.fh);
        assert_eq!(restored_dir_handle1.len(), dir_handle1.len());

        let restored_dir_handle2 = state2
            .find_dir_handle(dir_handle2.ino, dir_handle2.fh)
            .unwrap();
        assert_eq!(restored_dir_handle2.ino, dir_handle2.ino);
        assert_eq!(restored_dir_handle2.fh, dir_handle2.fh);

        // Verify file handle details
        let restored_handle1 = state2.find_handle(handle1.ino, handle1.fh).unwrap();
        assert_eq!(restored_handle1.ino, handle1.ino);
        assert_eq!(restored_handle1.fh, handle1.fh);
        assert_eq!(restored_handle1.status().path, original_handle1_status.path);
        assert_eq!(restored_handle1.status().id, original_handle1_status.id);

        // Verify locks are restored (check that locks exist by trying to remove them)
        let flock_owner = restored_handle1.remove_lock(curvine_common::state::LockFlags::Flock);
        assert_eq!(flock_owner, Some(100));
        let plock_owner = restored_handle1.remove_lock(curvine_common::state::LockFlags::Plock);
        assert_eq!(plock_owner, Some(200));

        // Verify counters
        let restored_node_count = state2.node_read().nodes_len();
        let restored_id_creator = state2.node_read().current_id();
        let restored_fh_creator = state2.current_fh();
        assert_eq!(restored_node_count, original_node_count);
        assert_eq!(restored_id_creator, original_id_creator);
        assert_eq!(restored_fh_creator, original_fh_creator);

        // Clean up test file
        let _ = std::fs::remove_file(&test_path);
    });
}

#[cfg(target_os = "linux")]
#[test]
fn test_fuse_read_truncate_bug() {
    use curvine_common::fs::Path;
    use curvine_common::fs::Writer;
    use curvine_common::state::{CreateFileOptsBuilder, OpenFlags};
    use curvine_fuse::fs::operator::Read;
    use curvine_fuse::fs::state::NodeState;
    use curvine_fuse::raw::fuse_abi::{fuse_in_header, fuse_read_in};
    use curvine_fuse::session::FuseResponse;
    use curvine_tests::Testing;
    use orpc::runtime::AsyncRuntime;
    use orpc::runtime::RpcRuntime;
    use orpc::sync::channel::AsyncChannel;
    use orpc::sys::DataSlice;
    use std::sync::Arc;

    let testing = Testing::default();
    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();
    rt1.block_on(async move {
        let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state = NodeState::new(fs.clone());

        let path = Path::from_str("/fuse_truncate_bug/file.bin").unwrap();

        // --- Setup: write a 2 MiB file ---
        let original_size: usize = 2 * 1024 * 1024;
        let original_data: Vec<u8> = (0..original_size).map(|i| (i % 251) as u8).collect();

        {
            let raw_fs = testing.get_fs(None, None).unwrap();
            let mut writer = raw_fs.create(&path, true).await.unwrap();
            writer
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from(original_data)))
                .await
                .unwrap();
            writer.complete().await.unwrap();
        }

        // --- Client A: open a FUSE FileHandle (read-only) ---
        let ino = 100u64; // synthetic inode id for the test
        let open_flags = OpenFlags::new_read_only().value();
        let opts = CreateFileOptsBuilder::with_conf(state.client_conf()).build();
        let handle_a = state
            .new_handle(ino, &path, open_flags, opts)
            .await
            .unwrap();

        // Confirm reader sees original length
        let reader_len_before = {
            let reader = handle_a.reader.as_ref().unwrap();
            reader.len()
        };
        assert_eq!(
            reader_len_before, original_size as i64,
            "reader should see original 2 MiB"
        );

        // --- Client B: truncate the file (overwrite with 256 KiB) ---
        let truncated_size: usize = 256 * 1024;
        {
            let raw_fs = testing.get_fs(None, None).unwrap();
            let mut writer_b = raw_fs.create(&path, true).await.unwrap();
            writer_b
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from(vec![
                    0xABu8;
                    truncated_size
                ])))
                .await
                .unwrap();
            writer_b.complete().await.unwrap();
        }

        // --- Simulate FUSE read at offset == original_size - 1 chunk ---
        // This simulates client A reading the last chunk of what it thinks is a 2 MiB file.
        // The offset is past the NEW file length (truncated_size), but handle_a's FuseReader
        // still has len == original_size, so FileHandle::read won't refresh it.
        let read_offset = truncated_size as u64; // offset is past truncated boundary but < original

        let header = fuse_in_header {
            len: 0,
            opcode: 0,
            unique: 1,
            nodeid: ino,
            uid: 0,
            gid: 0,
            pid: 0,
            padding: 0,
        };
        let arg = fuse_read_in {
            fh: handle_a.fh,
            offset: read_offset,
            size: 65536,
            read_flags: 0,
            lock_owner: 0,
            flags: 0,
            padding: 0,
        };
        let op = Read {
            header: &header,
            arg: &arg,
        };

        // Create a FuseResponse channel to capture the reply
        let (tx, mut rx) = AsyncChannel::new(1).split();
        let reply = FuseResponse::new(1, tx, false);

        // BUG: FileHandle::read will NOT refresh the reader because:
        //   1. read_offset (256 KiB) < reader.len() (2 MiB) — condition not triggered
        //   2. Even if it were triggered, state.find_writer(ino) returns None (no local writer)
        // So it reads from stale FuseReader which tries to fetch block data
        // at an offset that may no longer exist (blocks were deleted by truncation).
        let result = handle_a.read(&state, op, reply).await;

        // The reader still thinks the file is 2 MiB, so it attempts to read
        // from offset 256 KiB to 2 MiB using stale block locations.
        // After the fix: FuseReader.len must be refreshed to truncated_size,
        // so reading at offset == truncated_size returns EOF (0 bytes).
        println!("read result: {:?}", result);

        // Key assertion demonstrating the bug:
        // reader.len() on handle_a is still the pre-truncation value
        let reader_len_after = {
            let reader = handle_a.reader.as_ref().unwrap();
            reader.len()
        };

        // BUG: This assertion FAILS because FuseReader.len was never updated.
        // Without the fix: reader_len_after == 2 MiB (stale)
        // With the fix:    reader_len_after == 256 KiB (refreshed from master)
        assert_eq!(
            reader_len_after, truncated_size as i64,
            "BUG: FuseReader.len is stale ({} bytes) after concurrent truncation to {} bytes",
            reader_len_after, truncated_size
        );

        handle_a.complete(None).await.unwrap();
    });
}

#[cfg(target_os = "linux")]
#[test]
fn test_fuse_read_truncate_bug_rdwr() {
    use curvine_common::fs::Path;
    use curvine_common::fs::Writer;
    use curvine_common::state::{CreateFileOptsBuilder, OpenFlags};
    use curvine_fuse::fs::operator::Read;
    use curvine_fuse::fs::state::NodeState;
    use curvine_fuse::raw::fuse_abi::{fuse_in_header, fuse_read_in};
    use curvine_fuse::session::FuseResponse;
    use curvine_tests::Testing;
    use orpc::runtime::AsyncRuntime;
    use orpc::runtime::RpcRuntime;
    use orpc::sync::channel::AsyncChannel;
    use orpc::sys::DataSlice;
    use std::sync::Arc;
    let testing = Testing::default();
    let rt = Arc::new(AsyncRuntime::single());
    let rt1 = rt.clone();

    rt1.block_on(async move {
        let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
        let state = NodeState::new(fs.clone());

        let path = Path::from_str("/fuse_rdwr_truncate_bug/file.bin").unwrap();
        let original_size: i64 = 2 * 1024 * 1024; // 2 MiB
        let truncated_size: i64 = 256 * 1024; // 256 KiB

        // --- Setup: write a 2 MiB file via raw fs ---
        {
            let raw_fs = testing.get_fs(None, None).unwrap();
            let data: Vec<u8> = (0..original_size as usize)
                .map(|i| (i % 251) as u8)
                .collect();
            let mut writer = raw_fs.create(&path, true).await.unwrap();
            writer
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from(data)))
                .await
                .unwrap();
            writer.complete().await.unwrap();
        }

        // --- Client A: open with O_RDWR ---
        // O_RDWR causes new_handle to create BOTH a writer and a reader on the same inode.
        // This is the critical precondition: state.find_writer(nodeid) will return Some.
        let ino_a: u64 = 100;
        let handle_a = state
            .new_handle(
                ino_a,
                &path,
                OpenFlags::new_read_write().value(),
                CreateFileOptsBuilder::with_conf(state.client_conf()).build(),
            )
            .await
            .unwrap();

        // Verify: reader exists and has original length
        let len_before = handle_a.reader.as_ref().unwrap().len();
        assert_eq!(
            len_before, original_size,
            "pre-condition: reader must see 2 MiB"
        );

        // --- Client B: truncate to 256 KiB ---
        {
            let raw_fs2 = testing.get_fs(None, None).unwrap();
            let short_data: Vec<u8> = vec![0xAB; truncated_size as usize];
            let mut writer_b = raw_fs2.create(&path, true).await.unwrap();
            writer_b
                .write_chunk(DataSlice::Bytes(bytes::Bytes::from(short_data)))
                .await
                .unwrap();
            writer_b.complete().await.unwrap();
        }

        // --- Simulate FileHandle::read at offset >= truncated_size ---
        // This triggers the condition: op.arg.offset >= reader.len() is FALSE still (2MiB reader),
        // but we need to drain the reader to EOF first then issue a read past new EOF.
        // Instead: directly call FileHandle::read at offset = original_size - 1 (within old range)
        // after the master has updated. The bug: find_writer returns Some, so new_reader() is called,
        // replacing the reader with one bounded to 256 KiB.
        //
        // Simulate a read at offset == truncated_size (past new EOF, within old EOF):
        let header = fuse_in_header {
            nodeid: ino_a,
            ..Default::default()
        };
        let arg = fuse_read_in {
            fh: handle_a.fh,
            offset: truncated_size as u64, // past new EOF, but within old 2MiB range
            size: 4096,
            ..Default::default()
        };
        let op = Read {
            header: &header,
            arg: &arg,
        };

        let (tx, _rx) = AsyncChannel::new(1).split();
        let reply = FuseResponse::new(1, tx, false);
        // This call will trigger the buggy path:
        //   op.arg.offset (256KiB) >= reader.len() (2MiB)? NO — so bug not triggered here.
        // We need offset >= reader.len() to fire. Use offset = original_size:
        let arg2 = fuse_read_in {
            fh: handle_a.fh,
            offset: original_size as u64, // at exactly old EOF
            size: 4096,
            ..Default::default()
        };
        let op2 = Read {
            header: &header,
            arg: &arg2,
        };
        let (tx2, _rx2) = AsyncChannel::new(1).split();
        let reply2 = FuseResponse::new(2, tx2, false);

        // BUG: because find_writer(ino_a) returns Some (O_RDWR handle),
        // new_reader() is called here, replacing reader with post-truncation snapshot (256 KiB).
        let _ = handle_a.read(&state, op2, reply2).await;

        // After the buggy refresh: reader.len() is now 256 KiB instead of 2 MiB.
        let len_after = handle_a.reader.as_ref().unwrap().len();

        // BUG ASSERTION: client A's reader was replaced mid-read.
        // Expected (correct): len_after == original_size (2 MiB — stable snapshot)
        // Actual (buggy):     len_after == truncated_size (256 KiB — replaced by new_reader)
        assert_eq!(
            len_after, original_size,
            "BUG: FileHandle::read replaced client A's reader during concurrent truncation  
             Reader len changed from {} to {} (truncated_size), meaning client A will now 
             read only {} bytes instead of the original {} bytes.",
            original_size, len_after, len_after, original_size
        );

        handle_a.complete(None).await.unwrap();
    });
}
