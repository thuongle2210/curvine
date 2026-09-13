use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Writer};
use curvine_model::StorageType;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn requested_mem_and_reported_disk_mismatch_survives_rpc() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf.clone()))?;
    let mut remote_conf = conf;
    remote_conf.client.short_circuit = false;
    remote_conf.client.storage_type = StorageType::Mem;
    remote_conf.client.storage_type_str = "mem".to_string();
    let remote_fs = testing.get_fs(Some(rt.clone()), Some(remote_conf))?;
    let mut local_batch_conf = fs.conf().clone();
    local_batch_conf.client.storage_type = StorageType::Mem;
    local_batch_conf.client.storage_type_str = "mem".to_string();
    let local_batch_fs = testing.get_fs(Some(rt.clone()), Some(local_batch_conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/fsck/local-storage-fallback.data")?;
        let opts = fs
            .create_opts_builder()
            .create_parent(true)
            .storage_type(StorageType::Mem)
            .build();
        let mut writer = fs.create_with_opts(&path, opts, false).await?;
        writer.write(b"actual-storage-fallback").await?;
        writer.complete().await?;

        let located = fs.get_block_locations(&path).await?;
        assert_eq!(located.status.storage_policy.storage_type, StorageType::Mem);
        assert_eq!(located.block_locs.len(), 1);
        assert_eq!(
            located.block_locs[0].block.storage_type,
            StorageType::Mem,
            "normal block locations expose the requested storage type"
        );

        assert_mem_policy_disk_placement(&fs, &path).await?;

        let remote_path = Path::from_str("/fsck/remote-storage-fallback.data")?;
        let remote_opts = remote_fs
            .create_opts_builder()
            .create_parent(true)
            .storage_type(StorageType::Mem)
            .build();
        let mut remote_writer = remote_fs
            .create_with_opts(&remote_path, remote_opts, false)
            .await?;
        remote_writer.write(b"remote-storage-fallback").await?;
        remote_writer.complete().await?;
        assert_mem_policy_disk_placement(&remote_fs, &remote_path).await?;

        let batch_paths = [
            Path::from_str("/fsck/batch-storage-fallback-1.data")?,
            Path::from_str("/fsck/batch-storage-fallback-2.data")?,
        ];
        remote_fs
            .write_batch_string(&[
                (batch_paths[0].clone(), "batch-storage-fallback-1"),
                (batch_paths[1].clone(), "batch-storage-fallback-2"),
            ])
            .await?;
        for batch_path in &batch_paths {
            assert_mem_policy_disk_placement(&remote_fs, batch_path).await?;
        }

        let local_batch_paths = [
            Path::from_str("/fsck/local-batch-storage-fallback-1.data")?,
            Path::from_str("/fsck/local-batch-storage-fallback-2.data")?,
        ];
        local_batch_fs
            .write_batch_string(&[
                (
                    local_batch_paths[0].clone(),
                    "local-batch-storage-fallback-1",
                ),
                (
                    local_batch_paths[1].clone(),
                    "local-batch-storage-fallback-2",
                ),
            ])
            .await?;
        for batch_path in &local_batch_paths {
            assert_mem_policy_disk_placement(&local_batch_fs, batch_path).await?;
        }

        Ok(())
    })
}

async fn assert_mem_policy_disk_placement(
    fs: &curvine_client::file::CurvineFileSystem,
    path: &Path,
) -> Result<(), curvine_error::FsError> {
    let details = fs.fs_client().get_file_block_details(path).await?;
    assert_eq!(details.status.storage_policy.storage_type, StorageType::Mem);
    assert_eq!(details.blocks.len(), 1);
    assert_eq!(details.blocks[0].replicas.len(), 1);
    assert_eq!(
        details.blocks[0].replicas[0].storage_type,
        StorageType::Disk
    );
    assert_ne!(
        details.status.storage_policy.storage_type,
        details.blocks[0].replicas[0].storage_type
    );
    assert!(details.blocks[0].replicas[0].address.is_some());
    Ok(())
}
