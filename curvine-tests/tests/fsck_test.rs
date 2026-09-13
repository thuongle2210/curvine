use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Writer};
use curvine_model::{BlockReportInfo, BlockReportList, BlockReportStatus, StorageType};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn requested_mem_and_reported_disk_mismatch_survives_rpc() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let master_fs = cluster.get_active_master_fs();
    let cluster_id = cluster.cluster_conf.cluster_id.clone();

    rt.block_on(async move {
        let path = Path::from_str("/fsck/storage-fallback.data")?;
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

        let block = &located.block_locs[0];
        // CompleteFile initially persists the requested type. Apply the worker's
        // finalized-block report, whose type comes from the selected DISK dir.
        master_fs.block_report(
            BlockReportList {
                cluster_id,
                worker_id: block.locs[0].worker_id,
                full_report: false,
                total_len: block.block.len as u64,
                blocks: vec![BlockReportInfo::new(
                    block.block.id,
                    BlockReportStatus::Finalized,
                    StorageType::Disk,
                    block.block.len,
                )],
            },
            None,
        )?;

        let details = fs.fs_client().get_file_block_details(&path).await?;
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
    })
}
