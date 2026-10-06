use curvine_cli::cmds::FsckCommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn fsck_style_traversal_pages_nested_directories_without_duplicates() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.block_size = 64 * 1024;
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
        })
        .build()?;
    let _cluster = testing.start_cluster()?;
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        for path in [
            "/fsck-pages/below/file-1",
            "/fsck-pages/below/file-2",
            "/fsck-pages/exact/file-1",
            "/fsck-pages/exact/file-2",
            "/fsck-pages/exact/file-3",
            "/fsck-pages/over/file-1",
            "/fsck-pages/over/file-2",
            "/fsck-pages/over/file-3",
            "/fsck-pages/over/file-4",
            "/fsck-pages/over/nested/file-5",
        ] {
            fs.write_string(&Path::from_str(path)?, path).await?;
        }

        for (root, expected) in [
            ("/fsck-pages/below", 2),
            ("/fsck-pages/exact", 3),
            ("/fsck-pages/over", 5),
        ] {
            let command = FsckCommand {
                path: root.to_string(),
                detail: true,
                list_page_size: 3,
            };
            let output = command.render(fs.fs_client()).await?;

            assert!(output.contains(&format!("Files: {expected} | Blocks: {expected}")));
            assert!(output.contains("Status: OK"));

            for suffix in expected_paths(root) {
                assert!(
                    output.contains(&format!("File: {suffix}")),
                    "missing {suffix} in:\n{output}"
                );
            }

            let summary = FsckCommand {
                path: root.to_string(),
                detail: false,
                list_page_size: 3,
            }
            .render(fs.fs_client())
            .await?;

            assert!(summary.contains(&format!("Files: {expected} | Blocks: {expected}")));
            assert!(summary.contains(&format!("Expected replicas: {expected}")));
            assert!(summary.contains(&format!("Recorded replicas: {expected}")));
            assert!(summary.contains(&format!("Available replicas: {expected}")));
            assert!(summary.contains("Unavailable replicas: 0"));
            assert!(summary.contains("Under-replicated blocks: 0"));
            assert!(summary.contains("Status: OK"));
            assert!(
                !summary.contains("File: "),
                "unexpected detail output:\n{summary}"
            );
        }
        Ok(())
    })
}

#[test]
fn fsck_reports_lost_replica_in_two_worker_cluster() -> CommonResult<()> {
    let testing = Testing::builder()
        .workers(2)
        .mutate_conf(|conf| {
            conf.client.replicas = 2;
            conf.client.block_size = 64 * 1024;
            conf.client.block_size_str = "64KB".to_string();
            conf.master.min_block_size = 64 * 1024;
            conf.master.heartbeat_interval = "10s".to_string();
            conf.master.worker_blacklist_interval = "20s".to_string();
            conf.master.worker_lost_interval = "30s".to_string();
        })
        .build()?;
    let cluster = testing.start_cluster()?;
    let master = cluster.get_active_master_fs();
    let conf = testing.get_active_cluster_conf()?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/fsck-multi/file")?;
        fs.write_string(&path, "replicated data").await?;

        let details = fs.fs_client().get_file_block_details(&path).await?;
        assert_eq!(details.blocks.len(), 1);
        assert_eq!(details.blocks[0].replicas.len(), 2);
        let lost_worker_id = details.blocks[0].replicas[0].worker_id;
        assert!(master
            .worker_manager
            .write()
            .remove_expired_worker(lost_worker_id)
            .is_some());

        let output = FsckCommand {
            path: "/fsck-multi".to_string(),
            detail: true,
            list_page_size: 3,
        }
        .render(fs.fs_client())
        .await?;

        assert!(output.contains("Files: 1 | Blocks: 1"));
        assert!(output.contains("Expected replicas: 2"));
        assert!(output.contains("Recorded replicas: 2"));
        assert!(output.contains("Available replicas: 1"));
        assert!(output.contains("Unavailable replicas: 1"));
        assert!(output.contains("Under-replicated blocks: 1"));
        assert!(output.contains("lost"));
        assert!(output.contains("live"));
        assert!(output.contains("Status: WARNING"));
        Ok(())
    })
}

fn expected_paths(root: &str) -> &'static [&'static str] {
    match root {
        "/fsck-pages/below" => &["/fsck-pages/below/file-1", "/fsck-pages/below/file-2"],
        "/fsck-pages/exact" => &[
            "/fsck-pages/exact/file-1",
            "/fsck-pages/exact/file-2",
            "/fsck-pages/exact/file-3",
        ],
        "/fsck-pages/over" => &[
            "/fsck-pages/over/file-1",
            "/fsck-pages/over/file-2",
            "/fsck-pages/over/file-3",
            "/fsck-pages/over/file-4",
            "/fsck-pages/over/nested/file-5",
        ],
        _ => &[],
    }
}
