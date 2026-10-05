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
        }
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
