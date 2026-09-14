use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{FileStatus, FileType, ListOptions};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::collections::VecDeque;
use std::sync::Arc;

#[test]
fn fsck_style_traversal_pages_nested_directories_without_duplicates() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
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
            let files = collect_files(&fs, root, 3).await?;
            assert_eq!(files.len(), expected);

            let mut paths = files
                .iter()
                .map(|status| status.path.as_str())
                .collect::<Vec<_>>();
            paths.sort_unstable();
            paths.dedup();
            assert_eq!(paths.len(), expected);

            for status in files {
                let details = fs
                    .fs_client()
                    .get_file_block_details(&Path::from_str(&status.path)?)
                    .await?;
                assert_eq!(details.status.path, status.path);
                assert_eq!(details.blocks.len(), 1);
            }
        }
        Ok(())
    })
}

async fn collect_files(
    fs: &curvine_client::file::CurvineFileSystem,
    root: &str,
    page_size: usize,
) -> CommonResult<Vec<FileStatus>> {
    let mut directories = VecDeque::from([root.to_string()]);
    let mut files = Vec::new();

    while let Some(directory) = directories.pop_front() {
        let path = Path::from_str(directory)?;
        let mut start_after = None;
        loop {
            let entries = fs
                .fs_client()
                .list_options(
                    &path,
                    ListOptions {
                        limit: Some(page_size),
                        start_after: start_after.clone(),
                    },
                )
                .await?;
            let entry_count = entries.len();
            start_after = entries.last().map(|entry| entry.name.clone());

            for entry in entries {
                if entry.is_dir {
                    directories.push_back(entry.path);
                } else if entry.file_type != FileType::Link {
                    files.push(entry);
                }
            }

            if entry_count < page_size {
                break;
            }
        }
    }

    Ok(files)
}
