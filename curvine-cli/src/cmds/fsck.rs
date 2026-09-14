use clap::Parser;
use curvine_client_core::file::FsClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{FileBlockDetails, FileStatus, FileType, ListOptions};
use curvine_runtime::common::ByteUnit;
use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::Arc;

const DEFAULT_LIST_PAGE_SIZE: usize = 256;
const MAX_LIST_PAGE_SIZE: usize = 4096;

#[derive(Parser, Debug)]
pub struct FsckCommand {
    #[clap(value_name = "path")]
    pub path: String,

    /// Show per-file and per-block details for directories
    #[clap(long)]
    pub detail: bool,

    /// Maximum directory entries requested per listing RPC
    #[clap(
        long,
        default_value_t = DEFAULT_LIST_PAGE_SIZE,
        value_parser = parse_list_page_size
    )]
    pub list_page_size: usize,
}

fn parse_list_page_size(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_string())?;
    if !(1..=MAX_LIST_PAGE_SIZE).contains(&value) {
        return Err(format!("must be between 1 and {MAX_LIST_PAGE_SIZE}"));
    }
    Ok(value)
}

#[derive(Default)]
struct FsckReport {
    root: String,
    is_dir: bool,
    files: Vec<FileBlockDetails>,
    file_count: usize,
    block_count: usize,
    expected_replicas: usize,
    recorded_replicas: usize,
    available_replicas: usize,
    unavailable_replicas: usize,
    under_replicated_blocks: usize,
    errors: Vec<(String, String)>,
}

impl FsckReport {
    fn add_file(&mut self, mut details: FileBlockDetails) {
        details.blocks.sort_by_key(|block| block.offset);
        for block in &mut details.blocks {
            block.replicas.sort_by_key(|replica| replica.worker_id);
            let expected = details.status.replicas.max(0) as usize;
            let available = block
                .replicas
                .iter()
                .filter(|replica| replica.address.is_some())
                .count();

            self.block_count += 1;
            self.expected_replicas += expected;
            self.recorded_replicas += block.replicas.len();
            self.available_replicas += available;
            self.unavailable_replicas += block.replicas.len() - available;
            if block.replicas.len() < expected {
                self.under_replicated_blocks += 1;
            }
        }
        self.file_count += 1;
        self.files.push(details);
    }

    fn add_error(&mut self, path: String, error: String) {
        self.file_count += 1;
        self.errors.push((path, error));
    }

    fn has_warnings(&self) -> bool {
        self.under_replicated_blocks > 0 || self.unavailable_replicas > 0 || !self.errors.is_empty()
    }
}

impl FsckCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        let path = Path::from_str(&self.path)?;
        let status = client.file_status(&path).await?;
        let mut report = FsckReport {
            root: status.path.clone(),
            is_dir: status.is_dir,
            ..Default::default()
        };

        if status.is_dir {
            self.scan_directory(&client, status, &mut report).await?;
        } else if !should_inspect_blocks(&status) {
            report.add_file(FileBlockDetails {
                status,
                blocks: Vec::new(),
            });
        } else {
            self.scan_file(&client, status, &mut report, false).await?;
        }

        report
            .files
            .sort_by(|left, right| left.status.path.cmp(&right.status.path));
        print!("{}", render_report(&report, self.detail));
        Ok(())
    }

    async fn scan_directory(
        &self,
        client: &Arc<FsClient>,
        root: FileStatus,
        report: &mut FsckReport,
    ) -> CommonResult<()> {
        let mut directories = VecDeque::from([root.path]);
        while let Some(directory) = directories.pop_front() {
            let path = Path::from_str(&directory)?;
            let mut start_after = None;

            loop {
                let entries = client
                    .list_options(
                        &path,
                        ListOptions {
                            limit: Some(self.list_page_size),
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
                        self.scan_file(client, entry, report, true).await?;
                    }
                }

                if entry_count < self.list_page_size {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn scan_file(
        &self,
        client: &Arc<FsClient>,
        status: FileStatus,
        report: &mut FsckReport,
        tolerate_error: bool,
    ) -> CommonResult<()> {
        if status.storage_policy.ufs_only() {
            report.add_file(FileBlockDetails {
                status,
                blocks: Vec::new(),
            });
            return Ok(());
        }

        let path = Path::from_str(&status.path)?;
        match client.get_file_block_details(&path).await {
            Ok(details) => report.add_file(details),
            Err(error) if tolerate_error => report.add_error(status.path, error.to_string()),
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn should_inspect_blocks(status: &FileStatus) -> bool {
    !status.is_dir && status.file_type != FileType::Link && !status.storage_policy.ufs_only()
}

fn render_report(report: &FsckReport, detail: bool) -> String {
    if !report.is_dir {
        return report
            .files
            .first()
            .map(|file| render_file(file, true))
            .unwrap_or_default();
    }

    let mut output = String::new();
    writeln!(output, "Directory: {}", report.root).unwrap();
    writeln!(
        output,
        "Files: {} | Blocks: {}",
        report.file_count, report.block_count
    )
    .unwrap();

    if detail {
        writeln!(output).unwrap();
        for file in &report.files {
            output.push_str(&render_file(file, false));
            writeln!(output).unwrap();
        }
    }

    writeln!(output, "\n--- Replica Health ---").unwrap();
    writeln!(output, "  Expected replicas: {}", report.expected_replicas).unwrap();
    writeln!(output, "  Recorded replicas: {}", report.recorded_replicas).unwrap();
    writeln!(
        output,
        "  Available replicas: {}",
        report.available_replicas
    )
    .unwrap();
    writeln!(
        output,
        "  Unavailable replicas: {}",
        report.unavailable_replicas
    )
    .unwrap();
    writeln!(
        output,
        "  Under-replicated blocks: {}",
        report.under_replicated_blocks
    )
    .unwrap();
    writeln!(output, "  Failed files: {}", report.errors.len()).unwrap();
    for (path, error) in &report.errors {
        writeln!(output, "  Failed to inspect {}: {}", path, error).unwrap();
    }
    writeln!(
        output,
        "\nStatus: {}",
        if report.has_warnings() {
            "WARNING"
        } else {
            "OK"
        }
    )
    .unwrap();
    output
}

fn render_file(details: &FileBlockDetails, include_status: bool) -> String {
    let mut output = String::new();
    writeln!(output, "File: {}", details.status.path).unwrap();
    writeln!(
        output,
        "Size: {} | Blocks: {} | Expected replicas: {}",
        ByteUnit::byte_to_string(details.status.len.max(0) as u64),
        details.blocks.len(),
        details.status.replicas.max(0)
    )
    .unwrap();

    for (index, block) in details.blocks.iter().enumerate() {
        writeln!(
            output,
            "\nBlock {} (blk_{}): {}",
            index + 1,
            block.block_id,
            ByteUnit::byte_to_string(block.len.max(0) as u64)
        )
        .unwrap();
        if block.replicas.is_empty() {
            writeln!(output, "  no replicas").unwrap();
        }
        for replica in &block.replicas {
            let worker = replica
                .address
                .as_ref()
                .map(|address| format!("{}:{}", address.hostname, address.rpc_port))
                .unwrap_or_else(|| format!("worker-{}", replica.worker_id));
            let availability = if replica.address.is_some() {
                "available"
            } else {
                "unavailable"
            };
            writeln!(output, "  {:<28} {}", worker, availability).unwrap();
        }
    }

    if include_status {
        let warning = details.blocks.iter().any(|block| {
            block.replicas.len() < details.status.replicas.max(0) as usize
                || block
                    .replicas
                    .iter()
                    .any(|replica| replica.address.is_none())
        });
        writeln!(
            output,
            "\nStatus: {}",
            if warning { "WARNING" } else { "OK" }
        )
        .unwrap();
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{
        BlockReplicaDetail, FileBlockDetail, StoragePolicy, StorageState, WorkerAddress,
    };

    fn address(worker_id: u32) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            hostname: format!("worker-{worker_id}"),
            rpc_port: 50010,
            ..Default::default()
        }
    }

    fn details(replicas: i32, addresses: Vec<Option<WorkerAddress>>) -> FileBlockDetails {
        FileBlockDetails {
            status: FileStatus {
                path: "/data/file".to_string(),
                len: 100,
                replicas,
                ..Default::default()
            },
            blocks: vec![FileBlockDetail {
                block_id: 7,
                len: 100,
                offset: 0,
                replicas: addresses
                    .into_iter()
                    .enumerate()
                    .map(|(index, address)| BlockReplicaDetail {
                        worker_id: index as u32 + 1,
                        storage_type: Default::default(),
                        address,
                    })
                    .collect(),
            }],
        }
    }

    #[test]
    fn list_page_size_parser_enforces_bounds() {
        assert_eq!(parse_list_page_size("1").unwrap(), 1);
        assert_eq!(parse_list_page_size("4096").unwrap(), 4096);
        for value in ["0", "4097", "-1", "abc", "1.5"] {
            assert!(parse_list_page_size(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn healthy_and_empty_files_have_no_warnings() {
        let mut report = FsckReport::default();
        report.add_file(details(2, vec![Some(address(1)), Some(address(2))]));
        assert_eq!(report.expected_replicas, 2);
        assert_eq!(report.recorded_replicas, 2);
        assert_eq!(report.available_replicas, 2);
        assert!(!report.has_warnings());

        let mut empty = details(2, Vec::new());
        empty.status.len = 0;
        empty.blocks.clear();
        let mut empty_report = FsckReport::default();
        empty_report.add_file(empty);
        assert_eq!(empty_report.expected_replicas, 0);
        assert!(!empty_report.has_warnings());
    }

    #[test]
    fn missing_and_unavailable_replicas_are_accounted_separately() {
        let mut report = FsckReport::default();
        report.add_file(details(3, vec![Some(address(1)), None]));

        assert_eq!(report.expected_replicas, 3);
        assert_eq!(report.recorded_replicas, 2);
        assert_eq!(report.available_replicas, 1);
        assert_eq!(report.unavailable_replicas, 1);
        assert_eq!(report.under_replicated_blocks, 1);
        assert!(report.has_warnings());
    }

    #[test]
    fn zero_recorded_replicas_are_under_replicated() {
        let mut report = FsckReport::default();
        report.add_file(details(1, Vec::new()));
        assert_eq!(report.recorded_replicas, 0);
        assert_eq!(report.under_replicated_blocks, 1);
    }

    #[test]
    fn add_file_sorts_blocks_and_replicas() {
        let mut unordered = details(2, vec![Some(address(1))]);
        unordered.blocks = vec![
            FileBlockDetail {
                block_id: 8,
                len: 100,
                offset: 100,
                replicas: Vec::new(),
            },
            FileBlockDetail {
                block_id: 7,
                len: 100,
                offset: 0,
                replicas: vec![
                    BlockReplicaDetail {
                        worker_id: 3,
                        storage_type: Default::default(),
                        address: None,
                    },
                    BlockReplicaDetail {
                        worker_id: 1,
                        storage_type: Default::default(),
                        address: None,
                    },
                ],
            },
        ];
        let mut report = FsckReport::default();
        report.add_file(unordered);

        assert_eq!(report.files[0].blocks[0].block_id, 7);
        assert_eq!(report.files[0].blocks[1].block_id, 8);
        assert_eq!(report.files[0].blocks[0].replicas[0].worker_id, 1);
        assert_eq!(report.files[0].blocks[0].replicas[1].worker_id, 3);
    }

    #[test]
    fn recoverable_error_is_counted_and_rendered() {
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_error("/data/bad".to_string(), "rpc failed".to_string());
        let output = render_report(&report, false);

        assert!(output.contains("Files: 1 | Blocks: 0"));
        assert!(output.contains("Failed files: 1"));
        assert!(output.contains("Failed to inspect /data/bad: rpc failed"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn file_report_shows_health_without_storage_analysis() {
        let output = render_file(&details(2, vec![Some(address(1)), None]), true);
        assert!(output.contains("worker-1:50010"));
        assert!(output.contains("available"));
        assert!(output.contains("worker-2"));
        assert!(output.contains("unavailable"));
        assert!(!output.contains("MISMATCH"));
        assert!(!output.contains("Policy:"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn empty_directory_is_ok() {
        let report = FsckReport {
            root: "/empty".to_string(),
            is_dir: true,
            ..Default::default()
        };
        let output = render_report(&report, false);
        assert!(output.contains("Files: 0 | Blocks: 0"));
        assert!(output.contains("Status: OK"));
    }

    #[test]
    fn links_and_ufs_only_files_skip_block_inspection() {
        let mut status = FileStatus::default();
        assert!(should_inspect_blocks(&status));

        status.file_type = FileType::Link;
        assert!(!should_inspect_blocks(&status));

        status.file_type = FileType::File;
        status.storage_policy = StoragePolicy {
            state: StorageState::Ufs,
            ..Default::default()
        };
        assert!(!should_inspect_blocks(&status));
    }
}
