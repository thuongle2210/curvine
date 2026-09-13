use clap::Parser;
use curvine_client_core::file::FsClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{FileBlockDetail, FileBlockDetails, FileStatus, FileType, ListOptions};
use curvine_runtime::common::ByteUnit;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;
use std::sync::Arc;

const DEFAULT_LIST_PAGE_SIZE: usize = 256;

#[derive(Parser, Debug)]
pub struct FsckCommand {
    #[clap(value_name = "path")]
    pub path: String,

    /// Show per-file, per-block, per-worker details for directories
    #[clap(long)]
    pub detail: bool,

    /// Show only blocks whose actual storage type differs from file policy
    #[clap(long)]
    pub policy_mismatch: bool,

    /// Maximum directory entries requested per listing RPC
    #[clap(
        long,
        default_value_t = DEFAULT_LIST_PAGE_SIZE,
        value_parser = parse_positive_usize
    )]
    pub list_page_size: usize,
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| "must be a positive integer".to_string())?;
    if value == 0 {
        return Err("must be greater than 0".to_string());
    }
    Ok(value)
}

#[derive(Default)]
struct FsckReport {
    root: String,
    is_dir: bool,
    files: Vec<FileScan>,
    storage_placements: BTreeMap<String, usize>,
    worker_placements: BTreeMap<String, usize>,
    block_count: usize,
    expected_replicas: usize,
    recorded_replicas: usize,
    available_replicas: usize,
    mismatch_blocks: usize,
    mismatch_replicas: usize,
    under_replicated_blocks: usize,
    unavailable_replicas: usize,
    errors: Vec<(String, String)>,
}

struct FileScan {
    details: FileBlockDetails,
}

impl FileScan {
    fn mismatch_count(&self) -> usize {
        self.details
            .blocks
            .iter()
            .filter(|block| block_mismatches(&self.details.status, block))
            .count()
    }
}

impl FsckReport {
    fn add_file(&mut self, mut details: FileBlockDetails) {
        details.blocks.sort_by_key(|block| block.offset);
        for block in &mut details.blocks {
            block.replicas.sort_by_key(|replica| replica.worker_id);
            self.block_count += 1;
            self.expected_replicas += details.status.replicas.max(0) as usize;
            self.recorded_replicas += block.replicas.len();
            self.available_replicas += block
                .replicas
                .iter()
                .filter(|replica| replica.address.is_some())
                .count();

            if block_mismatches(&details.status, block) {
                self.mismatch_blocks += 1;
            }
            self.mismatch_replicas += block
                .replicas
                .iter()
                .filter(|replica| {
                    replica.storage_type != details.status.storage_policy.storage_type
                })
                .count();
            if block.replicas.len() < details.status.replicas.max(0) as usize {
                self.under_replicated_blocks += 1;
            }

            for replica in &block.replicas {
                *self
                    .storage_placements
                    .entry(replica.storage_type.as_str_name().to_string())
                    .or_default() += 1;

                let worker = match &replica.address {
                    Some(address) => format!("{}:{}", address.hostname, address.rpc_port),
                    None => {
                        self.unavailable_replicas += 1;
                        format!("worker-{} (unavailable)", replica.worker_id)
                    }
                };
                *self.worker_placements.entry(worker).or_default() += 1;
            }
        }
        self.files.push(FileScan { details });
    }

    fn has_warnings(&self) -> bool {
        self.mismatch_blocks > 0
            || self.under_replicated_blocks > 0
            || self.unavailable_replicas > 0
            || !self.errors.is_empty()
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
        } else if status.file_type == FileType::Link {
            report.add_file(FileBlockDetails {
                status,
                blocks: Vec::new(),
            });
        } else {
            self.scan_file(&client, status, &mut report, false).await?;
        }

        report
            .files
            .sort_by(|left, right| left.details.status.path.cmp(&right.details.status.path));
        print!(
            "{}",
            render_report(&report, self.detail, self.policy_mismatch)
        );
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
            Err(error) if tolerate_error => report.errors.push((status.path, error.to_string())),
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn block_mismatches(status: &FileStatus, block: &FileBlockDetail) -> bool {
    block
        .replicas
        .iter()
        .any(|replica| replica.storage_type != status.storage_policy.storage_type)
}

fn render_report(report: &FsckReport, detail: bool, policy_mismatch: bool) -> String {
    if !report.is_dir {
        return report
            .files
            .first()
            .map(|file| render_file(file, policy_mismatch, true))
            .unwrap_or_default();
    }

    let mut output = String::new();
    writeln!(output, "Directory: {}", report.root).unwrap();
    writeln!(
        output,
        "Files: {} | Blocks: {}",
        report.files.len(),
        report.block_count
    )
    .unwrap();

    if detail || policy_mismatch {
        writeln!(output).unwrap();
        for file in &report.files {
            if !policy_mismatch || file.mismatch_count() > 0 {
                output.push_str(&render_file(file, policy_mismatch, false));
                writeln!(output).unwrap();
            }
        }
    }

    render_distribution(
        &mut output,
        "Storage Distribution (replica placements)",
        &report.storage_placements,
    );
    render_distribution(
        &mut output,
        "Per-Worker Distribution (replica placements)",
        &report.worker_placements,
    );

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

    writeln!(output, "\n--- Findings ---").unwrap();
    writeln!(
        output,
        "  Policy mismatch blocks: {}",
        report.mismatch_blocks
    )
    .unwrap();
    writeln!(
        output,
        "  Mismatched replica placements: {}",
        report.mismatch_replicas
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

fn render_file(file: &FileScan, policy_mismatch: bool, include_status: bool) -> String {
    let mut output = String::new();
    let status = &file.details.status;
    writeln!(output, "File: {}", status.path).unwrap();
    writeln!(
        output,
        "Size: {} | Blocks: {} | Policy: {}",
        ByteUnit::byte_to_string(status.len.max(0) as u64),
        file.details.blocks.len(),
        status.storage_policy.storage_type.as_str_name()
    )
    .unwrap();

    for (index, block) in file.details.blocks.iter().enumerate() {
        if policy_mismatch && !block_mismatches(status, block) {
            continue;
        }
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
                .unwrap_or_else(|| format!("worker-{} (unavailable)", replica.worker_id));
            let mismatch = if replica.storage_type != status.storage_policy.storage_type {
                " [MISMATCH]"
            } else {
                ""
            };
            writeln!(
                output,
                "  {:<28} {}{}",
                worker,
                replica.storage_type.as_str_name(),
                mismatch
            )
            .unwrap();
        }
    }

    if include_status {
        let warning = file.mismatch_count() > 0
            || file.details.blocks.iter().any(|block| {
                block.replicas.len() < status.replicas.max(0) as usize
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

fn render_distribution(output: &mut String, title: &str, values: &BTreeMap<String, usize>) {
    writeln!(output, "\n--- {} ---", title).unwrap();
    let total: usize = values.values().sum();
    if total == 0 {
        writeln!(output, "  (none)").unwrap();
        return;
    }
    for (name, count) in values {
        let percent = (*count as f64 / total as f64) * 100.0;
        writeln!(output, "  {}: {} ({:.1}%)", name, count, percent).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{BlockReplicaDetail, StoragePolicy, StorageType, WorkerAddress};

    fn address(worker_id: u32, hostname: &str) -> WorkerAddress {
        WorkerAddress {
            worker_id,
            hostname: hostname.to_string(),
            rpc_port: 50010,
            ..Default::default()
        }
    }

    fn replica(
        worker_id: u32,
        storage_type: StorageType,
        address: Option<WorkerAddress>,
    ) -> BlockReplicaDetail {
        BlockReplicaDetail {
            worker_id,
            storage_type,
            address,
        }
    }

    fn block(block_id: i64, offset: i64, replicas: Vec<BlockReplicaDetail>) -> FileBlockDetail {
        FileBlockDetail {
            block_id,
            len: 100,
            offset,
            replicas,
        }
    }

    fn details() -> FileBlockDetails {
        FileBlockDetails {
            status: FileStatus {
                path: "/data/file".to_string(),
                len: 100,
                replicas: 2,
                storage_policy: StoragePolicy {
                    storage_type: StorageType::SpdkDisk,
                    ..Default::default()
                },
                ..Default::default()
            },
            blocks: vec![block(
                7,
                0,
                vec![
                    replica(1, StorageType::SpdkDisk, Some(address(1, "worker-a"))),
                    replica(2, StorageType::Disk, None),
                ],
            )],
        }
    }

    #[test]
    fn list_page_size_parser_accepts_positive_boundaries() {
        assert_eq!(parse_positive_usize("1").unwrap(), 1);
        assert_eq!(parse_positive_usize("256").unwrap(), 256);
        assert_eq!(
            parse_positive_usize(&usize::MAX.to_string()).unwrap(),
            usize::MAX
        );
    }

    #[test]
    fn list_page_size_parser_rejects_invalid_values() {
        for value in ["0", "-1", "abc", "1.5", "184467440737095516160"] {
            assert!(parse_positive_usize(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn healthy_file_has_no_warnings() {
        let mut healthy = details();
        healthy.blocks[0].replicas[1] =
            replica(2, StorageType::SpdkDisk, Some(address(2, "worker-b")));
        let mut report = FsckReport::default();
        report.add_file(healthy);

        assert_eq!(report.expected_replicas, 2);
        assert_eq!(report.recorded_replicas, 2);
        assert_eq!(report.available_replicas, 2);
        assert_eq!(report.unavailable_replicas, 0);
        assert_eq!(report.under_replicated_blocks, 0);
        assert!(!report.has_warnings());
    }

    #[test]
    fn empty_file_has_no_blocks_or_replica_expectations() {
        let mut empty = details();
        empty.status.len = 0;
        empty.blocks.clear();
        let mut report = FsckReport::default();
        report.add_file(empty);

        assert_eq!(report.block_count, 0);
        assert_eq!(report.expected_replicas, 0);
        assert_eq!(report.recorded_replicas, 0);
        assert!(!report.has_warnings());
    }

    #[test]
    fn missing_replicas_are_counted_as_under_replicated() {
        let mut under_replicated = details();
        under_replicated.blocks[0].replicas.truncate(1);
        let mut report = FsckReport::default();
        report.add_file(under_replicated);

        assert_eq!(report.expected_replicas, 2);
        assert_eq!(report.recorded_replicas, 1);
        assert_eq!(report.available_replicas, 1);
        assert_eq!(report.under_replicated_blocks, 1);
        assert!(report.has_warnings());
    }

    #[test]
    fn block_without_replicas_is_under_replicated_not_mismatched() {
        let mut missing = details();
        missing.blocks[0].replicas.clear();
        let mut report = FsckReport::default();
        report.add_file(missing);

        assert_eq!(report.under_replicated_blocks, 1);
        assert_eq!(report.mismatch_blocks, 0);
        assert_eq!(report.mismatch_replicas, 0);
    }

    #[test]
    fn unavailable_replica_is_recorded_but_not_available() {
        let mut report = FsckReport::default();
        report.add_file(details());

        assert_eq!(report.recorded_replicas, 2);
        assert_eq!(report.available_replicas, 1);
        assert_eq!(report.unavailable_replicas, 1);
        assert_eq!(report.under_replicated_blocks, 0);
    }

    #[test]
    fn add_file_sorts_blocks_and_replicas_deterministically() {
        let mut unordered = details();
        unordered.blocks = vec![
            block(8, 100, vec![replica(1, StorageType::SpdkDisk, None)]),
            block(
                7,
                0,
                vec![
                    replica(3, StorageType::SpdkDisk, None),
                    replica(1, StorageType::SpdkDisk, None),
                ],
            ),
        ];
        let mut report = FsckReport::default();
        report.add_file(unordered);

        assert_eq!(report.files[0].details.blocks[0].block_id, 7);
        assert_eq!(report.files[0].details.blocks[1].block_id, 8);
        assert_eq!(report.files[0].details.blocks[0].replicas[0].worker_id, 1);
        assert_eq!(report.files[0].details.blocks[0].replicas[1].worker_id, 3);
    }

    #[test]
    fn scan_error_sets_warning_and_is_rendered() {
        let report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            errors: vec![("/data/bad".to_string(), "rpc failed".to_string())],
            ..Default::default()
        };
        let output = render_report(&report, false, false);

        assert!(report.has_warnings());
        assert!(output.contains("Failed files: 1"));
        assert!(output.contains("Failed to inspect /data/bad: rpc failed"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn file_report_shows_actual_storage_and_warning() {
        let report = FsckReport {
            files: vec![FileScan { details: details() }],
            ..Default::default()
        };
        let output = render_report(&report, false, false);
        assert!(output.contains("worker-a:50010"));
        assert!(output.contains("SPDK_DISK"));
        assert!(output.contains("worker-2 (unavailable)"));
        assert!(output.contains("DISK"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn directory_summary_counts_replica_placements() {
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_file(details());
        let output = render_report(&report, false, false);
        assert!(output.contains("SPDK_DISK: 1 (50.0%)"));
        assert!(output.contains("DISK: 1 (50.0%)"));
        assert!(output.contains("Policy mismatch blocks: 1"));
        assert!(output.contains("Mismatched replica placements: 1"));
        assert!(output.contains("Expected replicas: 2"));
        assert!(output.contains("Recorded replicas: 2"));
        assert!(output.contains("Available replicas: 1"));
    }

    #[test]
    fn multiple_mismatched_replicas_count_as_one_mismatch_block() {
        let mut mixed = details();
        mixed.status.replicas = 3;
        mixed.blocks[0].replicas = vec![
            replica(1, StorageType::SpdkDisk, Some(address(1, "worker-a"))),
            replica(2, StorageType::Disk, Some(address(2, "worker-b"))),
            replica(3, StorageType::Ssd, Some(address(3, "worker-c"))),
        ];
        let mut report = FsckReport::default();
        report.add_file(mixed);

        assert_eq!(report.mismatch_blocks, 1);
        assert_eq!(report.mismatch_replicas, 2);
        assert_eq!(report.under_replicated_blocks, 0);
    }

    #[test]
    fn storage_distribution_uses_replica_placements_as_denominator() {
        let mut mixed = details();
        mixed.status.replicas = 4;
        mixed.blocks[0].replicas = vec![
            replica(1, StorageType::SpdkDisk, Some(address(1, "worker-a"))),
            replica(2, StorageType::SpdkDisk, Some(address(2, "worker-b"))),
            replica(3, StorageType::Disk, Some(address(3, "worker-c"))),
            replica(4, StorageType::Ssd, None),
        ];
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_file(mixed);
        let output = render_report(&report, false, false);

        assert!(output.contains("SPDK_DISK: 2 (50.0%)"));
        assert!(output.contains("DISK: 1 (25.0%)"));
        assert!(output.contains("SSD: 1 (25.0%)"));
    }

    #[test]
    fn unavailable_replica_contributes_persisted_storage_type() {
        let mut report = FsckReport::default();
        report.add_file(details());

        assert_eq!(report.storage_placements.get("DISK"), Some(&1));
        assert_eq!(
            report.worker_placements.get("worker-2 (unavailable)"),
            Some(&1)
        );
    }

    #[test]
    fn mismatch_marker_only_labels_mismatched_replicas() {
        let output = render_file(&FileScan { details: details() }, false, false);
        let matching = output
            .lines()
            .find(|line| line.contains("worker-a:50010"))
            .unwrap();
        let mismatching = output
            .lines()
            .find(|line| line.contains("worker-2 (unavailable)"))
            .unwrap();

        assert!(!matching.contains("[MISMATCH]"));
        assert!(mismatching.contains("[MISMATCH]"));
    }

    #[test]
    fn policy_mismatch_hides_matching_blocks() {
        let mut details = details();
        details
            .blocks
            .push(block(8, 100, vec![replica(1, StorageType::SpdkDisk, None)]));
        let output = render_file(&FileScan { details }, true, false);
        assert!(output.contains("blk_7"));
        assert!(!output.contains("blk_8"));
    }

    #[test]
    fn policy_mismatch_hides_fully_matching_files_in_directory_detail() {
        let mut matching = details();
        matching.status.path = "/data/matching".to_string();
        matching.blocks[0].replicas = vec![replica(
            1,
            StorageType::SpdkDisk,
            Some(address(1, "worker-a")),
        )];
        let mut mismatching = details();
        mismatching.status.path = "/data/mismatching".to_string();
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_file(matching);
        report.add_file(mismatching);

        let output = render_report(&report, false, true);
        assert!(!output.contains("File: /data/matching"));
        assert!(output.contains("File: /data/mismatching"));
    }

    #[test]
    fn empty_directory_renders_empty_distributions_and_ok_status() {
        let report = FsckReport {
            root: "/empty".to_string(),
            is_dir: true,
            ..Default::default()
        };
        let output = render_report(&report, false, false);

        assert_eq!(output.matches("  (none)").count(), 2);
        assert!(output.contains("Files: 0 | Blocks: 0"));
        assert!(output.contains("Status: OK"));
    }
}
