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
const MAX_LIST_PAGE_SIZE: usize = 4096;

#[derive(Parser, Debug)]
pub struct FsckCommand {
    #[clap(value_name = "path")]
    pub path: String,

    /// Show per-file and per-block details for directories
    #[clap(long)]
    pub detail: bool,

    /// Show only blocks whose actual storage type differs from file policy
    #[clap(long)]
    pub policy_mismatch: bool,

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
    files: Vec<FileScan>,
    storage_placements: BTreeMap<String, usize>,
    worker_placements: BTreeMap<String, usize>,
    file_count: usize,
    block_count: usize,
    expected_replicas: usize,
    recorded_replicas: usize,
    available_replicas: usize,
    unavailable_replicas: usize,
    under_replicated_blocks: usize,
    mismatch_blocks: usize,
    mismatch_replicas: usize,
    errors: Vec<(String, String)>,
}

struct FileScan {
    details: FileBlockDetails,
}

impl FileScan {
    fn mismatch_blocks(&self) -> usize {
        self.details
            .blocks
            .iter()
            .filter(|block| block_mismatches(&self.details.status, block))
            .count()
    }

    fn mismatch_replicas(&self) -> usize {
        self.details
            .blocks
            .iter()
            .flat_map(|block| &block.replicas)
            .filter(|replica| {
                replica.storage_type != self.details.status.storage_policy.storage_type
            })
            .count()
    }
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
            if block.replicas.len() < expected {
                self.under_replicated_blocks += 1;
            }

            for replica in &block.replicas {
                *self
                    .storage_placements
                    .entry(replica.storage_type.as_str_name().to_string())
                    .or_default() += 1;
                let worker = replica
                    .address
                    .as_ref()
                    .map(|address| format!("{}:{}", address.hostname, address.rpc_port))
                    .unwrap_or_else(|| format!("worker-{} (unavailable)", replica.worker_id));
                *self.worker_placements.entry(worker).or_default() += 1;
            }
        }
        self.file_count += 1;
        self.files.push(FileScan { details });
    }

    fn add_error(&mut self, path: String, error: String) {
        self.file_count += 1;
        self.errors.push((path, error));
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
            Err(error) if tolerate_error => report.add_error(status.path, error.to_string()),
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}

fn should_inspect_blocks(status: &FileStatus) -> bool {
    !status.is_dir && status.file_type != FileType::Link && !status.storage_policy.ufs_only()
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
        report.file_count, report.block_count
    )
    .unwrap();

    if detail || policy_mismatch {
        writeln!(output).unwrap();
        for file in &report.files {
            if !policy_mismatch || file.mismatch_blocks() > 0 {
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
    if report.mismatch_blocks > 0 {
        writeln!(
            output,
            "  Placement mismatches may reflect storage fallback; they do not indicate data corruption."
        )
        .unwrap();
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
    let details = &file.details;
    let status = &details.status;
    writeln!(output, "File: {}", status.path).unwrap();
    writeln!(
        output,
        "Size: {} | Blocks: {} | Policy: {} | Expected replicas: {}",
        ByteUnit::byte_to_string(status.len.max(0) as u64),
        details.blocks.len(),
        status.storage_policy.storage_type.as_str_name(),
        status.replicas.max(0)
    )
    .unwrap();

    for (index, block) in details.blocks.iter().enumerate() {
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
        let mismatch_blocks = file.mismatch_blocks();
        let mismatch_replicas = file.mismatch_replicas();
        let warning = mismatch_blocks > 0
            || details.blocks.iter().any(|block| {
                block.replicas.len() < status.replicas.max(0) as usize
                    || block
                        .replicas
                        .iter()
                        .any(|replica| replica.address.is_none())
            });
        writeln!(output, "\n--- Findings ---").unwrap();
        writeln!(output, "  Policy mismatch blocks: {mismatch_blocks}").unwrap();
        writeln!(
            output,
            "  Mismatched replica placements: {mismatch_replicas}"
        )
        .unwrap();
        if mismatch_blocks > 0 {
            writeln!(
                output,
                "  Placement mismatches may reflect storage fallback; they do not indicate data corruption."
            )
            .unwrap();
        }
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
    writeln!(output, "\n--- {title} ---").unwrap();
    let total: usize = values.values().sum();
    if total == 0 {
        writeln!(output, "  (none)").unwrap();
        return;
    }
    for (name, count) in values {
        let percent = (*count as f64 / total as f64) * 100.0;
        writeln!(output, "  {name}: {count} ({percent:.1}%)").unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{
        BlockReplicaDetail, FileBlockDetail, StoragePolicy, StorageState, StorageType,
        WorkerAddress,
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

        assert_eq!(report.files[0].details.blocks[0].block_id, 7);
        assert_eq!(report.files[0].details.blocks[1].block_id, 8);
        assert_eq!(report.files[0].details.blocks[0].replicas[0].worker_id, 1);
        assert_eq!(report.files[0].details.blocks[0].replicas[1].worker_id, 3);
    }

    #[test]
    fn recoverable_error_is_counted_and_rendered() {
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_error("/data/bad".to_string(), "rpc failed".to_string());
        let output = render_report(&report, false, false);

        assert!(output.contains("Files: 1 | Blocks: 0"));
        assert!(output.contains("Failed files: 1"));
        assert!(output.contains("Failed to inspect /data/bad: rpc failed"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn file_report_shows_requested_and_actual_storage() {
        let mut details = details(2, vec![Some(address(1)), None]);
        details.status.storage_policy.storage_type = StorageType::Mem;
        details.blocks[0].replicas[0].storage_type = StorageType::Mem;
        details.blocks[0].replicas[1].storage_type = StorageType::Disk;
        let output = render_file(&FileScan { details }, false, true);
        assert!(output.contains("worker-1:50010"));
        assert!(output.contains("Policy: MEM"));
        assert!(output.contains("worker-2 (unavailable)"));
        assert!(output.contains("DISK [MISMATCH]"));
        assert!(output.contains("Policy mismatch blocks: 1"));
        assert!(output.contains("Mismatched replica placements: 1"));
        assert!(output.contains("do not indicate data corruption"));
        assert!(output.contains("Status: WARNING"));
    }

    #[test]
    fn empty_directory_is_ok() {
        let report = FsckReport {
            root: "/empty".to_string(),
            is_dir: true,
            ..Default::default()
        };
        let output = render_report(&report, false, false);
        assert!(output.contains("Files: 0 | Blocks: 0"));
        assert_eq!(output.matches("  (none)").count(), 2);
        assert!(output.contains("Status: OK"));
    }

    #[test]
    fn multiple_mismatched_replicas_count_as_one_block() {
        let mut mixed = details(3, Vec::new());
        mixed.status.storage_policy.storage_type = StorageType::Mem;
        mixed.blocks[0].replicas = vec![
            replica(1, StorageType::Mem, Some(address(1))),
            replica(2, StorageType::Disk, Some(address(2))),
            replica(3, StorageType::Ssd, Some(address(3))),
        ];
        let mut report = FsckReport::default();
        report.add_file(mixed);

        assert_eq!(report.mismatch_blocks, 1);
        assert_eq!(report.mismatch_replicas, 2);
        assert_eq!(report.under_replicated_blocks, 0);
    }

    #[test]
    fn block_without_replicas_is_not_a_policy_mismatch() {
        let mut report = FsckReport::default();
        report.add_file(details(1, Vec::new()));
        assert_eq!(report.mismatch_blocks, 0);
        assert_eq!(report.mismatch_replicas, 0);
        assert_eq!(report.under_replicated_blocks, 1);
    }

    #[test]
    fn placement_distributions_use_all_replica_placements() {
        let mut mixed = details(4, Vec::new());
        mixed.status.storage_policy.storage_type = StorageType::Mem;
        mixed.blocks[0].replicas = vec![
            replica(1, StorageType::Mem, Some(address(1))),
            replica(2, StorageType::SpdkDisk, Some(address(2))),
            replica(3, StorageType::Disk, Some(address(3))),
            replica(4, StorageType::Ssd, None),
        ];
        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_file(mixed);
        let output = render_report(&report, false, false);

        for storage_type in ["MEM", "SPDK_DISK", "DISK", "SSD"] {
            assert!(output.contains(&format!("{storage_type}: 1 (25.0%)")));
        }
        assert_eq!(report.storage_placements.get("SSD"), Some(&1));
        assert_eq!(
            report.worker_placements.get("worker-4 (unavailable)"),
            Some(&1)
        );
    }

    #[test]
    fn mismatch_marker_only_labels_mismatched_replicas() {
        let mut mixed = details(2, Vec::new());
        mixed.status.storage_policy.storage_type = StorageType::Mem;
        mixed.blocks[0].replicas = vec![
            replica(1, StorageType::Mem, Some(address(1))),
            replica(2, StorageType::Disk, Some(address(2))),
        ];
        let output = render_file(&FileScan { details: mixed }, false, false);
        let matching = output
            .lines()
            .find(|line| line.contains("worker-1"))
            .unwrap();
        let mismatching = output
            .lines()
            .find(|line| line.contains("worker-2"))
            .unwrap();

        assert!(!matching.contains("[MISMATCH]"));
        assert!(mismatching.contains("[MISMATCH]"));
    }

    #[test]
    fn policy_mismatch_filter_hides_matching_blocks_and_files() {
        let mut matching = details(1, vec![Some(address(1))]);
        matching.status.path = "/data/matching".to_string();
        matching.status.storage_policy.storage_type = StorageType::Mem;
        matching.blocks[0].replicas[0].storage_type = StorageType::Mem;

        let mut mixed = details(1, vec![Some(address(2))]);
        mixed.status.path = "/data/mixed".to_string();
        mixed.status.storage_policy.storage_type = StorageType::Mem;
        mixed.blocks[0].replicas[0].storage_type = StorageType::Disk;
        mixed.blocks.push(FileBlockDetail {
            block_id: 8,
            len: 100,
            offset: 100,
            replicas: vec![replica(2, StorageType::Mem, Some(address(2)))],
        });

        let mut report = FsckReport {
            root: "/data".to_string(),
            is_dir: true,
            ..Default::default()
        };
        report.add_file(matching);
        report.add_file(mixed);
        let output = render_report(&report, false, true);

        assert!(!output.contains("File: /data/matching"));
        assert!(output.contains("File: /data/mixed"));
        assert!(output.contains("blk_7"));
        assert!(!output.contains("blk_8"));
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
