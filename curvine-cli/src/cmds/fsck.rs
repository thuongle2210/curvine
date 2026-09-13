use clap::Parser;
use curvine_client_core::file::FsClient;
use curvine_core_error::CommonResult;
use curvine_fs_api::Path;
use curvine_model::{FileBlockDetail, FileBlockDetails, FileStatus, FileType, ListOptions};
use curvine_runtime::common::ByteUnit;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;
use std::sync::Arc;

const LIST_PAGE_SIZE: usize = 256;

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
}

#[derive(Default)]
struct FsckReport {
    root: String,
    is_dir: bool,
    files: Vec<FileScan>,
    storage_placements: BTreeMap<String, usize>,
    worker_placements: BTreeMap<String, usize>,
    block_count: usize,
    mismatch_blocks: usize,
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

            if block_mismatches(&details.status, block) {
                self.mismatch_blocks += 1;
            }
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
                            limit: Some(LIST_PAGE_SIZE),
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

                if entry_count < LIST_PAGE_SIZE {
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

    writeln!(output, "\n--- Findings ---").unwrap();
    writeln!(
        output,
        "  Policy mismatch blocks: {}",
        report.mismatch_blocks
    )
    .unwrap();
    writeln!(
        output,
        "  Under-replicated blocks: {}",
        report.under_replicated_blocks
    )
    .unwrap();
    writeln!(
        output,
        "  Unavailable replicas: {}",
        report.unavailable_replicas
    )
    .unwrap();
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
            writeln!(
                output,
                "  {:<28} {}",
                worker,
                replica.storage_type.as_str_name()
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
            blocks: vec![FileBlockDetail {
                block_id: 7,
                len: 100,
                offset: 0,
                replicas: vec![
                    BlockReplicaDetail {
                        worker_id: 1,
                        storage_type: StorageType::SpdkDisk,
                        address: Some(WorkerAddress {
                            worker_id: 1,
                            hostname: "worker-a".to_string(),
                            rpc_port: 50010,
                            ..Default::default()
                        }),
                    },
                    BlockReplicaDetail {
                        worker_id: 2,
                        storage_type: StorageType::Disk,
                        address: None,
                    },
                ],
            }],
        }
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
    }

    #[test]
    fn policy_mismatch_hides_matching_blocks() {
        let mut details = details();
        details.blocks.push(FileBlockDetail {
            block_id: 8,
            len: 100,
            offset: 100,
            replicas: vec![BlockReplicaDetail {
                worker_id: 1,
                storage_type: StorageType::SpdkDisk,
                address: None,
            }],
        });
        let output = render_file(&FileScan { details }, true, false);
        assert!(output.contains("blk_7"));
        assert!(!output.contains("blk_8"));
    }
}
