use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct NvmeSubsystem {
    pub trtype: String,
    pub traddr: String,
    pub trsvcid: u16,
    pub subnqn: String,
    pub controller_count: u32,
}

/// Controller selection strategy stub
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ControllerSelectionStrategy {
    #[default]
    First,
    RoundRobin(RoundRobinController),
    Random(RandomController),
}

impl ControllerSelectionStrategy {
    pub fn name(&self) -> &str {
        match self {
            ControllerSelectionStrategy::First => "First",
            ControllerSelectionStrategy::RoundRobin(_) => "RoundRobin",
            ControllerSelectionStrategy::Random(_) => "Random",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RoundRobinController {
    // stub
}

impl RoundRobinController {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RandomController {
    // stub
}

impl RandomController {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool,
    pub app_name: String,
    #[serde(alias = "hugepage", default)]
    pub hugepage_str: String,
    pub reactor_mask: String,
    pub shm_id: i32,
    #[serde(default)]
    pub subsystems: Vec<NvmeSubsystem>,
    pub io_queue_depth: u32,
    pub io_queue_requests: u32,
    #[serde(alias = "io_timeout", default)]
    pub io_timeout_str: String,
    pub io_retry_count: u32,
    #[serde(alias = "keep_alive_timeout", default)]
    pub keep_alive_timeout_str: String,
    #[serde(alias = "controller_selection", default)]
    pub controller_selection_str: String,
    #[serde(skip)]
    pub controller_selection: ControllerSelectionStrategy,
    #[serde(alias = "dma_pool_size", default)]
    pub dma_pool_size_str: String,
}

impl SpdkConf {
    pub fn init(&mut self) -> crate::CommonResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BdevInfo {
    pub name: String,
    pub size_bytes: u64,
    pub block_size: u32,
    pub num_blocks: u64,
    pub target_endpoint: String,
    pub ctrlr_idx: u32,
    pub ctrlr: usize,
    pub ns: usize,
    pub nsid: u32,
}
