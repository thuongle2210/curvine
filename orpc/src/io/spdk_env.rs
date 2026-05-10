#![cfg(feature = "spdk")]

use crate::err_msg;
use crate::io::spdk_ffi;
use crate::{err_box, CommonResult};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

// ---------------------------------------------------------------------------
// Reactor State (native reactor only)
// ---------------------------------------------------------------------------

/// Single poller per reactor: processes completions for ALL qpairs on that reactor.
pub struct ReactorState {
    pub thread: *mut spdk_ffi::spdk_thread,
    pub os_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub qpairs: Mutex<Vec<*mut spdk_ffi::spdk_nvme_qpair>>,
    pub poller: *mut spdk_ffi::spdk_poller,
    pub ready: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub reactor_idx: usize,
    /// Controllers attached on this reactor thread (ctrlr_idx -> controller pointer)
    pub controllers: Mutex<HashMap<usize, *mut spdk_ffi::spdk_nvme_ctrlr>>,
    /// Set to true when `shutdown()`'s detach-controllers message has been processed
    /// on this reactor thread. Used to synchronize shutdown ordering.
    pub detach_done: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

unsafe impl Send for ReactorState {}
unsafe impl Sync for ReactorState {}

/// Controller selection strategy for distributing I/O across multiple controllers.
#[derive(Clone, Debug, PartialEq)]
pub enum ControllerSelectionStrategy {
    /// Always use the first controller for a given (subnqn, nsid)
    First,
    /// Round-robin across available controllers for the same (subnqn, nsid).
    RoundRobin(RoundRobinController),
    /// Randomly select a controller for each bdev lookup.
    Random(RandomController),
}

impl Default for ControllerSelectionStrategy {
    fn default() -> Self {
        ControllerSelectionStrategy::First
    }
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

/// Round-robin state: tracks next index per (subnqn, nsid) group.
#[derive(Clone, Debug, PartialEq)]
pub struct RoundRobinController {
    next: HashMap<(String, u32), usize>,
}

impl RoundRobinController {
    pub fn new() -> Self {
        Self {
            next: HashMap::new(),
        }
    }

    /// Select next controller index for the given (subnqn, nsid) group.
    pub fn select(&mut self, key: &(String, u32), group_size: usize) -> usize {
        let entry = self.next.entry(key.clone()).or_insert(0);
        let selected = *entry;
        *entry = (*entry + 1) % group_size;
        selected
    }
}

/// Random selection state: Xorshift64
#[derive(Clone, Debug, PartialEq)]
pub struct RandomController {
    seed: u64,
}

impl RandomController {
    pub fn new() -> Self {
        Self { seed: 42 }
    }

    /// Select a random controller index using Xorshift64 (shift + xor only).
    pub fn select(&mut self, group_size: usize) -> usize {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        (self.seed as usize) % group_size
    }
}

// Qpair pool - reuse NVMe qpairs across handles
/// Lazy allocate, cache on release
pub struct QpairPool {
    inner: Mutex<HashMap<usize, Vec<*mut spdk_ffi::spdk_nvme_qpair>>>,
    max_per_ctrlr: usize,
}
// SAFETY: exclusive ownership
unsafe impl Send for QpairPool {}
unsafe impl Sync for QpairPool {}
impl QpairPool {
    /// Default max idle qpairs per controller
    const DEFAULT_MAX_PER_CTRLR: usize = 16;

    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_per_ctrlr: Self::DEFAULT_MAX_PER_CTRLR,
        }
    }

    /// Acquire qpair - returns cached or allocates new
    fn acquire(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        let key = ctrlr as usize;
        {
            let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(stack) = pool.get_mut(&key) {
                if let Some(qpair) = stack.pop() {
                    log::trace!(
                        "QpairPool: reusing cached qpair for ctrlr {:p} (pool size now {})",
                        ctrlr,
                        stack.len()
                    );
                    return Ok(qpair);
                }
            }
        }
        // No cached qpair — allocate a new one (outside the lock)
        let qpair = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        if qpair.is_null() {
            return err_box!(
                "QpairPool: failed to allocate I/O qpair for ctrlr {:p}. \
                 This may indicate qpair exhaustion under high concurrency. \
                 Check NvmeSubsystem.io_queues configuration.",
                ctrlr
            );
        }
        log::trace!("QpairPool: allocated new qpair for ctrlr {:p}", ctrlr);
        Ok(qpair)
    }
    /// Return qpair to pool for reuse
    fn release(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) {
        let key = ctrlr as usize;
        let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let stack = pool.entry(key).or_default();
        if stack.len() >= self.max_per_ctrlr {
            // Pool full — free immediately to bound controller-side memory.
            drop(pool); // release lock before FFI call
            unsafe { spdk_ffi::curvine_spdk_free_io_qpair(qpair) };
            log::trace!(
                "QpairPool: pool full for ctrlr {:p} (max={}), freed qpair",
                ctrlr,
                self.max_per_ctrlr
            );
        } else {
            stack.push(qpair);
            log::trace!(
                "QpairPool: returned qpair to pool for ctrlr {:p} ({}/{})",
                ctrlr,
                stack.len(),
                self.max_per_ctrlr
            );
        }
    }
    /// Free all pooled qpairs
    fn drain_all(&self) {
        let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut total = 0usize;
        for (_ctrlr_key, qpairs) in pool.drain() {
            for qpair in qpairs {
                unsafe { spdk_ffi::curvine_spdk_free_io_qpair(qpair) };
                total += 1;
            }
        }
        if total > 0 {
            info!("QpairPool: freed {} cached qpair(s) during shutdown", total);
        }
    }
}
// Global singleton

static SPDK_ENV: OnceLock<SpdkEnv> = OnceLock::new();
static INIT_LOCK: Mutex<()> = Mutex::new(());
static REACTOR_STATES: OnceLock<Mutex<Vec<Arc<ReactorState>>>> = OnceLock::new();

/// Register reactor states (called after start_native_reactors completes)
pub fn register_reactor_states(states: Vec<Arc<ReactorState>>) {
    let _ = REACTOR_STATES.set(Mutex::new(states));
}

/// Find the ReactorState that owns a given controller pointer.
/// The controller was attached on that reactor's thread.
pub fn find_reactor_for_controller(
    ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
) -> Option<Arc<ReactorState>> {
    if let Some(states) = REACTOR_STATES.get() {
        let states = states.lock().unwrap();
        for state in states.iter() {
            let controllers = state.controllers.lock().unwrap();
            for (_, stored_ctrlr) in controllers.iter() {
                if *stored_ctrlr == ctrlr {
                    return Some(state.clone());
                }
            }
        }
    }
    None
}

/// Lifecycle state
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpdkEnvState {
    Created = 0,     // config validated, not init
    Initialized = 1, // controllers attached, bdevs available
    /// spdk_env_fini called; no further ops allowed.
    ShutDown = 2,
}

impl From<u8> for SpdkEnvState {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Created,
            1 => Self::Initialized,
            2 => Self::ShutDown,
            _ => Self::Created,
        }
    }
}

impl Display for SpdkEnvState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SpdkEnvState::Created => write!(f, "Created"),
            SpdkEnvState::Initialized => write!(f, "Initialized"),
            SpdkEnvState::ShutDown => write!(f, "ShutDown"),
        }
    }
}

/// NVMe subsystem (remote NVMe-oF subsystem identified by subnqn)
/// NVMe subsystem (remote NVMe-oF subsystem identified by subnqn)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct NvmeSubsystem {
    pub trtype: String,             // "rdma" or "tcp"
    pub adrfam: String,             // "ipv4" or "ipv6"
    pub traddr: String,             // target IP
    pub trsvcid: u16,               // port
    pub subnqn: String,             // NVMe subsystem NQN
    pub hostnqn: String,            // empty = auto-generated
    pub io_queues: u32,             // 0 = default
    pub keep_alive_timeout_ms: u64, // 0 = use global
    pub controller_count: u32,      // Number of controllers for this subsystem (default 1)
    pub namespace_ids: Vec<u32>, // Explicit namespace IDs to use (empty = auto-detect via spdk_nvme_ctrlr_get_num_ns)
}

impl NvmeSubsystem {
    pub fn new(traddr: &str, trsvcid: u16, subnqn: &str) -> Self {
        Self {
            traddr: traddr.to_string(),
            trsvcid,
            subnqn: subnqn.to_string(),
            ..Default::default()
        }
    }

    /// Validate required fields
    pub fn validate(&self) -> CommonResult<()> {
        if self.traddr.is_empty() {
            return err_box!("NvmeSubsystem: traddr cannot be empty");
        }
        if self.trsvcid == 0 {
            return err_box!("NvmeSubsystem: trsvcid cannot be 0");
        }
        if self.subnqn.is_empty() {
            return err_box!("NvmeSubsystem: subnqn cannot be empty");
        }
        let valid_trtype = ["rdma", "tcp"];
        if !valid_trtype.contains(&self.trtype.to_lowercase().as_str()) {
            return err_box!(
                "NvmeSubsystem: invalid trtype '{}', expected one of {:?}",
                self.trtype,
                valid_trtype
            );
        }
        if self.controller_count == 0 {
            return err_box!(
                "NvmeSubsystem: controller_count must be >= 1, got {}",
                self.controller_count
            );
        }
        Ok(())
    }

    /// Format as `trtype:traddr:trsvcid/subnqn`
    pub fn endpoint(&self) -> String {
        format!(
            "{}://{}:{}/{}",
            self.trtype, self.traddr, self.trsvcid, self.subnqn
        )
    }
}

impl Default for NvmeSubsystem {
    fn default() -> Self {
        Self {
            trtype: "rdma".to_string(),
            adrfam: "ipv4".to_string(),
            traddr: String::new(),
            trsvcid: 4420,
            subnqn: String::new(),
            hostnqn: String::new(),
            io_queues: 0,
            keep_alive_timeout_ms: 0,
            controller_count: 1,       // Default: 1 controller per subsystem
            namespace_ids: Vec::new(), // Empty = auto-detect
        }
    }
}

impl Display for NvmeSubsystem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.endpoint())
    }
}

// SPDK configuration

/// SPDK env config - string fields parsed by init()
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool, // master switch - false = skip init
    #[serde(default)]
    pub app_name: String, // SPDK EAL name, e.g. "curvine"
    #[serde(alias = "hugepage", default)]
    pub hugepage_str: String, // e.g. "1024MB"
    #[serde(skip)]
    pub hugepage_mb: u32, // parsed by init()
    #[serde(default)]
    pub reactor_mask: String, // hex, e.g. "0x3"
    #[serde(default)]
    pub shm_id: i32, // -1 = single-process
    #[serde(default)]
    pub mem_channel: u32, // 0 = auto-detect
    #[serde(default)]
    pub subsystems: Vec<NvmeSubsystem>,
    #[serde(default)]
    pub io_queue_depth: u32,
    #[serde(default)]
    pub io_queue_requests: u32,
    #[serde(alias = "io_timeout", default)]
    pub io_timeout_str: String, // e.g. "30s"
    #[serde(skip)]
    pub io_timeout_us: u64, // parsed by init()
    #[serde(default)]
    pub io_retry_count: u32,
    #[serde(alias = "keep_alive_timeout", default)]
    pub keep_alive_timeout_str: String, // e.g. "10s"
    #[serde(skip)]
    pub keep_alive_timeout_ms: u64, // parsed by init()
    #[serde(alias = "poll_interval", default)]
    pub poll_interval_ms: u64, // default = 1000
    #[serde(alias = "controller_selection", default)]
    pub controller_selection_str: String, // "First", "RoundRobin", or "Random"
    #[serde(skip)]
    pub controller_selection: ControllerSelectionStrategy, // parsed by init()
    #[serde(alias = "dma_pool_size", default)]
    pub dma_pool_size_str: String, // e.g. "64MB"
    #[serde(skip)]
    pub dma_pool_bytes: u64, // parsed by init()
    pub block_align: u32, // 0 = auto-detect
    /// Enable async read for SPDK bdev (uses spdk_read_async internally)
    /// When enabled, read_region will use non-blocking I/O with waker-based completion
    #[serde(alias = "spdk_async_read", default)]
    pub spdk_async_read: bool, // default = false
    /// Enable SPDK native reactor instead of custom poller threads.
    /// Deprecated: native reactor is always used when the `spdk` feature is enabled.
    #[deprecated(note = "native reactor is always enabled under the `spdk` feature")]
    #[serde(alias = "spdk_native_reactor", default)]
    pub spdk_native_reactor: bool,
    /// Number of reactor cores (default: all physical cores)
    #[serde(alias = "reactor_core_count", default)]
    pub reactor_core_count: Option<u32>,
}

impl SpdkConf {
    /// Parse string fields into computed values
    pub fn init(&mut self) -> CommonResult<()> {
        use crate::common::{ByteUnit, DurationUnit};

        self.hugepage_mb =
            (ByteUnit::from_str(&self.hugepage_str)?.as_byte() / ByteUnit::MB) as u32;

        let io_timeout = DurationUnit::from_str(&self.io_timeout_str)?;
        self.io_timeout_us = io_timeout.as_millis() * 1000;

        let keep_alive = DurationUnit::from_str(&self.keep_alive_timeout_str)?;
        self.keep_alive_timeout_ms = keep_alive.as_millis();

        let dma_pool = ByteUnit::from_str(&self.dma_pool_size_str)?;
        self.dma_pool_bytes = dma_pool.as_byte();

        // Parse controller selection strategy
        self.controller_selection = match self.controller_selection_str.to_lowercase().as_str() {
            "roundrobin" | "round_robin" | "round-robin" => {
                ControllerSelectionStrategy::RoundRobin(RoundRobinController::new())
            }
            "random" => ControllerSelectionStrategy::Random(RandomController::new()),
            "first" | "" => ControllerSelectionStrategy::First,
            other => {
                return err_box!(
                    "SpdkConf: invalid controller_selection '{}', expected 'First', 'RoundRobin', or 'Random'",
                    other
                );
            }
        };

        Ok(())
    }

    /// Validate config
    pub fn validate(&self) -> CommonResult<()> {
        use crate::common::ByteUnit;
        if !self.enabled {
            return Ok(());
        }

        // Allow empty subsystems for tests that only need EAL + thread library
        // (e.g., DMA buffer allocation tests that don't require NVMe connections)
        if !self.subsystems.is_empty() {
            // Validate reactor_mask is valid hex
            let mask = self
                .reactor_mask
                .trim_start_matches("0x")
                .trim_start_matches("0X");
            if u64::from_str_radix(mask, 16).is_err() {
                return err_box!(
                    "SpdkConf: invalid reactor_mask '{}', expected hex (e.g. '0x3')",
                    self.reactor_mask
                );
            }

            // Validate hugepage
            let hugepage_mb = if self.hugepage_mb > 0 {
                self.hugepage_mb
            } else {
                // Parse from source string as fallback
                match ByteUnit::from_str(&self.hugepage_str) {
                    Ok(v) => (v.as_byte() / ByteUnit::MB) as u32,
                    Err(_) => {
                        return err_box!(
                            "SpdkConf: invalid hugepage '{}', expected e.g. '1024MB'",
                            self.hugepage_str
                        );
                    }
                }
            };

            if hugepage_mb == 0 {
                return err_box!(
                    "SpdkConf: hugepage must be > 0 (got '{}')",
                    self.hugepage_str
                );
            }

            // Validate I/O parameters
            if self.io_queue_depth == 0 {
                return err_box!("SpdkConf: io_queue_depth must be > 0");
            }

            if self.io_queue_requests < self.io_queue_depth {
                return err_box!(
                    "SpdkConf: io_queue_requests ({}) must be >= io_queue_depth ({})",
                    self.io_queue_requests,
                    self.io_queue_depth
                );
            }

            // Validate subsystems
            for (i, subsystem) in self.subsystems.iter().enumerate() {
                subsystem.validate().map_err(|e| {
                    let msg = format!("SpdkConf: subsystems[{}]: {}", i, e);
                    err_msg!(msg)
                })?;
                if subsystem.keep_alive_timeout_ms > 0
                    && subsystem.keep_alive_timeout_ms < self.poll_interval_ms
                {
                    return err_box!(
                        "SpdkConf: subsystems[{}]: keep_alive_timeout_ms ({}) must be >= poll_interval_ms ({})",
                        i,
                        subsystem.keep_alive_timeout_ms,
                        self.poll_interval_ms
                    );
                }
            }

            // Validate poller interval
            if self.poll_interval_ms == 0 {
                return err_box!("SpdkConf: poll_interval_ms must be > 0");
            }
            if self.poll_interval_ms > i32::MAX as u64 {
                return err_box!(
                    "SpdkConf: poll_interval_ms ({}) exceeds i32::MAX ({})",
                    self.poll_interval_ms,
                    i32::MAX
                );
            }
        } // end: if !self.subsystems.is_empty()

        Ok(())
    }
}

impl Default for SpdkConf {
    fn default() -> Self {
        Self {
            enabled: false,
            app_name: "curvine".to_string(),
            hugepage_str: "1024MB".to_string(),
            hugepage_mb: 1024,
            reactor_mask: "0x1".to_string(),
            shm_id: -1,
            mem_channel: 0,
            subsystems: vec![],
            io_queue_depth: 128,
            io_queue_requests: 512,
            io_timeout_str: "30s".to_string(),
            io_timeout_us: 30_000_000,
            io_retry_count: 4,
            keep_alive_timeout_str: "10s".to_string(),
            keep_alive_timeout_ms: 10_000,
            poll_interval_ms: 1000,
            controller_selection_str: "First".to_string(),
            controller_selection: ControllerSelectionStrategy::First,
            dma_pool_size_str: "64MB".to_string(),
            dma_pool_bytes: 64 * 1024 * 1024,
            block_align: 0,
            spdk_async_read: false,
            spdk_native_reactor: false,
            reactor_core_count: None,
        }
    }
}

impl Display for SpdkConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkConf(enabled={}, hugepage={}MB, reactor_mask={}, subsystems={})",
            self.enabled,
            self.hugepage_mb,
            self.reactor_mask,
            self.subsystems.len()
        )
    }
}

// Bdev descriptor (discovered after init)

/// Discovered SPDK block device
#[derive(Debug, Clone)]
pub struct BdevInfo {
    pub name: String,    // e.g. "NVMe0n1"
    pub size_bytes: u64, // total size in bytes
    pub block_size: u32, // typically 512 or 4096
    pub num_blocks: u64,
    pub target_endpoint: String, // trtype://traddr:trsvcid/subnqn
    pub ctrlr: usize,            // raw pointer for SpdkBdev
    pub ns: usize,               // raw pointer for SpdkBdev
    pub nsid: u32,               // namespace ID (1-based)
    pub ctrlr_idx: u32,          // controller index (0-based, for multi-controller)
    pub reactor_idx: usize,      // Which reactor thread owns this controller
}

impl Display for BdevInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BdevInfo(name={}, size={}B, block_size={}, target={})",
            self.name, self.size_bytes, self.block_size, self.target_endpoint
        )
    }
}

// SpdkEnv - the core struct

/// SPDK env lifecycle (singleton). Send + Sync
pub struct SpdkEnv {
    conf: SpdkConf,
    state: AtomicU8,
    bdevs: Vec<BdevInfo>, // populated during init(), immutable after
    /// Group bdevs by (subnqn, nsid) for controller selection
    bdev_groups: HashMap<(String, u32), Vec<usize>>,
    open_handles: AtomicUsize,
    qpair_pool: QpairPool,
    /// Controller selection strategy state
    controller_selection: Mutex<ControllerSelectionStrategy>,
    /// SPDK native reactor threads + single poller per reactor (round-robin mapping)
    reactor_states: Mutex<Vec<Arc<ReactorState>>>,
    next_reactor_idx: Mutex<usize>,
}

// SAFETY: Fields are either immutable after init (conf, bdevs) or atomic (state).
unsafe impl Send for SpdkEnv {}
unsafe impl Sync for SpdkEnv {}

impl SpdkEnv {
    /// Create in Created state
    pub fn new(conf: SpdkConf) -> CommonResult<Self> {
        conf.validate()?;

        info!("SpdkEnv created: {}", conf);

        Ok(Self {
            conf: conf.clone(),
            state: AtomicU8::new(SpdkEnvState::Created as u8),
            bdevs: Vec::new(),
            bdev_groups: HashMap::new(),
            open_handles: AtomicUsize::new(0),
            qpair_pool: QpairPool::new(),
            controller_selection: Mutex::new(conf.controller_selection.clone()),
            reactor_states: Mutex::new(Vec::new()),
            next_reactor_idx: Mutex::new(0),
        })
    }

    // Global singleton access

    /// Init global SPDK (OnceLock + Mutex)
    pub fn init_global(conf: SpdkConf) -> CommonResult<&'static SpdkEnv> {
        // Fast path: already initialized
        if let Some(env) = SPDK_ENV.get() {
            if env.is_initialized() {
                return Ok(env);
            }
        }

        // Slow path: acquire lock
        let _lock = match INIT_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("SPDK init lock was poisoned (previous panic), recovering");
                poisoned.into_inner()
            }
        };

        // Double-check after acquiring lock
        if let Some(env) = SPDK_ENV.get() {
            if env.is_initialized() {
                return Ok(env);
            }
        }

        info!("Initializing global SPDK environment: {}", conf);

        let mut env = Self::new(conf)?;
        env.init()?;

        SPDK_ENV.set(env).map_err(|_| {
            err_msg!("Failed to store SpdkEnv instance (concurrent initialization conflict)")
        })?;

        Ok(SPDK_ENV.get().expect("SpdkEnv was just initialized"))
    }

    /// Get global SPDK if initialized
    pub fn global() -> Option<&'static SpdkEnv> {
        SPDK_ENV.get().filter(|env| env.is_initialized())
    }

    /// Get global SPDK regardless of state
    pub fn global_including_shutdown() -> Option<&'static SpdkEnv> {
        SPDK_ENV.get()
    }

    /// Get the global SPDK environment, or return an error.
    pub fn global_or_err() -> CommonResult<&'static SpdkEnv> {
        Self::global().ok_or_else(|| err_msg!("SPDK environment not initialized").into())
    }

    /// Shut down the global SPDK environment singleton.
    /// Safe to call multiple times or when not initialized (no-ops).
    pub fn shutdown_global() {
        if let Some(env) = SPDK_ENV.get() {
            env.shutdown();
        }
    }

    // Initialization (instance-level)

    /// Init SPDK: allocate hugepages, start reactors, attach controllers, discover bdevs.
    pub fn init(&mut self) -> CommonResult<()> {
        let current = self.state();
        if current != SpdkEnvState::Created {
            return err_box!(
                "SpdkEnv::init() called in invalid state: {} (expected Created)",
                current
            );
        }

        info!(
            "SPDK env init: app_name={}, hugepage={}MB, reactor_mask={}, subsystems={}, controller_selection={}",
            self.conf.app_name,
            self.conf.hugepage_mb,
            self.conf.reactor_mask,
            self.conf.subsystems.len(),
            self.conf.controller_selection.name()
        );

        // Phase 1: Initialize SPDK environment (hugepages, DPDK, reactors)
        self.env_init()?;

        // Phase 2: Attach NVMe-oF controllers and discover bdevs on reactor threads
        let all_bdevs = self.start_native_reactors()?;

        // Build bdev_groups from discovered bdevs
        let mut bdev_groups: HashMap<(String, u32), Vec<usize>> = HashMap::new();
        for (idx, bdev) in all_bdevs.iter().enumerate() {
            if let Some(subnqn) = bdev.target_endpoint.split('/').nth(3) {
                let key = (subnqn.to_string(), bdev.nsid);
                bdev_groups.entry(key).or_default().push(idx);
            }
        }

        info!(
            "SPDK env initialized: {} bdev(s) from {} target(s), {} controller group(s)",
            all_bdevs.len(),
            self.conf.subsystems.len(),
            bdev_groups.len()
        );

        info!("Discovered bdevs:");
        for (idx, bdev) in all_bdevs.iter().enumerate() {
            info!(
                "  bdev[{}]: name={}, ctrlr_idx={}, reactor_idx={}",
                idx, bdev.name, bdev.ctrlr_idx, bdev.reactor_idx
            );
        }

        self.bdevs = all_bdevs;
        self.bdev_groups = bdev_groups;

        self.state
            .store(SpdkEnvState::Initialized as u8, Ordering::Release);

        Ok(())
    }

    /// Start native reactors matching C demo pattern:
    /// 1. Create SPDK threads
    /// 2. Spawn OS threads that:
    ///    a. Call spdk_set_thread()
    ///    b. Attach controllers (ALL on reactor thread)
    ///    c. Register poller
    ///    d. Run reactor loop via curvine_spdk_run_reactor_loop()
    fn start_native_reactors(&mut self) -> CommonResult<Vec<BdevInfo>> {
        use std::ffi::CString;
        use std::sync::Arc;

        info!(
            "Starting SPDK native reactors: hugepage_mb={}, reactor_mask={}",
            self.conf.hugepage_mb, self.conf.reactor_mask
        );

        unsafe { spdk_ffi::curvine_check_eal_memory() };

        info!("Calling spdk_thread_lib_init...");
        let mut rc = -12;
        for attempt in 1..=3 {
            info!("spdk_thread_lib_init attempt {}/3", attempt);
            rc = unsafe { spdk_ffi::curvine_spdk_thread_lib_init(None, 0) };
            if rc == 0 {
                break;
            }
            error!(
                "spdk_thread_lib_init failed: rc={} (attempt {})",
                rc, attempt
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if rc != 0 {
            return err_box!(
                "Failed to initialize SPDK thread library: rc={} (check EAL memory)",
                rc
            );
        }
        info!("SPDK thread library initialized successfully");

        let core_count = self
            .conf
            .reactor_core_count
            .unwrap_or_else(|| num_cpus::get() as u32) as usize;

        info!("Starting {} SPDK native reactor threads", core_count);

        // Distribute subsystems across reactor threads (C demo pattern: each reactor owns its controllers)
        // Simple round-robin: subsystem[i] -> reactor[i % core_count]
        let subsystems = self.conf.subsystems.clone();
        let subsystems_per_reactor: Vec<Vec<NvmeSubsystem>> = {
            let mut map: Vec<Vec<NvmeSubsystem>> = (0..core_count).map(|_| Vec::new()).collect();
            for (idx, subsystem) in subsystems.iter().enumerate() {
                let reactor_idx = idx % core_count;
                map[reactor_idx].push(subsystem.clone());
            }
            map
        };

        // Shared vector to collect bdevs from all reactor threads
        let all_bdevs = Arc::new(Mutex::new(Vec::new()));

        let mut reactor_states: Vec<Arc<ReactorState>> = Vec::new();

        for core_idx in 0..core_count {
            let thread_name = CString::new(format!("spdk-reactor-{}", core_idx))
                .map_err(|e| err_msg!(format!("invalid thread name: {}", e)))?;

            let thread =
                unsafe { spdk_ffi::spdk_thread_create(thread_name.as_ptr(), std::ptr::null()) };
            if thread.is_null() {
                for state in &reactor_states {
                    unsafe { spdk_ffi::spdk_thread_exit(state.thread) };
                }
                unsafe {
                    spdk_ffi::spdk_thread_lib_fini();
                }
                return err_box!("Failed to create SPDK thread for core {}", core_idx);
            }

            let ready_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let all_bdevs_clone = all_bdevs.clone();
            let reactor_subsystems = subsystems_per_reactor[core_idx].clone();
            let reactor_idx = core_idx;

            let state = Arc::new(ReactorState {
                thread,
                os_thread: Mutex::new(None),
                qpairs: Mutex::new(Vec::new()),
                poller: std::ptr::null_mut(),
                ready: ready_flag.clone(),
                controllers: Mutex::new(HashMap::new()),
                reactor_idx,
                detach_done: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            });

            // Spawn OS thread to run reactor loop (matching C demo pattern)
            let state_clone = state.clone();
            let os_thread = std::thread::spawn(move || {
                info!("[Reactor-{}] OS thread started", reactor_idx);

                unsafe {
                    // C demo pattern: spdk_set_thread() FIRST
                    spdk_ffi::spdk_set_thread(state_clone.thread);
                    info!("[Reactor-{}] SPDK thread set", reactor_idx);

                    // Attach ONLY the subsystems assigned to THIS reactor thread
                    let mut thread_bdevs = Vec::new();
                    for subsystem in &reactor_subsystems {
                        info!(
                            "[Reactor-{}] Attaching subsystem: {} (ctrlrs={})",
                            reactor_idx,
                            subsystem.endpoint(),
                            subsystem.controller_count
                        );
                        match Self::attach_controllers_on_thread(
                            subsystem,
                            reactor_idx,
                            &state_clone,
                        ) {
                            Ok(bdevs) => {
                                info!(
                                    "[Reactor-{}] Attached subsystem {}: {} bdev(s)",
                                    reactor_idx,
                                    subsystem.endpoint(),
                                    bdevs.len()
                                );
                                thread_bdevs.extend(bdevs);
                            }
                            Err(e) => {
                                error!(
                                    "[Reactor-{}] Failed to attach subsystem {}: {}",
                                    reactor_idx,
                                    subsystem.endpoint(),
                                    e
                                );
                            }
                        }
                    }

                    // Store discovered bdevs in shared vector with reactor_idx
                    if !thread_bdevs.is_empty() {
                        let mut shared_bdevs = all_bdevs_clone.lock().unwrap();
                        shared_bdevs.extend(thread_bdevs);
                    }

                    // Register poller on this thread (BEFORE signaling ready)
                    // IMPORTANT: pass pointer to SAME state that holds controllers and qpairs
                    // Use Arc::as_ptr() to get the data pointer (stable Rust 1.92+)
                    // This way poller and controller storage point to same ReactorState
                    let state_ptr: *mut ReactorState =
                        Arc::as_ptr(&state_clone) as *mut ReactorState;

                    let poller = spdk_ffi::spdk_poller_register(
                        crate::io::spdk_poller::reactor_poller_cb,
                        state_ptr as *mut c_void,
                        0,
                    );
                    if poller.is_null() {
                        error!("[Reactor-{}] Failed to register poller", reactor_idx);
                    } else {
                        info!(
                            "[Reactor-{}] Poller registered at {:?} with state_ptr={:?}",
                            reactor_idx, poller, state_ptr
                        );
                    }

                    // Now signal that thread is ready (controllers attached AND poller registered)
                    state_clone
                        .ready
                        .store(true, std::sync::atomic::Ordering::Release);

                    // Run reactor loop (C demo pattern: spdk_thread_poll in loop)
                    info!("[Reactor-{}] Entering reactor loop", reactor_idx);
                    spdk_ffi::curvine_spdk_run_reactor_loop(state_clone.thread);
                    info!("[Reactor-{}] Exited reactor loop", reactor_idx);

                    // Cleanup: unregister poller
                    if !poller.is_null() {
                        spdk_ffi::spdk_poller_unregister(poller);
                    }

                    // Destroy SPDK thread (required before spdk_thread_lib_fini)
                    spdk_ffi::spdk_thread_destroy(state_clone.thread);
                    info!("[Reactor-{}] SPDK thread destroyed", reactor_idx);
                }

                info!("[Reactor-{}] OS thread ended", reactor_idx);
            });

            // Store OS thread handle
            {
                let mut thread_guard = state.os_thread.lock().unwrap();
                *thread_guard = Some(os_thread);
            }

            reactor_states.push(state);
        }

        // Wait for all reactor threads to be ready
        for (idx, state) in reactor_states.iter().enumerate() {
            let mut attempts = 0;
            while !state.ready.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(10));
                attempts += 1;
                if attempts > 1000 {
                    // Increase timeout for controller attachment
                    error!("[Reactor-{}] Timed out waiting for thread to be ready", idx);
                    break;
                }
            }
        }

        // Collect all bdevs from shared vector
        let all_bdevs = std::mem::take(&mut *all_bdevs.lock().unwrap());

        if all_bdevs.is_empty() && !self.conf.subsystems.is_empty() {
            // Send self-exit message to each reactor thread
            for state in &reactor_states {
                unsafe {
                    let rc = spdk_ffi::spdk_thread_send_msg(
                        state.thread,
                        spdk_ffi::curvine_reactor_exit_handler as spdk_ffi::spdk_thread_fn,
                        state.thread as *mut std::ffi::c_void,
                    );
                    if rc != 0 {
                        error!(
                            "Failed to send exit message to reactor thread {} (thread={:p}), rc={}",
                            state.reactor_idx, state.thread, rc
                        );
                    }
                }
            }
            // Wait for OS threads to finish
            for state in &reactor_states {
                let mut thread_guard = state.os_thread.lock().unwrap();
                if let Some(handle) = thread_guard.take() {
                    let _ = handle.join();
                }
            }
            unsafe {
                spdk_ffi::spdk_thread_lib_fini();
            }

            return err_box!(
                "SpdkEnv::start_native_reactors() failed: no bdevs discovered from {} target(s)",
                self.conf.subsystems.len()
            );
        }

        *self.reactor_states.lock().unwrap() = reactor_states;
        *self.next_reactor_idx.lock().unwrap() = 0;

        // Register in global for reactor thread access
        register_reactor_states(self.reactor_states.lock().unwrap().clone());

        info!(
            "SPDK native reactors started: {} reactor(s), {} bdev(s)",
            self.reactor_states.lock().unwrap().len(),
            all_bdevs.len()
        );

        Ok(all_bdevs)
    }

    /// Attach controllers on the current thread (must be called with spdk_set_thread already called)
    /// Returns discovered bdevs and stores controllers in reactor state
    fn attach_controllers_on_thread(
        subsystem: &NvmeSubsystem,
        reactor_idx: usize,
        state: &Arc<ReactorState>,
    ) -> CommonResult<Vec<BdevInfo>> {
        let mut all_bdevs = Vec::new();

        for ctrlr_idx in 0..subsystem.controller_count {
            info!(
                "[Reactor-{}] Attaching controller {}/{} for subsystem {}",
                reactor_idx,
                ctrlr_idx + 1,
                subsystem.controller_count,
                subsystem.endpoint()
            );

            let bdevs =
                Self::attach_single_controller_on_thread(subsystem, ctrlr_idx, reactor_idx, state)?;
            all_bdevs.extend(bdevs);
        }

        Ok(all_bdevs)
    }

    /// Attach a single controller on the current thread
    fn attach_single_controller_on_thread(
        subsystem: &NvmeSubsystem,
        ctrlr_idx: u32,
        reactor_idx: usize,
        state: &Arc<ReactorState>,
    ) -> CommonResult<Vec<BdevInfo>> {
        use std::ffi::CString;
        let traddr = CString::new(subsystem.traddr.as_str())
            .map_err(|e| err_msg!(format!("invalid traddr: {}", e)))?;
        let trsvcid = CString::new(subsystem.trsvcid.to_string())
            .map_err(|e| err_msg!(format!("invalid trsvcid: {}", e)))?;
        let subnqn = CString::new(subsystem.subnqn.as_str())
            .map_err(|e| err_msg!(format!("invalid subnqn: {}", e)))?;
        let trtype = match subsystem.trtype.to_lowercase().as_str() {
            "rdma" => spdk_ffi::SPDK_NVME_TRANSPORT_RDMA,
            "tcp" => spdk_ffi::SPDK_NVME_TRANSPORT_TCP,
            _ => return err_box!("unsupported transport type: {}", subsystem.trtype),
        };
        let adrfam = match subsystem.adrfam.to_lowercase().as_str() {
            "ipv4" => spdk_ffi::SPDK_NVMF_ADRFAM_IPV4,
            "ipv6" => spdk_ffi::SPDK_NVMF_ADRFAM_IPV6,
            _ => return err_box!("unsupported address family: {}", subsystem.adrfam),
        };

        unsafe {
            // Verify our opaque buffers are large enough for the installed SPDK version.
            let trid_real = spdk_ffi::curvine_spdk_trid_sizeof();
            let trid_buf = std::mem::size_of::<spdk_ffi::spdk_nvme_transport_id>();
            if trid_real > trid_buf {
                return err_box!(
                    "spdk_nvme_transport_id is {} bytes but our buffer is only {}",
                    trid_real,
                    trid_buf
                );
            }
            let opts_real = spdk_ffi::curvine_spdk_ctrlr_opts_sizeof();
            let opts_buf = std::mem::size_of::<spdk_ffi::spdk_nvme_ctrlr_opts>();
            if opts_real > opts_buf {
                return err_box!(
                    "spdk_nvme_ctrlr_opts is {} bytes but our buffer is only {}",
                    opts_real,
                    opts_buf
                );
            }

            // Build transport ID via C helpers
            let mut trid: spdk_ffi::spdk_nvme_transport_id = std::mem::zeroed();
            spdk_ffi::curvine_spdk_trid_set_trtype(&mut trid, trtype);
            spdk_ffi::curvine_spdk_trid_set_adrfam(&mut trid, adrfam);
            spdk_ffi::curvine_spdk_trid_set_traddr(&mut trid, traddr.as_ptr());
            spdk_ffi::curvine_spdk_trid_set_trsvcid(&mut trid, trsvcid.as_ptr());
            spdk_ffi::curvine_spdk_trid_set_subnqn(&mut trid, subnqn.as_ptr());

            // Build controller opts via C helpers
            let mut opts: spdk_ffi::spdk_nvme_ctrlr_opts = std::mem::zeroed();
            spdk_ffi::curvine_spdk_ctrlr_get_default_opts(&mut opts);
            if subsystem.io_queues > 0 {
                spdk_ffi::curvine_spdk_ctrlr_opts_set_num_io_queues(&mut opts, subsystem.io_queues);
            }
            if subsystem.keep_alive_timeout_ms > 0 {
                spdk_ffi::curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(
                    &mut opts,
                    subsystem.keep_alive_timeout_ms as u32,
                );
            }
            if !subsystem.hostnqn.is_empty() {
                let hostnqn = CString::new(subsystem.hostnqn.as_str())
                    .map_err(|e| err_msg!(format!("invalid hostnqn: {}", e)))?;
                spdk_ffi::curvine_spdk_ctrlr_opts_set_hostnqn(&mut opts, hostnqn.as_ptr());
            }

            // Connect - must be on reactor thread with spdk_set_thread called
            let ctrlr = spdk_ffi::curvine_spdk_nvme_connect(&mut trid, &mut opts);
            if ctrlr.is_null() {
                return err_box!(
                    "spdk_nvme_connect failed for {} (controller {})",
                    subsystem.endpoint(),
                    ctrlr_idx
                );
            }

            info!(
                "[Reactor-{}] Controller connected: {:?} for subsystem {}",
                reactor_idx,
                ctrlr,
                subsystem.endpoint()
            );

            // Store controller in reactor state with reactor_idx
            {
                let mut controllers = state.controllers.lock().unwrap();
                controllers.insert(ctrlr_idx as usize, ctrlr);
            }

            // Enumerate active namespaces
            let nsids: Vec<u32> = if !subsystem.namespace_ids.is_empty() {
                subsystem.namespace_ids.clone()
            } else {
                let num_ns = spdk_ffi::spdk_nvme_ctrlr_get_num_ns(ctrlr);
                (1..=num_ns).collect()
            };

            let mut bdevs = Vec::new();
            for nsid in nsids {
                let ns = spdk_ffi::spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
                if ns.is_null() {
                    warn!(
                        "  Namespace {} not found on controller {}, skipping",
                        nsid, ctrlr_idx
                    );
                    continue;
                }
                if !spdk_ffi::spdk_nvme_ns_is_active(ns) {
                    warn!(
                        "  Namespace {} not active on controller {}, skipping",
                        nsid, ctrlr_idx
                    );
                    continue;
                }
                let sector_size = spdk_ffi::spdk_nvme_ns_get_sector_size(ns);
                let num_sectors = spdk_ffi::spdk_nvme_ns_get_num_sectors(ns);
                let size_bytes = sector_size as u64 * num_sectors;
                let name = format!(
                    "NVMe_{}_{}_c{}_n{}",
                    subsystem.traddr, subsystem.trsvcid, ctrlr_idx, nsid
                );
                info!(
                    "  Discovered ns {} on controller {} (reactor-{}): size={}B, sector_size={}, sectors={}",
                    nsid, ctrlr_idx, reactor_idx, size_bytes, sector_size, num_sectors
                );
                bdevs.push(BdevInfo {
                    name,
                    size_bytes,
                    block_size: sector_size,
                    num_blocks: num_sectors,
                    target_endpoint: subsystem.endpoint(),
                    ctrlr: ctrlr as usize,
                    ns: ns as usize,
                    nsid,
                    ctrlr_idx,
                    reactor_idx,
                });
            }

            if bdevs.is_empty() {
                warn!(
                    "Controller {} has no active namespaces, detaching controller",
                    subsystem.endpoint()
                );
                spdk_ffi::spdk_nvme_detach(ctrlr);
            }

            Ok(bdevs)
        }
    }

    // Shutdown

    /// Maximum time `shutdown()` will wait for in-flight handles to close.
    const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Shutdown: detach controllers, free hugepages
    pub fn shutdown(&self) {
        // CAS Initialized => ShutDown, reject new opens after transition
        let prev = self.state.compare_exchange(
            SpdkEnvState::Initialized as u8,
            SpdkEnvState::ShutDown as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if prev.is_err() {
            warn!(
                "SpdkEnv::shutdown() called in state: {} (expected Initialized)",
                self.state()
            );
            return;
        }

        info!(
            "Shutting down SPDK environment ({} bdevs)",
            self.bdevs.len()
        );

        // Wait for in-flight handles to close (acquire_handle checks state).
        let deadline = std::time::Instant::now() + Self::SHUTDOWN_DRAIN_TIMEOUT;
        let mut logged = false;
        loop {
            let count = self.open_handles.load(Ordering::Acquire);
            if count == 0 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                error!(
                    "Timed out waiting for {} SpdkBdev handle(s) to close after {}s. \
                     Skipping controller detach and env_fini to avoid use-after-free. \
                     Resources will be leaked — this indicates a shutdown ordering bug.",
                    count,
                    Self::SHUTDOWN_DRAIN_TIMEOUT.as_secs()
                );
                return;
            }
            if !logged {
                info!(
                    "Waiting for {} outstanding SpdkBdev handle(s) to close before detach",
                    count
                );
                logged = true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Phase 1: detach all controllers on reactor threads
        // This must happen BEFORE thread exit so keep-alive is stopped cleanly.
        let states = self.reactor_states.lock().unwrap();
        for state in states.iter() {
            state
                .detach_done
                .store(false, std::sync::atomic::Ordering::Release);
            let state_ptr = Arc::as_ptr(state) as *mut c_void;
            unsafe {
                let rc = spdk_ffi::spdk_thread_send_msg(
                    state.thread,
                    detach_reactor_controllers as spdk_ffi::spdk_thread_fn,
                    state_ptr,
                );
                if rc != 0 {
                    error!(
                        "Failed to send detach message to reactor thread {} (thread={:p}), rc={}",
                        state.reactor_idx, state.thread, rc
                    );
                    // Mark done anyway so shutdown can proceed
                    state
                        .detach_done
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
        // Wait for all reactor threads to finish detach
        let detach_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        for state in states.iter() {
            while !state.detach_done.load(std::sync::atomic::Ordering::Acquire) {
                if std::time::Instant::now() >= detach_deadline {
                    error!(
                        "Timed out waiting for reactor thread {} to detach controllers",
                        state.reactor_idx
                    );
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        info!("All NVMe controllers detached");

        // Phase 2: send exit message to each reactor thread
        for state in states.iter() {
            unsafe {
                let rc = spdk_ffi::spdk_thread_send_msg(
                    state.thread,
                    spdk_ffi::curvine_reactor_exit_handler as spdk_ffi::spdk_thread_fn,
                    state.thread as *mut std::ffi::c_void,
                );
                if rc != 0 {
                    error!(
                        "Failed to send exit message to reactor thread {} (thread={:p}), rc={}. \
                         Shutdown may hang.",
                        state.reactor_idx, state.thread, rc
                    );
                }
            }
        }
        // Wait for all OS threads to finish
        for state in states.iter() {
            let mut thread_guard = state.os_thread.lock().unwrap();
            if let Some(handle) = thread_guard.take() {
                let _ = handle.join();
            }
        }
        info!("SPDK native reactor threads stopped");

        // Now that controllers are detached and reactor threads are stopped,
        // drain the qpair pool and finalize the thread library.
        self.qpair_pool.drain_all();

        unsafe {
            spdk_ffi::spdk_thread_lib_fini();
        }
        info!("SPDK thread library finalized");

        self.env_fini(); // release hugepages, cleanup DPDK EAL

        info!("SPDK environment shut down successfully");
    }

    /// Current lifecycle state.
    pub fn state(&self) -> SpdkEnvState {
        SpdkEnvState::from(self.state.load(Ordering::Acquire))
    }

    /// Whether the environment is initialized and ready for I/O.
    pub fn is_initialized(&self) -> bool {
        self.state() == SpdkEnvState::Initialized
    }

    /// The validated configuration.
    pub fn conf(&self) -> &SpdkConf {
        &self.conf
    }

    /// Number of configured subsystems.
    pub fn subsystem_count(&self) -> usize {
        self.conf.subsystems.len()
    }

    /// Names of all discovered bdevs.
    pub fn bdev_names(&self) -> Vec<String> {
        self.bdevs.iter().map(|b| b.name.clone()).collect()
    }

    /// All discovered bdev metadata.
    pub fn bdevs(&self) -> &[BdevInfo] {
        &self.bdevs
    }

    /// Look up a bdev by name.
    pub fn get_bdev(&self, name: &str) -> Option<&BdevInfo> {
        self.bdevs.iter().find(|b| b.name == name)
    }

    /// Look up a bdev by subsystem NQN and namespace ID.
    /// Uses controller selection strategy to pick among multiple controllers.
    /// Returns None if not found.
    pub fn get_bdev_by_nsid(&self, subnqn: &str, nsid: u32) -> Option<&BdevInfo> {
        let key = (subnqn.to_string(), nsid);
        let indices = self.bdev_groups.get(&key)?;

        if indices.is_empty() {
            return None;
        }

        let selected_idx = match &mut *self.controller_selection.lock().unwrap() {
            ControllerSelectionStrategy::First => 0,
            ControllerSelectionStrategy::RoundRobin(rr) => rr.select(&key, indices.len()),
            ControllerSelectionStrategy::Random(rnd) => rnd.select(indices.len()),
        };

        let idx = indices[selected_idx % indices.len()];
        Some(&self.bdevs[idx])
    }

    /// Check if SPDK async read is enabled in config.
    pub fn spdk_async_read_enabled(&self) -> bool {
        self.conf.spdk_async_read
    }

    /// Look up a bdev by subsystem NQN, namespace ID, and specific controller index.
    /// Returns None if not found.
    pub fn get_bdev_by_nsid_and_ctrlr(
        &self,
        subnqn: &str,
        nsid: u32,
        ctrlr_idx: u32,
    ) -> Option<&BdevInfo> {
        let key = (subnqn.to_string(), nsid);
        let indices = self.bdev_groups.get(&key)?;

        // Find bdev with matching ctrlr_idx
        for &idx in indices {
            if self.bdevs[idx].ctrlr_idx == ctrlr_idx {
                return Some(&self.bdevs[idx]);
            }
        }

        None
    }

    /// Validate that all expected namespaces (from worker data_dir config) are present.
    /// Called after init() to detect namespace mismatches.
    /// Returns error listing missing (subnqn, nsid) pairs.
    pub fn validate_namespaces(&self, expected: &[(String, u32)]) -> CommonResult<()> {
        let mut missing = Vec::new();
        for (subnqn, nsid) in expected {
            if self.get_bdev_by_nsid(subnqn, *nsid).is_none() {
                missing.push(format!("{}:{}", subnqn, nsid));
            }
        }
        if !missing.is_empty() {
            return err_box!(
                "SpdkEnv: namespaces not found: [{}]. \
                 Verify SPDK target namespaces match datadir config. \
                 Discovered bdevs: [{}]",
                missing.join(", "),
                self.bdevs
                    .iter()
                    .map(|b| format!("{}:{}", b.target_endpoint, b.nsid))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        Ok(())
    }

    /// Total capacity across all bdevs, in bytes.
    pub fn total_capacity(&self) -> u64 {
        self.bdevs.iter().map(|b| b.size_bytes).sum()
    }

    /// Number of live `SpdkBdev` handles.
    pub fn open_handle_count(&self) -> usize {
        self.open_handles.load(Ordering::Acquire)
    }
    /// Number of idle qpairs currently cached in the pool.
    pub fn pooled_qpair_count(&self) -> usize {
        let pool = self
            .qpair_pool
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        pool.values().map(|v| v.len()).sum()
    }

    // Handle tracking, which is used by SpdkBdev open/drop
    /// Register SpdkBdev handle. Returns Err if not Initialized.
    pub fn acquire_handle(&self) -> CommonResult<()> {
        // Increment first so shutdown() sees us in the drain loop.
        self.open_handles.fetch_add(1, Ordering::AcqRel);
        // Now verify the state — if shutdown already won the CAS, undo.
        if self.state() != SpdkEnvState::Initialized {
            self.open_handles.fetch_sub(1, Ordering::AcqRel);
            return err_box!(
                "Cannot open SpdkBdev: SPDK environment is in state {} (expected Initialized)",
                self.state()
            );
        }
        Ok(())
    }
    /// Unregister SpdkBdev handle.
    pub fn release_handle(&self) {
        self.open_handles.fetch_sub(1, Ordering::AcqRel);
    }

    /// Borrow qpair from pool for controller.
    pub fn acquire_qpair(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        self.qpair_pool.acquire(ctrlr)
    }
    /// Return qpair to pool, or free if at capacity.
    pub fn release_qpair(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) {
        self.qpair_pool.release(ctrlr, qpair);
    }

    /// Allocate a new qpair (bypassing pool) - for native reactor mode.
    pub fn alloc_qpair_on_reactor(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        let qpair = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        if qpair.is_null() {
            return err_box!(
                "Failed to allocate I/O qpair for ctrlr {:p} on reactor thread",
                ctrlr
            );
        }
        Ok(qpair)
    }

    /// Get the SPDK thread for a given controller index (native reactor).
    /// Returns the thread that OWNS this controller (based on reactor_idx in BdevInfo).
    pub fn get_spdk_thread(&self, ctrlr_idx: usize) -> *mut spdk_ffi::spdk_thread {
        let states = self.reactor_states.lock().unwrap();

        // Find the bdev for this ctrlr_idx to get the reactor_idx
        // We need to find which reactor owns this controller
        // For now, use the reactor_idx stored in bdevs
        if let Some(bdev) = self
            .bdevs
            .iter()
            .find(|b| b.ctrlr_idx as usize == ctrlr_idx)
        {
            let reactor_idx = bdev.reactor_idx;
            if reactor_idx < states.len() {
                let state = &states[reactor_idx];
                info!(
                    "Controller {} found on reactor-{} (matching BdevInfo)",
                    ctrlr_idx, reactor_idx
                );
                return state.thread;
            }
        }

        // Fallback: use round-robin if bdev not found
        let mut next_idx = self.next_reactor_idx.lock().unwrap();
        let target_idx = *next_idx % states.len();
        let state = &states[target_idx];
        let assigned_thread = state.thread;

        warn!(
            "Controller {} not found in bdevs, using round-robin: reactor-{}",
            ctrlr_idx, target_idx
        );

        *next_idx = (*next_idx + 1) % states.len().max(1);
        assigned_thread
    }

    /// Get the ReactorState for a given controller index (native reactor).
    /// Returns the reactor that OWNS this controller.
    pub fn get_reactor_state(&self, ctrlr_idx: usize) -> Arc<ReactorState> {
        let states = self.reactor_states.lock().unwrap();

        // Find the bdev for this ctrlr_idx to get the reactor_idx
        if let Some(bdev) = self
            .bdevs
            .iter()
            .find(|b| b.ctrlr_idx as usize == ctrlr_idx)
        {
            let reactor_idx = bdev.reactor_idx;
            if reactor_idx < states.len() {
                info!("Controller {} found on reactor-{}", ctrlr_idx, reactor_idx);
                return states[reactor_idx].clone();
            }
        }

        // Fallback: use round-robin
        let mut next_idx = self.next_reactor_idx.lock().unwrap();
        let target_idx = *next_idx % states.len();
        warn!(
            "Controller {} not found in bdevs, using round-robin: reactor-{}",
            ctrlr_idx, target_idx
        );
        states[target_idx].clone()
    }

    /// Unregister qpair from poller.
    /// Native reactor handles qpair cleanup via SPDK thread exit.
    pub fn unregister_qpair_from_poller(
        &self,
        _ctrlr_idx: usize,
        _qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) -> bool {
        true
    }

    // SPDK FFI — feature-gated

    fn env_init(&self) -> CommonResult<()> {
        use std::ffi::CString;
        info!(
            "SPDK env_init: app={}, hugepage={}MB, mask={}, shm_id={}, mem_ch={}",
            self.conf.app_name,
            self.conf.hugepage_mb,
            self.conf.reactor_mask,
            self.conf.shm_id,
            self.conf.mem_channel
        );
        let app_name = CString::new(self.conf.app_name.as_str())
            .map_err(|e| err_msg!(format!("invalid app_name: {}", e)))?;
        let core_mask = CString::new(self.conf.reactor_mask.as_str())
            .map_err(|e| err_msg!(format!("invalid reactor_mask: {}", e)))?;
        unsafe {
            // Verify our opaque buffer is large enough.
            let real_size = spdk_ffi::curvine_spdk_env_opts_sizeof();
            let buf_size = std::mem::size_of::<spdk_ffi::spdk_env_opts>();
            if real_size > buf_size {
                return err_box!(
                    "spdk_env_opts is {} bytes but our buffer is only {}",
                    real_size,
                    buf_size
                );
            }

            let mut opts: spdk_ffi::spdk_env_opts = std::mem::zeroed();
            spdk_ffi::curvine_spdk_env_opts_init(&mut opts);
            // Set our fields via C helpers — these write at the correct
            // C struct offsets regardless of SPDK version.
            spdk_ffi::curvine_spdk_env_opts_set_name(&mut opts, app_name.as_ptr());
            spdk_ffi::curvine_spdk_env_opts_set_core_mask(&mut opts, core_mask.as_ptr());
            spdk_ffi::curvine_spdk_env_opts_set_shm_id(&mut opts, self.conf.shm_id);
            spdk_ffi::curvine_spdk_env_opts_set_mem_channel(
                &mut opts,
                self.conf.mem_channel as i32,
            );
            spdk_ffi::curvine_spdk_env_opts_set_mem_size(&mut opts, self.conf.hugepage_mb as i32);

            info!(
                "Calling spdk_env_init with: name={}, mask={}, mem_size={}MB, shm_id={}, mem_channel={}",
                self.conf.app_name,
                self.conf.reactor_mask,
                self.conf.hugepage_mb,
                self.conf.shm_id,
                self.conf.mem_channel
            );

            let rc = spdk_ffi::curvine_spdk_env_init(&mut opts);
            info!("spdk_env_init returned rc={}", rc);
            if rc != 0 {
                error!("spdk_env_init failed: rc={} (EAL init failed)", rc);
                return err_box!("spdk_env_init failed with rc={}", rc);
            }

            // Verify EAL has memory by checking if we can allocate a small buffer
            let test_buf = spdk_ffi::curvine_spdk_dma_malloc(4096, 4096);
            if test_buf.is_null() {
                error!("EAL init succeeded but cannot allocate DMA memory - EAL has no memory!");
                return err_box!("EAL has no memory after init (check hugepages)");
            }
            spdk_ffi::curvine_spdk_dma_free(test_buf);
            info!("EAL memory verification passed (DMA alloc/free works)");

            // Register NVMe transports (TCP, RDMA, PCIe).
            // Must be called after env_init but before nvme_connect.
            spdk_ffi::curvine_spdk_register_transports();
            // Force rte_mempool_ring link (RTE_INIT constructors register
            // the ring mempool handler; needed for spdk_thread_lib_init).
            spdk_ffi::curvine_register_mempool_ring();
        }
        info!("SPDK environment initialized successfully");
        Ok(())
    }

    fn env_fini(&self) {
        info!("Finalizing SPDK environment");
        unsafe { spdk_ffi::spdk_env_fini() };
    }
}

/// Detach all NVMe controllers on a reactor thread.
/// Called via `spdk_thread_send_msg` from `shutdown()` so it runs
/// on the correct SPDK thread (required by SPDK for safe detach).
pub unsafe extern "C" fn detach_reactor_controllers(arg: *mut c_void) {
    let state = &*(arg as *const ReactorState);

    // Collect controllers under lock, then detach outside lock
    let ctrlrs: Vec<*mut spdk_ffi::spdk_nvme_ctrlr> = {
        let guard = state.controllers.lock().unwrap();
        guard.values().copied().filter(|c| !c.is_null()).collect()
    };

    for ctrlr in ctrlrs {
        info!(
            "Detaching NVMe controller on reactor {}: ctrlr={:p}",
            state.reactor_idx, ctrlr
        );
        let rc = spdk_ffi::spdk_nvme_detach(ctrlr);
        if rc != 0 {
            warn!(
                "spdk_nvme_detach failed on reactor {}: rc={}",
                state.reactor_idx, rc
            );
        }
    }

    // Clear the controllers map
    state.controllers.lock().unwrap().clear();

    // Signal completion to shutdown()
    state
        .detach_done
        .store(true, std::sync::atomic::Ordering::Release);
}

// Display

impl Display for SpdkEnv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkEnv(state={:?}, subsystems={}, bdevs={}, capacity={})",
            self.state(),
            self.conf.subsystems.len(),
            self.bdevs.len(),
            self.total_capacity()
        )
    }
}

// Tests
#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    fn fake(id: usize) -> *mut spdk_ffi::spdk_nvme_qpair {
        id as *mut spdk_ffi::spdk_nvme_qpair
    }
    fn push(pool: &QpairPool, key: usize) {
        pool.inner
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .push(fake(key));
    }
    fn pop(pool: &QpairPool, key: usize) -> bool {
        pool.inner
            .lock()
            .unwrap()
            .get_mut(&key)
            .and_then(|v| v.pop())
            .is_some()
    }
    fn cnt(pool: &QpairPool, key: usize) -> usize {
        pool.inner.lock().unwrap().get(&key).map_or(0, |v| v.len())
    }
    fn tot(pool: &QpairPool) -> usize {
        pool.inner.lock().unwrap().values().map(|v| v.len()).sum()
    }
    #[test]
    fn roundtrip() {
        let p = QpairPool::new();
        for _ in 0..3 {
            push(&p, 1);
        }
        assert_eq!(cnt(&p, 1), 3);
        for _ in 0..3 {
            assert!(pop(&p, 1));
        }
    }
    #[test]
    fn isolated() {
        let p = QpairPool::new();
        push(&p, 1);
        push(&p, 1);
        push(&p, 2);
        assert_eq!(cnt(&p, 1), 2);
        assert_eq!(cnt(&p, 2), 1);
    }
    #[test]
    fn cap() {
        let p = QpairPool {
            inner: Mutex::new(HashMap::new()),
            max_per_ctrlr: 2,
        };
        push(&p, 1);
        push(&p, 1);
        assert!(cnt(&p, 1) >= 2);
    }
    #[test]
    fn drain() {
        let p = QpairPool::new();
        push(&p, 1);
        push(&p, 2);
        p.inner.lock().unwrap().drain();
        assert_eq!(tot(&p), 0);
    }
    #[test]
    fn concurrent() {
        let p = Arc::new(QpairPool::new());
        let h: Vec<_> = (0..8)
            .map(|_| {
                let x = Arc::clone(&p);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        push(&x, 1);
                    }
                    for _ in 0..100 {
                        pop(&x, 1);
                    }
                })
            })
            .collect();
        for t in h {
            t.join().unwrap();
        }
        assert_eq!(tot(&p), 0);
    }
}
