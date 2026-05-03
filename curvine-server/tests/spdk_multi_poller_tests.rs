#![cfg(feature = "spdk")]

use orpc::io::spdk_bdev::SpdkBdev;
use orpc::io::spdk_env::{
    ControllerSelectionStrategy, NvmeSubsystem, RandomController, RoundRobinController, SpdkConf,
    SpdkEnv, SpdkEnvState,
};
use orpc::io::BlockIO;
use std::sync::Once;
use std::sync::{Arc, Barrier, Mutex};

/// Typical NVMe block size for alignment
const BLOCK_SIZE: usize = 512;
/// I/O pattern size (4K page)
const IO_SIZE: usize = 4096;

static INIT: Once = Once::new();

/// Read controller selection strategy from env var
fn get_controller_selection() -> String {
    std::env::var("SPDK_CONTROLLER_SELECTION").unwrap_or("First".to_string())
}

/// Build test config from env vars.
fn make_conf() -> SpdkConf {
    let controller_selection = get_controller_selection();
    let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or("127.0.0.1".to_string());
    let trsvcid: u16 = std::env::var("SPDK_TARGET_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4420);
    let subnqn = std::env::var("SPDK_SUBNQN").unwrap_or("nqn.2024-01.io.curvine:test".to_string());
    let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or("tcp".to_string());
    let controller_count: u32 = std::env::var("SPDK_CONTROLLER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let hugepage_mb: u32 = std::env::var("SPDK_HUGEPAGE_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);

    let mut conf = SpdkConf {
        enabled: true,
        app_name: "curvine-test".to_string(),
        hugepage_str: format!("{}MB", hugepage_mb),
        hugepage_mb,
        reactor_mask: std::env::var("SPDK_REACTOR_MASK").unwrap_or("0x1".to_string()),
        controller_selection_str: controller_selection,
        controller_selection: ControllerSelectionStrategy::First,
        subsystems: vec![NvmeSubsystem {
            traddr,
            trsvcid,
            subnqn,
            trtype,
            adrfam: "ipv4".to_string(),
            controller_count,
            ..Default::default()
        }],
        ..Default::default()
    };
    conf.init().expect("Failed to init SpdkConf");
    conf
}

/// Get or init the SPDK env (once per process).
fn get_spdk_env() -> &'static SpdkEnv {
    static mut ENV: Option<&'static SpdkEnv> = None;

    INIT.call_once(|| {
        let conf = make_conf();
        let env = SpdkEnv::init_global(conf).expect("Failed to init SPDK env");
        unsafe { ENV = Some(env) };
    });
    unsafe { ENV.expect("SPDK env not initialized") }
}

#[test]
fn test_full_lifecycle() {
    let controller_selection = get_controller_selection();
    let env = get_spdk_env();
    assert_eq!(env.state(), SpdkEnvState::Initialized);

    let bdev_names = env.bdev_names();
    assert!(!bdev_names.is_empty(), "No bdevs discovered");
    println!("Discovered {} bdevs: {:?}", bdev_names.len(), bdev_names);
    println!("Controller selection strategy: {}", controller_selection);

    // Test 1: Basic I/O
    println!("\n=== Test 1: Basic I/O ===");
    let test_data = b"Controller selection test data!";
    let aligned_len = ((test_data.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE) * BLOCK_SIZE;

    for (i, name) in bdev_names.iter().enumerate() {
        let mut bdev = SpdkBdev::open_write(name, 0, 0).expect("open");
        bdev.write_all(test_data).unwrap();
        bdev.flush().unwrap();

        let mut read_buf = vec![0u8; aligned_len];
        bdev.seek(0).unwrap();
        bdev.read_all(&mut read_buf).unwrap();
        assert_eq!(
            &read_buf[..test_data.len()],
            test_data,
            "Data mismatch for bdev {}",
            i
        );
    }
    println!("Basic I/O passed");

    // Test 2: Concurrent I/O
    println!("\n=== Test 2: Concurrent I/O ===");
    let num_threads = 8usize;
    let barrier = Arc::new(Barrier::new(num_threads));
    let errors = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..num_threads {
        let name = bdev_names[0].clone();
        let b = barrier.clone();
        let errs = errors.clone();
        let handle = std::thread::spawn(move || {
            let offset = (i as i64) * IO_SIZE as i64 * 2;
            let mut bdev: SpdkBdev = match SpdkBdev::open_write(&name, offset, 0) {
                Ok(b) => b,
                Err(e) => {
                    errs.lock()
                        .unwrap()
                        .push(format!("Thread {} open failed: {}", i, e));
                    return;
                }
            };

            let pattern = vec![(i as u8).wrapping_add(0x41); IO_SIZE];
            b.wait();

            if let Err(e) = bdev.write_all(&pattern) {
                errs.lock()
                    .unwrap()
                    .push(format!("Thread {} write failed: {}", i, e));
                return;
            }
            if let Err(e) = bdev.flush() {
                errs.lock()
                    .unwrap()
                    .push(format!("Thread {} flush failed: {}", i, e));
                return;
            }

            bdev.seek(offset).unwrap();
            let mut read_buf = vec![0u8; IO_SIZE];
            if let Err(e) = bdev.read_all(&mut read_buf) {
                errs.lock()
                    .unwrap()
                    .push(format!("Thread {} read failed: {}", i, e));
                return;
            }

            if read_buf != pattern {
                errs.lock()
                    .unwrap()
                    .push(format!("Thread {} data corruption", i));
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "Concurrent I/O errors: {:?}", *errs);
    println!(" Concurrent I/O passed");

    println!("\n All tests passed with {}!", controller_selection);

    // Shutdown at the end
    env.shutdown();
    assert_eq!(env.state(), SpdkEnvState::ShutDown);
    println!("✓ Shutdown passed");
}

// ---------------------------------------------------------------------------
// Note: To test RoundRobin/Random, run separately with SPDK_CONTROLLER_SELECTION=RoundRobin
// SPDK can only be initialized ONCE per process, so we can't test all
// strategies in the same test run.
// ---------------------------------------------------------------------------
