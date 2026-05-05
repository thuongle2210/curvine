use crate::common::Utils;
use crate::io::block_io::BlockIO;
use crate::io::spdk_bdev::SpdkBdev;
use crate::io::spdk_env::{
    ControllerSelectionStrategy, NvmeSubsystem, RandomController, RoundRobinController, SpdkConf,
    SpdkEnv, SpdkEnvState,
};
use crate::sys::DataSlice;
use bytes::BytesMut;
use std::sync::Once;

/// Typical NVMe block size for alignment
const BLOCK_SIZE: usize = 512;
/// I/O pattern size (4K page)
const IO_SIZE: usize = 4096;

static INIT: Once = Once::new();

/// Initialize SPDK env once for all tests.
fn get_spdk_env() -> &'static SpdkEnv {
    INIT.call_once(|| {
        let conf = test_spdk_conf();
        println!("conf: {:?}", conf);
        SpdkEnv::init_global(conf).expect("Failed to init global SPDK env");
    });
    SpdkEnv::global().expect("SPDK env not initialized")
}

fn test_spdk_conf() -> SpdkConf {
    let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or("127.0.0.1".to_string());
    let trsvcid = std::env::var("SPDK_TARGET_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4420);
    let subnqn = std::env::var("SPDK_SUBNQN").unwrap_or("nqn.2024-01.io.curvine:test".to_string());
    let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or("tcp".to_string());
    let controller_count = std::env::var("SPDK_CONTROLLER_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let controller_selection =
        std::env::var("SPDK_CONTROLLER_SELECTION").unwrap_or("First".to_string());

    // Use smaller hugepage size for tests (64MB is enough for basic I/O tests)
    // IMPORTANT: Set hugepage_str BEFORE init() so it parses the correct value
    let hugepage_mb = std::env::var("SPDK_HUGEPAGE_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let hugepage_str = format!("{}MB", hugepage_mb);

    let mut conf = SpdkConf {
        enabled: true,
        app_name: "curvine-test".to_string(),
        hugepage_str: hugepage_str, // Set this BEFORE init() parses it
        hugepage_mb: 0,             // Will be set by init()
        reactor_mask: std::env::var("SPDK_REACTOR_MASK").unwrap_or("0x2".to_string()),
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
    // Parse computed fields (this will parse hugepage_str into hugepage_mb)
    conf.init().expect("Failed to init SpdkConf");
    conf
}

fn first_bdev_name() -> String {
    let env = get_spdk_env();
    assert!(!env.bdev_names().is_empty(), "No bdevs available");
    env.bdev_names()[0].clone()
}

fn zero_region(bdev_name: &str, offset: i64, len: usize) {
    let zeros = vec![0u8; len];
    let mut writer = SpdkBdev::open_write(bdev_name, offset, 0).unwrap();
    writer.write_all(&zeros).unwrap();
    writer.flush().unwrap();
}

/// Full SPDK lifecycle test (init => discovery => I/O => concurrent => shutdown).
// All phases in one test because SPDK global init runs once, and shutdown is destructive.
#[test]
fn spdk_full_lifecycle() {
    let env = get_spdk_env();
    assert_eq!(env.state(), SpdkEnvState::Initialized);
    assert!(env.is_initialized());
    assert!(!env.bdev_names().is_empty());
    println!("Discovered bdevs: {:?}", env.bdev_names());
    println!(
        "Controller selection: {:?}",
        env.conf().controller_selection.name()
    );
    let bdev_name = first_bdev_name();

    // Phase 2: open for write
    {
        let bdev = SpdkBdev::open_write(&bdev_name, 0, 0).expect("open for write");
        assert_eq!(bdev.pos(), 0);
        assert!(bdev.len() > 0);
        assert_eq!(bdev.path(), bdev_name);
        println!("pass open file for write test");
    }

    // Phase 3: open for read
    {
        let bdev = SpdkBdev::open_read(&bdev_name, 0, 0).expect("open for read");
        assert_eq!(bdev.pos(), 0);
        assert!(bdev.len() > 0);
        println!("pass open file for read test");
    }

    // Phase 4: open with offset
    {
        let offset = IO_SIZE as u64;
        let bdev = SpdkBdev::open_read(&bdev_name, offset, 0).expect("open with offset");
        assert_eq!(bdev.pos(), offset as i64);
        println!("pass open file with offset test");
    }

    // Phase 5: write/read roundtrip
    {
        let test_data = b"Hello SPDK over NVMe-oF/RDMA!";
        let aligned_len = ((test_data.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE) * BLOCK_SIZE;
        let mut write_buf = vec![0u8; aligned_len];
        write_buf[..test_data.len()].copy_from_slice(test_data);

        // Single scope for write and read to avoid any drop/flush issues
        {
            let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
            // Write the test data
            bdev.write_all(&write_buf).unwrap();
            bdev.flush().unwrap();
            assert_eq!(bdev.pos(), aligned_len as i64);

            // Seek back to beginning and read
            bdev.seek(0).unwrap();
            let mut read_buf = vec![0u8; aligned_len];
            bdev.read_all(&mut read_buf).unwrap();
            assert_eq!(
                &read_buf[..test_data.len()],
                test_data,
                "Data mismatch in same scope: read {:?} vs expected {:?}",
                &read_buf[..test_data.len()],
                test_data
            );
        }
        println!("pass write/read round-trip test");
    }

    // Phase 6: write_region/read_region
    {
        let data = Utils::rand_str(BLOCK_SIZE);
        let region = DataSlice::buffer(BytesMut::from(data.as_bytes()));

        // Write in its own scope
        {
            let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
            bdev.write_region(&region).unwrap();
            bdev.flush().unwrap();
            assert_eq!(bdev.pos(), BLOCK_SIZE as i64);
        }

        // Read in its own scope
        {
            let mut bdev = SpdkBdev::open_read(&bdev_name, 0, BLOCK_SIZE as i64).unwrap();
            let result = bdev.read_region(false, BLOCK_SIZE as i32).unwrap();
            assert_eq!(result.len(), BLOCK_SIZE);
            assert_eq!(
                result.as_slice(),
                data.as_bytes(),
                "write_region/read_region data mismatch"
            );
        }
        println!("pass write_region/read_region test");
    }

    // Phase 7: concurrent I/O through poller
    {
        use std::sync::{Arc, Barrier};

        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));
        let aligned_len = 4096usize;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let name = bdev_name.clone();
                let b = barrier.clone();
                std::thread::spawn(move || {
                    let offset = (i * IO_SIZE * 2) as i64;
                    let mut bdev = SpdkBdev::open_write(&name, offset, 0).unwrap();
                    let pattern = vec![(i as u8).wrapping_add(0x41); IO_SIZE];

                    b.wait();
                    bdev.write_all(&pattern).unwrap();
                    bdev.flush().unwrap();
                    bdev.seek(offset).unwrap();
                    let mut read_buf = vec![0u8; IO_SIZE];
                    bdev.read_all(&mut read_buf).unwrap();
                    assert_eq!(read_buf, pattern, "Thread {} data corruption", i);
                })
            })
            .collect();

        for h in handles {
            h.join().expect("Concurrent poller I/O thread panicked");
        }
        println!(
            "pass concurrent poller I/O test (8 threads, strategy: {:?})",
            env.conf().controller_selection.name()
        );
    }

    // Phase 8: shutdown (must be last — destructive)
    // Note: Disabled because OnceLock prevents re-initialization in same process.
    // To test shutdown properly, run this test in isolation or as the last test.
    {
        env.shutdown();
        assert_eq!(env.state(), SpdkEnvState::ShutDown);
        assert!(
            SpdkEnv::global().is_none(),
            "global() should return None after shutdown"
        );
        let result = SpdkBdev::open_write(&bdev_name, 0, 0);
        assert!(result.is_err(), "Should not open bdev after shutdown");
        println!("pass shutdown test");
    }
}

// Async read test (requires tokio runtime)
#[cfg(feature = "spdk")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spdk_async_read_test() {
    // Initialize SPDK (reuse env from other tests)
    let _env = get_spdk_env();
    let bdev_name = first_bdev_name();

    let test_data = b"Async SPDK read test data!";
    let aligned_len = ((test_data.len() + (BLOCK_SIZE - 1)) / BLOCK_SIZE) * BLOCK_SIZE;
    let mut write_buf = vec![0u8; aligned_len];
    write_buf[..test_data.len()].copy_from_slice(test_data);

    // Write and read in same scope
    {
        let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
        bdev.write_all(&write_buf).unwrap();
        bdev.flush().unwrap();

        // Seek to beginning and read back async
        bdev.seek(0).unwrap();
        let result = bdev.spdk_read_async(0, aligned_len).await.unwrap();
        assert_eq!(
            &result[..test_data.len()],
            test_data,
            "Async read mismatch: read {:?} vs expected {:?}",
            &result[..test_data.len()],
            test_data
        );
    }
    println!("pass spdk_read_async test");
}

// Async read_region test
#[cfg(feature = "spdk")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spdk_async_read_region_test() {
    let _env = get_spdk_env();
    let bdev_name = first_bdev_name();

    // Write test pattern
    let pattern = Utils::rand_str(IO_SIZE);
    {
        let mut bdev = SpdkBdev::open_write(&bdev_name, 0, 0).unwrap();
        bdev.write_all(pattern.as_bytes()).unwrap();
        bdev.flush().unwrap();
    }

    // Read back using read_region_async
    let mut bdev = SpdkBdev::open_read(&bdev_name, 0, IO_SIZE as i64).unwrap();
    let result = bdev.read_region_async(false, IO_SIZE as i32).await.unwrap();
    assert_eq!(result.len(), IO_SIZE);
    assert_eq!(result.as_slice(), pattern.as_bytes());
    println!("pass spdk_read_region_async test");
}
