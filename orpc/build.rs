fn main() {
    #[cfg(feature = "spdk")]
    link_spdk();
}

#[cfg(feature = "spdk")]
fn link_spdk() {
    let spdk_dir = std::env::var("SPDK_DIR").ok();
    let lib_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());
    let dpdk_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/dpdk/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());
    println!("cargo:rustc-link-search=native={}", lib_dir);

    // Get OUT_DIR for linking compiled C helpers
    let out_dir = std::env::var("OUT_DIR").unwrap();

    // Compile C helper for version-safe opts setters
    let include_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/include", d))
        .unwrap_or_else(|| "/usr/local/include".to_string());
    let dpdk_include = spdk_dir
        .as_ref()
        .map(|d| format!("{}/dpdk/build/include", d))
        .unwrap_or_else(|| "/usr/local/include/dpdk".to_string());
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let helper_src = format!("{}/csrc/spdk_opts_helper.c", manifest_dir);
    cc::Build::new()
        .file(&helper_src)
        .include(&include_dir)
        .include(&dpdk_include)
        .compile("spdk_opts_helper");
    println!("cargo:rerun-if-changed={}", helper_src);
    // Explicitly link spdk_opts_helper from OUT_DIR
    println!("cargo:rustc-link-search=native={}", out_dir);
    println!("cargo:rustc-link-lib=static=spdk_opts_helper");

    // DPDK lib subdirs
    if let Some(ref dir) = spdk_dir {
        println!("cargo:rustc-link-search=native={}/dpdk/build/lib", dir);
        println!("cargo:rustc-link-search=native={}/build/lib/dpdk", dir);
        println!("cargo:rustc-link-search=native={}/dpdk/lib", dir);
    }

    // Link SPDK libs (static)
    let whole = ["spdk_nvme", "spdk_sock", "spdk_sock_posix"];
    let libs = vec![
        "spdk_trace",
        "spdk_json",
        "spdk_jsonrpc",
        "spdk_rpc",
        "spdk_log",
        "spdk_util",
        "spdk_env_dpdk",
        "spdk_env_dpdk_rpc",
        "spdk_init",
        "spdk_thread",
        "spdk_dma",
    ];

    #[cfg(feature = "spdk-rdma")]
    libs.extend(["spdk_rdma_provider", "spdk_rdma_utils"]);

    // SPDK whole libs need --whole-archive so constructors (transport
    // registration, device drivers, etc.) are included unconditionally.
    // Also push them via rustc-link-lib=static= so they propagate to
    // dependent workspace crates (rustc-link-arg does NOT propagate).
    // Both mechanisms put the same .a files into the link, causing LLD
    // to see duplicate object definitions — harmless because they resolve
    // to the same symbols.  The static-link-wrapper.sh adds
    // --allow-multiple-definition to suppress LLD errors.
    for lib in &whole {
        let lib_path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&lib_path).exists() {
            println!("cargo:rustc-link-arg=-Wl,--whole-archive");
            println!("cargo:rustc-link-arg={}", lib_path);
            println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    for lib in &libs {
        let path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    let dpdk_libs = [
        "rte_mempool",
        "rte_mempool_ring",
        "rte_eal",
        "rte_ring",
        "rte_mbuf",
        "rte_bus_pci",
        "rte_pci",
        "rte_kvargs",
        "rte_telemetry",
        "rte_power",
        "rte_ethdev",
        "rte_net",
        "rte_vhost",
        "rte_cryptodev",
        "rte_compressdev",
        "rte_dmadev",
    ];
    for lib in &dpdk_libs {
        let path = format!("{}/lib{}.a", dpdk_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    // Extra DPDK libs
    let extra = [
        "rte_log",
        "rte_argparse",
        "rte_bus_vdev",
        "rte_cmdline",
        "rte_hash",
        "rte_meter",
        "rte_rcu",
        "rte_stack",
        "rte_timer",
    ];
    for lib in &extra {
        let path = format!("{}/lib{}.a", dpdk_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    // SPDK keyring
    let keyring = format!("{}/libspdk_keyring.a", lib_dir);
    if std::path::Path::new(&keyring).exists() {
        println!("cargo:rustc-link-lib=static=spdk_keyring");
    }

    // System libs
    for lib in &[
        "pthread", "dl", "rt", "numa", "uuid", "fuse3", "isal", "ssl", "crypto",
    ] {
        println!("cargo:rustc-link-lib=dylib={}", lib);
    }

    // RDMA libs (spdk-rdma only)
    #[cfg(feature = "spdk-rdma")]
    for lib in &["ibverbs", "rdmacm"] {
        println!("cargo:rustc-link-lib=dylib={}", lib);
    }

    // Force-include librte_mempool_ring objects into libspdk_opts_helper.a.
    // The ring library exports zero global symbols; without --whole-archive
    // the linker drops all its objects. We globalize the local 'generator'
    // symbol in the constructor-bearing object so the helper code can
    // reference it, forcing the linker to include the ring object (which
    // contains RTE_INIT constructors that register the mempool handler).
    {
        let ring_archive = format!("{}/librte_mempool_ring.a", dpdk_dir);
        let helper_archive = format!("{}/libspdk_opts_helper.a", out_dir);
        if std::path::Path::new(&ring_archive).exists()
            && std::path::Path::new(&helper_archive).exists()
        {
            let work_dir = format!("{}/ring_embed", out_dir);
            let _ = std::fs::remove_dir_all(&work_dir);
            if std::fs::create_dir_all(&work_dir).is_ok() {
                if std::process::Command::new("ar")
                    .arg("x")
                    .arg(&ring_archive)
                    .current_dir(&work_dir)
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false)
                {
                    let mut objs = Vec::new();
                    if let Ok(entries) = std::fs::read_dir(&work_dir) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if path.extension().map_or(false, |e| e == "o") {
                                if let Ok(nm_out) = std::process::Command::new("nm")
                                    .arg(&path)
                                    .output()
                                {
                                    let nm = String::from_utf8_lossy(&nm_out.stdout);
                                    if nm.contains("mp_hdlr_init") {
                                        std::process::Command::new("objcopy")
                                            .arg("--globalize-symbol=mp_hdlr_init_ops_mp_mc")
                                            .arg(&path)
                                            .status()
                                            .ok();
                                    }
                                }
                                objs.push(path);
                            }
                        }
                    }
                    if !objs.is_empty() {
                        let mut ar = std::process::Command::new("ar");
                        ar.arg("rcs").arg(&helper_archive);
                        for o in &objs {
                            ar.arg(o);
                        }
                        let _ = ar.status();
                    }
                }
            }
        }
    }

    println!("cargo:rerun-if-env-changed=SPDK_DIR");
}
