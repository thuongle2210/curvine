fn main() {
    if std::env::var("CARGO_FEATURE_SPDK").is_ok() {
        link_spdk();
    }
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_SPDK");
}

fn link_spdk() {
    let spdk_dir = std::env::var("SPDK_DIR").ok();
    let lib_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());

    println!("cargo:rustc-link-search=native={}", lib_dir);

    if let Some(ref dir) = spdk_dir {
        println!("cargo:rustc-link-search=native={}/dpdk/build/lib", dir);
        println!("cargo:rustc-link-search=native={}/build/lib/dpdk", dir);
        println!("cargo:rustc-link-search=native={}/dpdk/lib", dir);
    }

    let whole = ["spdk_nvme", "spdk_sock", "spdk_sock_posix"];
    for lib in &whole {
        let lib_path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&lib_path).exists() {
            println!("cargo:rustc-link-arg=-Wl,--whole-archive");
            println!("cargo:rustc-link-arg={}", lib_path);
            println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");
        }
    }

    if let Some(ref dir) = spdk_dir {
        let dpdk_lib_dir = format!("{}/dpdk/build/lib", dir);
        for lib in &["rte_mempool_ring", "rte_mempool"] {
            let lib_path = format!("{}/lib{}.a", dpdk_lib_dir, lib);
            if std::path::Path::new(&lib_path).exists() {
                println!("cargo:rustc-link-arg=-Wl,--whole-archive");
                println!("cargo:rustc-link-arg={}", lib_path);
                println!("cargo:rustc-link-arg=-Wl,--no-whole-archive");
            }
        }
    }

    let spdk_libs = [
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
    for lib in &spdk_libs {
        let path = format!("{}/lib{}.a", lib_dir, lib);
        if std::path::Path::new(&path).exists() {
            println!("cargo:rustc-link-lib=static={}", lib);
        }
    }

    if std::env::var("CARGO_FEATURE_SPDK_RDMA").is_ok() {
        for lib in &["spdk_rdma_provider", "spdk_rdma_utils"] {
            let path = format!("{}/lib{}.a", lib_dir, lib);
            if std::path::Path::new(&path).exists() {
                println!("cargo:rustc-link-lib=static={}", lib);
            }
        }
    }

    let dpdk_dir = spdk_dir
        .as_ref()
        .map(|d| format!("{}/dpdk/build/lib", d))
        .unwrap_or_else(|| "/usr/local/lib".to_string());
    let dpdk_libs = [
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

    let keyring = format!("{}/libspdk_keyring.a", lib_dir);
    if std::path::Path::new(&keyring).exists() {
        println!("cargo:rustc-link-lib=static=spdk_keyring");
    }

    println!("cargo:rerun-if-env-changed=SPDK_DIR");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_SPDK_RDMA");
}
