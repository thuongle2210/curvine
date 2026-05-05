#include "spdk/thread.h"
#include "spdk/env.h"
#include <stdio.h>
#include <dlfcn.h>

// Constructor: runs BEFORE main() - guarantees mempool driver is loaded before EAL init
// This is a backup in case static linking with --whole-archive doesn't work
__attribute__((constructor)) static void auto_load_mempool_driver(void) {
    fprintf(stderr, "[DEBUG C] Constructor: loading mempool ring driver...\n");
    void *handle = dlopen("/home/thuongle/spdk_project/spdk/dpdk/build/lib/librte_mempool_ring.so",
                          RTLD_NOW | RTLD_GLOBAL);
    if (handle) {
        fprintf(stderr, "[DEBUG C] Constructor: Loaded mempool ring driver\n");
    } else {
        fprintf(stderr, "[DEBUG C] Constructor: Failed: %s\n", dlerror());
    }
}

// Wrapper for spdk_thread_lib_init that uses spdk_thread_lib_init_ext
// with a smaller mempool size to avoid ENOMEM failures due to hugepage fragmentation
int curvine_spdk_thread_lib_init(spdk_new_thread_fn new_thread_fn, size_t ctx_sz) {
    fprintf(stderr, "[DEBUG C] curvine_spdk_thread_lib_init: new_thread_fn=%p, ctx_sz=%zu\n",
            (void*)new_thread_fn, ctx_sz);

    size_t small_mempool_size = 4096;

    fprintf(stderr, "[DEBUG C] Calling spdk_thread_lib_init_ext with msg_mempool_size=%zu...\n",
            small_mempool_size);

    int rc = spdk_thread_lib_init_ext(NULL, NULL, ctx_sz, small_mempool_size);
    fprintf(stderr, "[DEBUG C] spdk_thread_lib_init_ext returned: rc=%d\n", rc);
    return rc;
}
