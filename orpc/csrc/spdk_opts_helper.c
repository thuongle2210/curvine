#include "spdk/env.h"
#include "spdk/nvme.h"
#include "spdk/thread.h"
#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <rte_mempool.h>
#include <rte_eal.h>
#include <rte_errno.h>

// Pre-load SPDK/DPDK shared libraries BEFORE spdk_env_init()
// This ensures constructors run and mempool drivers are registered.
// Based on working spdk_native_reactor_highest_perf.c pattern.
// void curvine_spdk_preload_libs(void) {
    // fprintf(stderr, "[DEBUG C] Pre-loading SPDK/DPDK libraries...\n");
    // const char *libs[] = {
    //     "/home/thuongle/spdk_project/spdk/dpdk/build/lib/librte_mempool_ring.so",
    //     NULL
    // };
    // for (int i = 0; libs[i]; i++) {
    //     void *handle = dlopen(libs[i], RTLD_NOW | RTLD_GLOBAL);
    //     if (handle) {
    //         fprintf(stderr, "[DEBUG C]   Loaded: %s\n", libs[i]);
    //     } else {
    //         fprintf(stderr, "[DEBUG C]   FAILED to load: %s (error=%s)\n", libs[i], dlerror());
    //     }
    // }
// }

// env_opts helpers
void curvine_spdk_env_opts_init(struct spdk_env_opts *opts) {
    opts->opts_size = sizeof(*opts);
    spdk_env_opts_init(opts);
}
size_t curvine_spdk_env_opts_sizeof(void) {
    return sizeof(struct spdk_env_opts);
}
void curvine_spdk_env_opts_set_name(struct spdk_env_opts *opts, const char *name) {
    opts->name = name;
}
void curvine_spdk_env_opts_set_core_mask(struct spdk_env_opts *opts, const char *mask) {
    opts->core_mask = mask;
}
void curvine_spdk_env_opts_set_shm_id(struct spdk_env_opts *opts, int shm_id) {
    opts->shm_id = shm_id;
}
void curvine_spdk_env_opts_set_mem_channel(struct spdk_env_opts *opts, int channel) {
    opts->mem_channel = channel;
}
void curvine_spdk_env_opts_set_mem_size(struct spdk_env_opts *opts, int size_mb) {
    opts->mem_size = size_mb;
}
int curvine_spdk_env_init(struct spdk_env_opts *opts) {
    return spdk_env_init(opts);
}

// transport_id helpers
size_t curvine_spdk_trid_sizeof(void) {
    return sizeof(struct spdk_nvme_transport_id);
}
void curvine_spdk_trid_set_trtype(struct spdk_nvme_transport_id *trid, int trtype) {
    trid->trtype = trtype;
}
void curvine_spdk_trid_set_adrfam(struct spdk_nvme_transport_id *trid, int adrfam) {
    trid->adrfam = adrfam;
}
void curvine_spdk_trid_set_traddr(struct spdk_nvme_transport_id *trid, const char *addr) {
    snprintf(trid->traddr, sizeof(trid->traddr), "%s", addr);
}
void curvine_spdk_trid_set_trsvcid(struct spdk_nvme_transport_id *trid, const char *port) {
    snprintf(trid->trsvcid, sizeof(trid->trsvcid), "%s", port);
}
void curvine_spdk_trid_set_subnqn(struct spdk_nvme_transport_id *trid, const char *nqn) {
    snprintf(trid->subnqn, sizeof(trid->subnqn), "%s", nqn);
}

// ctrlr_opts helpers
size_t curvine_spdk_ctrlr_opts_sizeof(void) {
    return sizeof(struct spdk_nvme_ctrlr_opts);
}
void curvine_spdk_ctrlr_get_default_opts(struct spdk_nvme_ctrlr_opts *opts) {
    opts->opts_size = sizeof(*opts);
    spdk_nvme_ctrlr_get_default_ctrlr_opts(opts, sizeof(*opts));
}
void curvine_spdk_ctrlr_opts_set_num_io_queues(struct spdk_nvme_ctrlr_opts *opts, uint32_t num) {
    opts->num_io_queues = num;
}
void curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(struct spdk_nvme_ctrlr_opts *opts, uint32_t ms) {
    opts->keep_alive_timeout_ms = ms;
}
void curvine_spdk_ctrlr_opts_set_hostnqn(struct spdk_nvme_ctrlr_opts *opts, const char *nqn) {
    snprintf(opts->hostnqn, sizeof(opts->hostnqn), "%s", nqn);
}

// connect
struct spdk_nvme_ctrlr *curvine_spdk_nvme_connect(struct spdk_nvme_transport_id *trid,
                                                    struct spdk_nvme_ctrlr_opts *opts) {
    return spdk_nvme_connect(trid, opts, sizeof(*opts));
}

// Force transport registration (static link)
extern void __attribute__((weak)) _spdk_nvme_transport_register_tcp(void);
extern void __attribute__((weak)) _spdk_nvme_transport_register_rdma(void);
extern void __attribute__((weak)) _spdk_nvme_transport_register_pcie(void);
void curvine_spdk_register_transports(void) {
    if (_spdk_nvme_transport_register_tcp) _spdk_nvme_transport_register_tcp();
    if (_spdk_nvme_transport_register_rdma) _spdk_nvme_transport_register_rdma();
    if (_spdk_nvme_transport_register_pcie) _spdk_nvme_transport_register_pcie();
}

// DMA buffer
void *curvine_spdk_dma_malloc(uint64_t size, uint64_t align) {
    return spdk_dma_zmalloc(size, align, NULL);
}
void curvine_spdk_dma_free(void *buf) {
    spdk_dma_free(buf);
}

// Thread helpers
// #include "spdk/thread.h"
// bool curvine_spdk_thread_is_current(struct spdk_thread *thread) {
//     struct spdk_thread *current = spdk_get_thread();
//     return (current != NULL) && (current == thread);
// }

// Debug: check EAL memory availability
void curvine_check_eal_memory(void) {
    fprintf(stderr, "[DEBUG C] Checking EAL memory:\n");
    void *buf = spdk_dma_malloc(4096, 4096, NULL);
    if (buf) {
        fprintf(stderr, "[DEBUG C]   spdk_dma_malloc(4096) succeeded: %p\n", buf);
        spdk_dma_free(buf);
    } else {
        fprintf(stderr, "[DEBUG C]   spdk_dma_malloc(4096) FAILED - EAL has no memory!\n");
    }
}

// Run SPDK reactor loop on current thread
// ASSUME: spdk_set_thread() has already been called by the caller.
// The loop continues until spdk_thread_exit() is called on this thread.
// stop_flag: if non-NULL, checked after first N iterations; if true, exits the loop.
//            This allows Rust to signal stop from another thread after initialization.
void curvine_spdk_run_reactor_loop_with_stop(struct spdk_thread *thread, volatile void *stop_flag) {
    fprintf(stderr, "[DEBUG C] Entering reactor poll loop for thread %p, stop_flag=%p...\n", thread, stop_flag);
    
    // Main reactor loop - poll until thread is exited OR stop_flag is set
    int iteration = 0;
    int init_complete = 0;  // Flag to track initialization
    
    while (!spdk_thread_is_exited(thread)) {
        // After first 10 iterations, check stop flag if provided
        // This gives time for initialization (controller attach, qpair create, etc.)
        if (init_complete && stop_flag != NULL && *(volatile int*)stop_flag) {
            fprintf(stderr, "[DEBUG C] Reactor loop: stop_flag set, exiting after %d iterations\n", iteration);
            break;
        }
        
        // Also check global shutdown flag (set by Rust shutdown())
        if (init_complete && curvine_spdk_get_shutdown_flag()) {
            fprintf(stderr, "[DEBUG C] Reactor loop: global shutdown flag set, exiting after %d iterations\n", iteration);
            break;
        }
        
        // Poll this thread - processes messages and pollers
        int msgs = spdk_thread_poll(thread, 0, 0);  // max_msgs=0 means no limit
        
        // Mark init as complete after some iterations or msgs processed
        if (iteration >= 10 || msgs > 0) {
            init_complete = 1;
        }
        
        if (msgs > 0) {
            fprintf(stderr, "[DEBUG C] Reactor iteration %d: processed %d message(s), init_complete=%d\n", 
                    iteration, msgs, init_complete);
        }
        iteration++;
        if (iteration % 100000 == 0) {
            fprintf(stderr, "[DEBUG C] Reactor still running, iteration %d, exited=%d, shutdown=%d\n", 
                    iteration, spdk_thread_is_exited(thread), curvine_spdk_get_shutdown_flag());
        }
    }
    
    fprintf(stderr, "[DEBUG C] Reactor loop exiting after %d iterations\n", iteration);
}

// Backward compatibility: original function without stop flag
void curvine_spdk_run_reactor_loop(struct spdk_thread *thread) {
    curvine_spdk_run_reactor_loop_with_stop(thread, NULL);
}

// Signal the reactor thread to exit.
// This should be called from WITHIN the reactor thread.
// Sets the thread's exit flag so next spdk_thread_poll will return.
// void curvine_spdk_signal_reactor_exit(struct spdk_thread *thread) {
//     if (thread) {
//         fprintf(stderr, "[DEBUG C] curvine_spdk_signal_reactor_exit: thread=%p\n", thread);
//         spdk_thread_exit(thread);
//     }
// }

// Global shutdown flag (atomic) - set by Rust shutdown()
static volatile int g_shutdown_flag = 0;

// Set global shutdown flag (called from Rust during shutdown)
void curvine_spdk_set_shutdown_flag(void) {
    __sync_synchronize();  // Memory barrier
    g_shutdown_flag = 1;
    __sync_synchronize();
    fprintf(stderr, "[DEBUG C] curvine_spdk_set_shutdown_flag: g_shutdown_flag=1\n");
}

// Check global shutdown flag
int curvine_spdk_get_shutdown_flag(void) {
    __sync_synchronize();  // Memory barrier
    return g_shutdown_flag;
}



// I/O qpair
struct spdk_nvme_qpair *curvine_spdk_alloc_io_qpair(struct spdk_nvme_ctrlr *ctrlr) {
    fprintf(stderr, "[DEBUG C] curvine_spdk_alloc_io_qpair: ctrlr=%p\n", ctrlr);
    if (!ctrlr) {
        fprintf(stderr, "[ERROR C] ctrlr is NULL!\n");
        return NULL;
    }
    struct spdk_nvme_io_qpair_opts opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    struct spdk_nvme_qpair *qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
    fprintf(stderr, "[DEBUG C] spdk_nvme_ctrlr_alloc_io_qpair returned: %p\n", qpair);
    if (!qpair) {
        fprintf(stderr, "[ERROR C] Failed to allocate qpair for ctrlr=%p\n", ctrlr);
    }
    return qpair;
}
void curvine_spdk_free_io_qpair(struct spdk_nvme_qpair *qpair) {
    if (qpair) spdk_nvme_ctrlr_free_io_qpair(qpair);
}

// I/O context
struct curvine_io_ctx {
    volatile bool done;
    int status;
};
static void curvine_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct curvine_io_ctx *ctx = arg;
    if (spdk_nvme_cpl_is_error(cpl)) {
        ctx->status = -((int)cpl->status.sct * 256 + (int)cpl->status.sc);
        if (ctx->status == 0) ctx->status = -1;
    } else {
        ctx->status = 0;
    }
    ctx->done = true;
}
static int curvine_poll(struct spdk_nvme_qpair *qpair, struct curvine_io_ctx *ctx, uint64_t timeout_us) {
    uint64_t start = 0, hz = 0, ticks = 0;
    if (timeout_us > 0) {
        start = spdk_get_ticks();
        hz = spdk_get_ticks_hz();
        ticks = timeout_us * hz / 1000000ULL;
    }
    while (!ctx->done) {
        int rc = spdk_nvme_qpair_process_completions(qpair, 0);
        if (rc < 0) return rc;
        if (timeout_us > 0 && (spdk_get_ticks() - start) > ticks) return -ETIMEDOUT;
    }
    return ctx->status;
}

// Sync I/O (unused, replaced by async _submit variants)
// int curvine_spdk_ns_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
//                          void *buf, uint64_t offset, uint64_t nbytes, uint64_t timeout_us) {
//     uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
//     fprintf(stderr, "[DEBUG C] curvine_spdk_ns_read: ns=%p, qpair=%p, offset=%lu, nbytes=%lu, ss=%u, lba=%lu, nblocks=%lu\n",
//             ns, qpair, offset, nbytes, ss, offset / ss, nbytes / ss);
//     struct curvine_io_ctx ctx = { .done = false, .status = 0 };
//     int rc = spdk_nvme_ns_cmd_read(ns, qpair, buf, offset / ss, nbytes / ss, curvine_cb, &ctx, 0);
//     fprintf(stderr, "[DEBUG C] curvine_spdk_ns_read: spdk_nvme_ns_cmd_read returned rc=%d\n", rc);
//     return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
// }
// int curvine_spdk_ns_write(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
//                           void *buf, uint64_t offset, uint64_t nbytes, uint64_t timeout_us) {
//     uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
//     fprintf(stderr, "[DEBUG C] curvine_spdk_ns_write: ns=%p, qpair=%p, offset=%lu, nbytes=%lu, ss=%u, lba=%lu, nblocks=%lu\n",
//             ns, qpair, offset, nbytes, ss, offset / ss, nbytes / ss);
//     struct curvine_io_ctx ctx = { .done = false, .status = 0 };
//     int rc = spdk_nvme_ns_cmd_write(ns, qpair, buf, offset / ss, nbytes / ss, curvine_cb, &ctx, 0);
//     fprintf(stderr, "[DEBUG C] curvine_spdk_ns_write: spdk_nvme_ns_cmd_write returned rc=%d\n", rc);
//     return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
// }
// int curvine_spdk_ns_flush(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, uint64_t timeout_us) {
//     struct curvine_io_ctx ctx = { .done = false, .status = 0 };
//     int rc = spdk_nvme_ns_cmd_flush(ns, qpair, curvine_cb, &ctx);
//     return rc ? rc : curvine_poll(qpair, &ctx, timeout_us);
// }

// Async I/O
typedef void (*curvine_async_cb)(void *arg, int status);
struct curvine_async_ctx {
    curvine_async_cb cb;
    void *cb_arg;
};
static void curvine_async_cb_fn(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct curvine_async_ctx *ctx = arg;
    int s = 0;
    if (spdk_nvme_cpl_is_error(cpl)) {
        s = -((int)cpl->status.sct * 256 + (int)cpl->status.sc);
        if (s == 0) s = -1;
    }
    ctx->cb(ctx->cb_arg, s);
}
int curvine_spdk_ns_submit_read(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                                void *buf, uint64_t offset, uint64_t nbytes, struct curvine_async_ctx *ctx) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    fprintf(stderr, "[DEBUG C] curvine_spdk_ns_submit_read: ns=%p, qpair=%p, offset=%lu, nbytes=%lu, ss=%u, lba=%lu, nblocks=%lu, ctx=%p\n",
            ns, qpair, offset, nbytes, ss, offset / ss, nbytes / ss, (void*)ctx);
    return spdk_nvme_ns_cmd_read(ns, qpair, buf, offset / ss, nbytes / ss, curvine_async_cb_fn, ctx, 0);
}
int curvine_spdk_ns_submit_write(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                                void *buf, uint64_t offset, uint64_t nbytes, struct curvine_async_ctx *ctx) {
    uint32_t ss = spdk_nvme_ns_get_sector_size(ns);
    fprintf(stderr, "[DEBUG C] curvine_spdk_ns_submit_write: ns=%p, qpair=%p, offset=%lu, nbytes=%lu, ss=%u, lba=%lu, nblocks=%lu, ctx=%p\n",
            ns, qpair, offset, nbytes, ss, offset / ss, nbytes / ss, (void*)ctx);
    return spdk_nvme_ns_cmd_write(ns, qpair, buf, offset / ss, nbytes / ss, curvine_async_cb_fn, ctx, 0);
}
int curvine_spdk_ns_submit_flush(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair,
                                struct curvine_async_ctx *ctx) {
    return spdk_nvme_ns_cmd_flush(ns, qpair, curvine_async_cb_fn, ctx);
}
int curvine_spdk_qpair_poll(struct spdk_nvme_qpair *qpair, int max_completions) {
    return spdk_nvme_qpair_process_completions(qpair, max_completions);
}
size_t curvine_spdk_async_ctx_sizeof(void) {
    return sizeof(struct curvine_async_ctx);
}
void curvine_spdk_async_ctx_init(struct curvine_async_ctx *ctx, curvine_async_cb cb, void *cb_arg) {
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    fprintf(stderr, "[DEBUG C] curvine_spdk_async_ctx_init: ctx=%p, cb=%p, cb_arg=%p\n", (void*)ctx, (void*)(uintptr_t)cb, cb_arg);
}

// Thread library init wrapper - uses smaller mempool size to avoid ENOMEM
// This is the function previously in spdk_thread_wrapper.c
int curvine_spdk_thread_lib_init(spdk_new_thread_fn new_thread_fn, size_t ctx_sz) {
    // Use smaller mempool size to avoid ENOMEM failures due to hugepage fragmentation
    size_t small_mempool_size = 4096;
    return spdk_thread_lib_init_ext(NULL, NULL, ctx_sz, small_mempool_size);
}