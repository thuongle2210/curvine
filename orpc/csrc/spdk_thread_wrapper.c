#include "spdk/thread.h"
#include "spdk/env.h"
#include <stdio.h>
#include <stdlib.h>

// Wrapper for spdk_thread_lib_init that uses spdk_thread_lib_init_ext
// with a smaller mempool size to avoid ENOMEM failures due to hugepage fragmentation
int curvine_spdk_thread_lib_init(spdk_new_thread_fn new_thread_fn, size_t ctx_sz) {
    fprintf(stderr, "[DEBUG C] curvine_spdk_thread_lib_init: new_thread_fn=%p, ctx_sz=%zu\n",
            (void*)new_thread_fn, ctx_sz);

    // SPDK_DEFAULT_MSG_MEMPOOL_SIZE = 262143 is too large and may fail
    // Use a much smaller size sufficient for testing (4096 messages)
    size_t small_mempool_size = 4096;

    fprintf(stderr, "[DEBUG C] Calling spdk_thread_lib_init_ext with msg_mempool_size=%zu...\n",
            small_mempool_size);

    // Use spdk_thread_lib_init_ext to specify a smaller mempool
    // Pass NULL for thread_op_fn and thread_op_supported_fn since we don't need them
    int rc = spdk_thread_lib_init_ext(NULL, NULL, ctx_sz, small_mempool_size);
    fprintf(stderr, "[DEBUG C] spdk_thread_lib_init_ext returned: rc=%d\n", rc);
    return rc;
}
