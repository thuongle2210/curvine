#!/bin/bash
# Linker wrapper that forces -lstdc++, -lgcc_s, and -lz to link statically.
# Needed because crate build scripts (e.g. librocksdb-sys) emit
# `cargo:rustc-link-lib=stdc++` which lands in the -Bdynamic section.
#
# Also adds --allow-multiple-definition to tolerate SPDK/DPDK archives
# being listed both via -l (for transitive propagation) and via
# --whole-archive (for constructor retention).  The duplicate object
# files resolve to the same symbols and the extra definitions are harmless.
#
# NOTE: gcc/clang support comma-separated `-Wl,...` lists. This wrapper
# emits `-Wl,-Bstatic` / `-Wl,-Bdynamic` as separate args around the
# affected libraries to preserve the intended linker mode transitions.

set -euo pipefail

NEWARGS=()
for arg in "$@"; do
    if [ "$arg" = "-lstdc++" ]; then
        NEWARGS+=("-Wl,-Bstatic" "-lstdc++" "-Wl,-Bdynamic")
    elif [ "$arg" = "-lgcc_s" ]; then
        # libgcc_s.so provides both the GCC runtime helpers and the EH
        # (exception-handling) unwinder. GCC does not ship a libgcc_s.a;
        # the static equivalents are libgcc.a (runtime) + libgcc_eh.a (EH).
        NEWARGS+=("-Wl,-Bstatic" "-lgcc_eh" "-lgcc" "-Wl,-Bdynamic")
    elif [ "$arg" = "-lz" ]; then
        NEWARGS+=("-Wl,-Bstatic" "-lz" "-Wl,-Bdynamic")
    else
        NEWARGS+=("$arg")
    fi
done

# Add --allow-multiple-definition to tolerate SPDK/DPDK whole-archive + -l overlap
NEWARGS+=("-Wl,--allow-multiple-definition")

exec "${CC:-cc}" "${NEWARGS[@]}"
