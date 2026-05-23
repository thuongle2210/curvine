# SPDK Multi-stage Compile Image
#
# Produces two runtime stages:
#   spdk-full   — full SPDK + RDMA for nvmf_tgt (target/storage nodes)
#   spdk-runtime — minimal SPDK shared libs for initiator/compute nodes
#
# Usage:
#   docker build --target spdk-full -t spdk-full:latest .
#   docker build --target spdk-runtime -t spdk-runtime:latest .

# ============================================================
# Stage 0: Install dependencies & clone SPDK
# ============================================================
FROM rockylinux:9 AS deps

ARG SPDK_VERSION=v25.09

# Mirror fix + EPEL
RUN for repo in /etc/yum.repos.d/*.repo; do \
        [ -f "$repo" ] || continue; \
        sed -i 's/^mirrorlist=/#mirrorlist=/g' "$repo"; \
        sed -i 's/^metalink=/#metalink=/g' "$repo"; \
        sed -i 's|^#baseurl=http://dl.rockylinux.org/$contentdir|baseurl=https://dl.rockylinux.org/pub/rocky|g' "$repo"; \
        sed -i 's|^#baseurl=https://download.example/pub/epel|baseurl=https://dl.fedoraproject.org/pub/epel|g' "$repo"; \
    done

RUN dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y git && \
    dnf clean all

# Clone SPDK with full tag history (needed for `git describe --tags` version detection)
RUN git clone --branch "$SPDK_VERSION" https://github.com/spdk/spdk.git /spdk
WORKDIR /spdk
RUN git submodule update --init --depth 1

# Install SPDK build deps via pkgdep.sh
RUN scripts/pkgdep.sh --rdma && \
    dnf clean all

# Build nasm from source (needed by ISA-L, not available in EPEL 9 for aarch64)
RUN curl -sL https://www.nasm.us/pub/nasm/releasebuilds/2.16.03/nasm-2.16.03.tar.gz | \
    tar -xz -C /tmp && \
    cd /tmp/nasm-2.16.03 && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    rm -rf /tmp/nasm-2.16.03

# Build isa-l from source (needed by SPDK, not available in all Rocky 9 arches)
RUN cd /tmp && \
    git clone --depth 1 --branch v2.31.1 https://github.com/intel/isa-l.git isa-l-src && \
    cd isa-l-src && \
    ./autogen.sh && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    cd / && rm -rf /tmp/isa-l-src

# ============================================================
# Stage 1: Build SPDK
# ============================================================
FROM deps AS build

ARG SPDK_VERSION
WORKDIR /spdk

RUN ./configure \
    --disable-tests \
    --with-vhost \
    --with-virtio \
    --with-sma \
    --with-vfio-user \
    --with-rdma \
    --target-arch=native \
    --with-shared && \
    make -j$(nproc) && \
    mkdir -p /opt/spdk && \
    cp -r ./* /opt/spdk/ && \
    ldconfig

# ============================================================
# Stage 2: Full runtime for NVMe-oF Target nodes
# ============================================================
FROM rockylinux:9 AS spdk-full

RUN for repo in /etc/yum.repos.d/*.repo; do \
        [ -f "$repo" ] || continue; \
        sed -i 's/^mirrorlist=/#mirrorlist=/g' "$repo"; \
        sed -i 's/^metalink=/#metalink=/g' "$repo"; \
        sed -i 's|^#baseurl=http://dl.rockylinux.org/$contentdir|baseurl=https://dl.rockylinux.org/pub/rocky|g' "$repo"; \
        sed -i 's|^#baseurl=https://download.example/pub/epel|baseurl=https://dl.fedoraproject.org/pub/epel|g' "$repo"; \
    done

RUN dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y \
    libaio numactl-libs libuuid ncurses-libs openssl-libs \
    python3 python3-pip \
    kmod pciutils procps-ng \
    libffi \
    rdma-core libibverbs librdmacm \
    fuse3-libs \
    && dnf clean all

COPY --from=build /opt/spdk /opt/spdk

RUN ldconfig && \
    mkdir -p /var/log/spdk /var/run/spdk

ENV SPDK_DIR=/opt/spdk
ENV LD_LIBRARY_PATH="/opt/spdk/build/lib:/opt/spdk/dpdk/build/lib:/opt/spdk/isa-l/.libs:/opt/spdk/isa-l-crypto/.libs:/opt/spdk/build/libvfio-user/usr/local/lib"
EXPOSE 4420

# ============================================================
# Stage 3: Minimal runtime for Initiator/Compute nodes
# ============================================================
FROM rockylinux:9 AS spdk-runtime

RUN for repo in /etc/yum.repos.d/*.repo; do \
        [ -f "$repo" ] || continue; \
        sed -i 's/^mirrorlist=/#mirrorlist=/g' "$repo"; \
        sed -i 's/^metalink=/#metalink=/g' "$repo"; \
        sed -i 's|^#baseurl=http://dl.rockylinux.org/$contentdir|baseurl=https://dl.rockylinux.org/pub/rocky|g' "$repo"; \
        sed -i 's|^#baseurl=https://download.example/pub/epel|baseurl=https://dl.fedoraproject.org/pub/epel|g' "$repo"; \
    done

RUN dnf install -y epel-release && \
    dnf config-manager --set-enabled crb && \
    dnf install -y \
    libaio numactl-libs libuuid openssl-libs python3 \
    rdma-core libibverbs librdmacm \
    nvme-cli kmod pciutils \
    && dnf clean all

# Only copy shared libs + scripts (not the full build tree)
COPY --from=build /opt/spdk/build/lib/*.so* /opt/spdk/build/lib/
COPY --from=build /opt/spdk/scripts /opt/spdk/scripts
COPY --from=build /opt/spdk/include /opt/spdk/include
COPY --from=build /opt/spdk/isa-l/.libs/*.so* /opt/spdk/isa-l/.libs/
COPY --from=build /opt/spdk/isa-l-crypto/.libs/*.so* /opt/spdk/isa-l-crypto/.libs/
RUN cp /opt/spdk/build/bin/spdk_bdev /opt/spdk/build/bin/ 2>/dev/null || true
RUN ldconfig

ENV SPDK_DIR=/opt/spdk

# ============================================================
# Stage 4: Build curvine-server (uses pre-built SPDK headers)
# ============================================================
FROM ghcr.io/curvineio/curvine-compile:rocky9 AS curvine-builder

LABEL stage="curvine-builder"

COPY --from=spdk-full /opt/spdk /opt/spdk
ENV SPDK_DIR=/opt/spdk
ENV PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig

COPY . /workspace/
WORKDIR /workspace

RUN --mount=type=cache,target=/workspace/target,sharing=locked \
    --mount=type=cache,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,target=/root/.cargo/git,sharing=locked \
    echo "Building curvine-server with SPDK-RDMA support..." && \
    export TMPDIR=/workspace/tmp && mkdir -p /workspace/tmp && \
    cargo build --release \
        -p curvine-server \
        --no-default-features \
        --features "spdk-rdma" && \
    mkdir -p /build-output && \
    cp target/release/curvine-server /build-output/ && \
    rm -rf /workspace/tmp && \
    echo "curvine-server built successfully"

# ============================================================
# Stage 5: Initiator runtime (curvine worker + SPDK initiator libs)
# ============================================================
FROM spdk-runtime AS initiator-runtime

LABEL stage="initiator-runtime"

RUN dnf install -y fuse3 procps-ng libuuid-devel && dnf clean all

COPY --from=curvine-builder /build-output/curvine-server /app/curvine/lib/curvine-server
COPY curvine-docker/deploy/spdk/entrypoint-initiator.sh /entrypoint-initiator.sh
RUN chmod +x /entrypoint-initiator.sh && \
    chmod +x /app/curvine/lib/curvine-server

ENV CURVINE_HOME=/app/curvine
ENV PATH=$PATH:$CURVINE_HOME/bin
ENV LD_LIBRARY_PATH="/opt/spdk/build/lib:/opt/spdk/dpdk/build/lib:/opt/spdk/isa-l/.libs:/opt/spdk/isa-l-crypto/.libs:/usr/local/lib64:/usr/local/lib"

RUN mkdir -p /app/curvine/logs /app/curvine/conf /app/curvine/bin

ENTRYPOINT ["/entrypoint-initiator.sh"]

# ============================================================
# Stage 6: Target runtime (SPDK NVMe-oF target)
# ============================================================
FROM spdk-full AS target-runtime

LABEL stage="target-runtime"

COPY curvine-docker/deploy/spdk/entrypoint-target.sh /entrypoint-target.sh
RUN chmod +x /entrypoint-target.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD /opt/spdk/scripts/rpc.py -s /var/tmp/spdk.sock rpc_get_methods >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/entrypoint-target.sh"]
