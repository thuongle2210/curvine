# Curvine SPDK Docker Benchmark Guide

## Prerequisites

```bash
# On host (Vagrant/VM)
sudo sysctl -w vm.nr_hugepages=1024
sudo modprobe uio_pci_generic
```

## Build & Start Cluster

```bash
cd curvine-docker/deploy/spdk

# Start all 3 containers (master + target + worker)
sudo docker compose up --build -d

# Check status
sudo docker ps
# Expected: spdk-master, spdk-target, spdk-worker all Up

# Wait for master to be ready
sudo docker logs master --tail 5
# Look for: "Rpc server [curvine-master] start successfully"
```

## View Logs

```bash
# All containers at once
sudo docker compose logs

# Live streaming
sudo docker compose logs -f

# Individual containers
sudo docker logs master --tail 30
sudo docker logs target-1 --tail 30
sudo docker logs worker --tail 30
```

## Config Override

The cluster mounts config via `CURVINE_CONF_FILE` env var. Default: `./curvine-cluster.toml` in `curvine-docker/deploy/spdk/`.

```bash
# Use default config (shipped with repo)
sudo docker compose up --build -d

# Override with custom config
CURVINE_CONF_FILE=/path/to/your-config.toml \
  sudo docker compose up --build -d
```

## Build Benchmark Binary

```bash
# Bench is in curvine-tests crate (no SPDK features needed)
cargo build --release -p curvine-tests

# Result: ./target/release/curvine-bench
```

## Run Benchmarks

### Export config path for convenience

```bash
export CURVINE_BENCH_CONF=curvine-docker/deploy/spdk/curvine-cluster.toml

# Or use the example SPDK config
export CURVINE_BENCH_CONF=curvine-docker/deploy/example/conf/curvine-cluster-spdk.toml
```

### Write benchmark

```bash
sudo -E ./target/release/curvine-bench \
  --action fs.write \
  --conf "$CURVINE_BENCH_CONF" \
  --dir /bench-large \
  --file-num 4 \
  --file-size 200MB \
  --buf-size 4KB \
  --client-threads 50
```

### Read benchmark

```bash
sudo -E ./target/release/curvine-bench \
  --action fs.read \
  --conf "$CURVINE_BENCH_CONF" \
  --dir /bench-large \
  --file-num 4 \
  --file-size 200MB \
  --buf-size 4KB \
  --client-threads 50
```

### Benchmark options

| Flag | Default | Description |
|---|---|---|
| `--action` | (required) | `fs.write`, `fs.read`, `fuse.write`, `fuse.read` |
| `--conf` | (required) | Path to cluster TOML config |
| `--dir` | `/fuse-bench` | Directory path for test files |
| `--file-num` | `1` | Number of files |
| `--file-size` | `100MB` | Size of each file |
| `--buf-size` | `4KB` | I/O buffer size |
| `--client-threads` | `1` | Number of concurrent threads |
| `--delete-file` | `false` | Delete files after benchmark |
| `--checksum` | `true` | Enable data checksum verification |

## Expected Output

```
fs.write size: 800.0MB, cost: 1.34 s, speed: 595.6MB/s, bandwidth: 5.0Gbps
```

## Stop Cluster

```bash
cd curvine-docker/deploy/spdk && docker compose down

# Or with volume cleanup
cd curvine-docker/deploy/spdk && docker compose down -v
```
