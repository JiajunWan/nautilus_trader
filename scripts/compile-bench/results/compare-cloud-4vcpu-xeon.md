# NautilusTrader compile benchmark comparison

## Machines

| Machine | Logical CPUs | CPU | rustc | Base commit | Package | Edit |
|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 4 | Intel(R) Xeon(R) Processor @ 2.10GHz | rustc 1.99.0 (b940084d7 2026-09-28) | f9487c5935 | nautilus-hyperliquid | 3 .rs files, 216 lines |
| cloud-4vcpu-xeon+gate-bins | 4 | Intel(R) Xeon(R) Processor @ 2.10GHz | rustc 1.99.0 (b940084d7 2026-09-28) | f9487c5935 | nautilus-hyperliquid | 3 .rs files, 216 lines |

## test-crate-incremental: Edit -> build + run one crate's tests, incremental (recommended dev loop)

| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs cloud-4vcpu-xeon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 4 | 5 | 74.4s | 0.2s | 74.2s | 1m 50.0s | 1.5 | 2.2 GiB | 24.4s / 49.1s | - | - |
| cloud-4vcpu-xeon+gate-bins | 4 | 5 | 64.3s | 0.3s | 64.0s | 74.0s | 1.2 | 2.1 GiB | 13.7s / 49.6s | - | 1.16x faster |

## test-crate-tuned: test-crate-incremental with nextest --test-threads = 4 x cores (I/O-bound integration tests)

| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs cloud-4vcpu-xeon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 4 | 5 | 50.6s | 3.0s | 47.4s | 1m 55.8s | 2.3 | 2.1 GiB | 28.1s / 21.5s | - | - |
| cloud-4vcpu-xeon+gate-bins | 4 | 5 | 36.2s | 0.6s | 35.5s | 73.1s | 2.0 | 2.1 GiB | 13.8s / 21.5s | - | 1.40x faster |

## test-crate-sccache: Edit -> build + run one crate's tests, sccache + CARGO_INCREMENTAL=0 (make cargo-test-crate-<pkg> style)

| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs cloud-4vcpu-xeon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 4 | 5 | 1m 33.6s | 1.8s | 1m 31.6s | n/a | n/a | 2.2 GiB | 43.3s / 49.4s | - | - |

## test-crate-cold: Edit -> empty target dir, warm sccache (fresh worktree / CI), one crate's tests

| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs cloud-4vcpu-xeon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 4 | 5 | 2m 44.5s | 3.2s | 2m 41.8s | n/a | n/a | 2.1 GiB | 1m 54.6s / 48.7s | - | - |

## debug-crate: Edit -> incremental dev build of one crate (cargo build -p)

| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs cloud-4vcpu-xeon |
|---|---|---|---|---|---|---|---|---|---|---|---|
| cloud-4vcpu-xeon | 1 | 5 | 33.5s | 0.3s | 33.2s | 46.3s | 1.4 | 2.0 GiB | - | - | - |
| cloud-4vcpu-xeon | 4 | 5 | 19.9s | 0.6s | 19.1s | 45.6s | 2.3 | 2.0 GiB | - | 1.68x | - |
| cloud-4vcpu-xeon+gate-bins | 1 | 5 | 8.5s | 0.5s | 7.9s | 8.3s | 1.0 | 1.5 GiB | - | - | 3.96x faster |
| cloud-4vcpu-xeon+gate-bins | 4 | 5 | 7.7s | 0.2s | 7.5s | 8.7s | 1.1 | 1.5 GiB | - | 1.10x | 2.58x faster |

*Cores busy* = (user + system CPU time) / wall time. A value far below the -j count means the build was serialized somewhere (dependency chains, the fat-LTO step, linking). CPU time is not reported for sccache scenarios because the sccache server does the compiling outside cargo's process tree. *Peak RSS* is the largest resident set of a single process in the timed command's tree (hyperfine 2.x), not total machine memory. *Build / tests* splits the mean into cargo's reported build time and nextest's test-run time (from the logs).
