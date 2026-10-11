# Compile benchmark: standard operating procedure

This folder benchmarks how long NautilusTrader takes to rebuild and retest after a realistic
edit to one adapter. It runs identically on Linux (EC2, cloud dev boxes) and macOS, and it is
written so a fresh agent session can pick it up from this file alone.

| File | Purpose |
|---|---|
| `bench_nautilus.sh` | Runs the scenarios through hyperfine and stores JSON/Markdown/logs. |
| `compare_results.py` | Puts several machines' result folders side by side (first = reference). |
| `hyperliquid-edit.patch` | The edit: 3 files, 300 lines in `crates/adapters/hyperliquid` (src + integration tests). |
| `BASE_COMMIT` | The commit every machine benchmarks from. The patch applies cleanly on it. |
| `results/<machine>-<timestamp>/` | Committed results, one folder per machine run. |

## What is measured

The edit is the reverse diff of upstream commit `95feaf3` ("Fix Hyperliquid order closure after
failed modifies"): `src/execution.rs`, `src/websocket/dispatch.rs` and
`tests/integration/dispatch.rs`, 24 lines added and 276 removed. Each timed run applies it on top
of `BASE_COMMIT` with a unique trailing comment per edited file, so no cache can replay an earlier
run. HEAD never moves (the `nautilus-common` build script embeds the git hash).

| Scenario | Timed command (after an untimed base build + applying the edit) |
|---|---|
| `release` | `cargo clean --release` (untimed), then `cargo build --release --workspace --features <base set>` |
| `debug-ws` | `cargo build --workspace --features <base set>` (dev profile, incremental) |
| `debug-crate` | `cargo build -p nautilus-hyperliquid --features <crate set>` (dev profile, incremental) |
| `test-ws-incremental` | `cargo nextest run --workspace --lib --tests --cargo-profile nextest -E 'package(nautilus-hyperliquid)'` |
| `test-ws-sccache` | same, with `RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0` (what `make cargo-test` does when sccache is installed) |
| `test-crate-incremental` | `cargo nextest run -p nautilus-hyperliquid --lib --tests --features <crate set> --cargo-profile nextest` |
| `test-crate-sccache` | same, with `RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0` (what `make cargo-test-crate-nautilus-hyperliquid` does when sccache is installed, plus integration tests) |
| `test-crate-cold` | same as above but `target/nextest` is emptied first while sccache is warm (fresh worktree / CI) |

`<base set>` is `arrow,ffi,python,high-precision,streaming,defi` (the Makefile's
`scripts/cargo-features.bash`). `<crate set>` is `scripts/crate-test-features.sh nautilus-hyperliquid`
(`arrow,examples,high-precision,nautilus-serialization,pyo3,pyo3-async-runtimes,pyo3-stub-gen,python`),
the same set `make cargo-test-crate-<pkg>` uses.

Every `test-*` scenario runs the same tests (Hyperliquid unit + integration tests), so the
differences between them are build cost only.

## Procedure (identical on every machine)

### 1. Prerequisites

Rust is installed by `rustup` from the repo's `rust-toolchain.toml` on first use.

```bash
# Linux (Debian/Ubuntu, e.g. EC2):
sudo apt-get update && sudo apt-get install -y build-essential clang lld jq git curl python3-dev pkg-config
# macOS:
xcode-select --install; brew install jq python@3.13

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
# Prebuilt binaries are fastest (brew install hyperfine sccache cargo-nextest on macOS);
# building from crates.io works everywhere:
cargo install --locked hyperfine cargo-nextest sccache
```

EC2 notes: avoid burstable `t` instances; on Intel/AMD a vCPU is half a physical core
(Graviton: a full core); allow about 2 GB RAM per build job and 60 GB+ free disk
(a workspace dev + test + release target dir is large; `FREE_DISK=1`, the default, deletes
`target/debug` and `target/nextest` before the release scenario).

macOS notes: keep it plugged in, exclude the checkout from Spotlight, and add Terminal under
System Settings → Privacy & Security → Developer Tools (stops a scan of every new binary).

### 2. Get the benchmark kit and a dedicated checkout

```bash
git clone https://github.com/JiajunWan/nautilus_trader.git nt-kit
cd nt-kit && git checkout claude/eager-allen-rpc4c5
# A clean, separate checkout for the benchmark (the script refuses a dirty tree):
git worktree add --detach ../nt-bench "$(cat scripts/compile-bench/BASE_COMMIT)"
```

### 3. Run

```bash
cd nt-kit/scripts/compile-bench
REPO_DIR=$PWD/../../../nt-bench MACHINE_LABEL="<short name, e.g. mac-m3max or ec2-c7i.4xlarge>" \
  ./bench_nautilus.sh 2>&1 | tee bench-$(date +%Y%m%d-%H%M%S).log
```

Useful knobs (environment variables, see `./bench_nautilus.sh help`):

- `SCENARIOS=test-crate-incremental,debug-crate` runs a subset (the order of execution is fixed:
  per-crate tests, per-crate build, workspace tests/build, cold, then release).
- `JOBS_LIST=1,<cores>` (default) times release/debug at one job and all cores. `-j 1` release is
  many hours on most machines; `JOBS_LIST=<cores>` skips it.
- `RUNS_RELEASE=3 RUNS_DEBUG=5 RUNS_TEST=5` are the defaults.
- `PACKAGE`, `PATCH_FILE`, `BASE_REF` change the adapter / edit / base (keep them identical
  across machines being compared).

Expect: per-crate scenarios take minutes, workspace scenarios tens of minutes (each run first
rebuilds the base state, untimed), release with `-j 1` can take hours. Use `tmux`/`nohup` on
remote machines.

### 4. Compare and save

```bash
python3 compare_results.py results/<reference-dir> results/<other-dir> ... --out results/compare.md
git add results && git commit -m "Add compile benchmark results for <machine>" && git push
```

## Findings so far

See `results/` for raw data, and the "Recommendations" section below.

## Recommendations for the edit → build → test loop

(filled in from measured results; see below)
