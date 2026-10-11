# Compile benchmark: standard operating procedure

This folder benchmarks how long NautilusTrader takes to rebuild and retest after a realistic
edit to one adapter, and which settings make that loop fastest. It runs identically on Linux
(EC2, cloud dev boxes) and macOS, and it is written so a fresh agent session can pick it up from
this file alone (see "Hand-off for another agent session" at the end).

| File | Purpose |
|---|---|
| `bench_nautilus.sh` | Runs the scenarios through hyperfine and stores JSON/Markdown/logs. |
| `compare_results.py` | Puts several result folders side by side (first = reference). |
| `hyperliquid-edit.patch` | The edit: 3 files, 216 lines in `crates/adapters/hyperliquid` (src + integration tests). |
| `variant-hyperliquid-gate-bins.patch` | Optional adapter build tweak (see findings): its 11 CLI binaries need `--features bins`. |
| `BASE_COMMIT` | The commit every machine benchmarks from. The patches apply cleanly on it. |
| `results/<machine>-<date>/` | Committed results, one folder per machine (and per variant). |

`.gitignore` ignores `*.sh` and `*.patch`, so `git add -f` is needed when committing those files
or a results folder (it contains `change.patch`).

## What is measured

**The edit.** The reverse of upstream commit `95feaf3` ("Fix Hyperliquid order closure after
failed modifies") on `src/execution.rs`, `src/websocket/dispatch.rs` and
`tests/integration/dispatch.rs`: 10 lines added, 206 removed. Later commits reworked the unit-test
module, so the patch keeps the base commit's tests minus the one case that asserts the reverted
behaviour; all 1438 Hyperliquid tests pass on the edited tree (the script checks this before
timing). Each timed run applies the patch on top of `BASE_COMMIT` and appends a unique comment to
every edited file, so no cache can replay an earlier run. HEAD never moves (the `nautilus-common`
build script embeds the git hash, so moving HEAD would rebuild most of the workspace).

**The scenarios.** Each is timed by hyperfine after an untimed `--prepare` step that restores the
base tree, builds it, and applies the edit.

| Scenario | Timed command |
|---|---|
| `test-crate-incremental` | `cargo nextest run -p nautilus-hyperliquid --lib --tests --features <crate set> --cargo-profile nextest --build-jobs N --test-threads N` |
| `test-crate-tuned` | same, `--test-threads 4×N` |
| `test-crate-sccache` | same as `-incremental`, with `RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0` (what `make cargo-test-crate-nautilus-hyperliquid` does when sccache is installed, plus the integration tests) |
| `test-crate-cold` | same, but `target/nextest` is emptied first while sccache is warm (fresh worktree / CI) |
| `test-ws-incremental` | `cargo nextest run --workspace --lib --tests --features <base set> --cargo-profile nextest -E 'package(nautilus-hyperliquid)'` |
| `test-ws-sccache` | same, with `RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0` (what `make cargo-test` does when sccache is installed) |
| `debug-crate` | `cargo build -p nautilus-hyperliquid --features <crate set>` (dev profile) |
| `debug-ws` | `cargo build --workspace --features <base set>` (dev profile) |
| `release` | `cargo clean --release` (untimed), then `cargo build --release --workspace --features <base set>` (fat LTO, `codegen-units=1`) |

`N` = number of logical CPUs. `<base set>` = `arrow,ffi,python,high-precision,streaming,defi`
(the Makefile's `scripts/cargo-features.bash`). `<crate set>` =
`scripts/crate-test-features.sh nautilus-hyperliquid` =
`arrow,examples,high-precision,nautilus-serialization,pyo3,pyo3-async-runtimes,pyo3-stub-gen,python`,
the set `make cargo-test-crate-<pkg>` uses.

All `test-*` scenarios run the same Hyperliquid unit + integration tests, so differences between
them are build cost, except `test-crate-tuned`, which also changes how the tests are scheduled.
`compare_results.py` splits every test scenario into cargo's build time and nextest's run time.

## Procedure (identical on every machine)

### 1. Prerequisites

```bash
# Linux (Debian/Ubuntu, e.g. EC2):
sudo apt-get update && sudo apt-get install -y build-essential clang lld jq git curl python3-dev pkg-config
# macOS:
xcode-select --install; brew install jq python@3.13

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
# macOS: brew install hyperfine sccache cargo-nextest   (prebuilt, fastest)
# anywhere (about 15 minutes to compile):
cargo install --locked hyperfine cargo-nextest sccache
```

The repo's `rust-toolchain.toml` pins the toolchain (1.99.0 at `BASE_COMMIT`); rustup installs it
on first use. Results here used hyperfine 2.0.0, cargo-nextest 0.9.148 and sccache 0.18.0.
hyperfine 2.x changed its JSON format; `compare_results.py` reads both 1.x and 2.x.

- **EC2.** Avoid burstable `t` instances. On Intel/AMD a vCPU is half a physical core (Graviton:
  a full core). Allow about 2 GB RAM per build job. Disk: the adapter scenarios need about 25 GB
  for `target/`, a workspace test build considerably more; give the volume 100 GB+ or run the
  workspace scenarios one at a time with `rm -rf target` in between (as the cloud run did).
- **macOS.** Keep it plugged in, exclude the checkout from Spotlight, and add Terminal under
  System Settings → Privacy & Security → Developer Tools (stops a scan of every new binary).
- **PyO3.** The script uses `python/.venv/bin/python` if it exists (after `make sync`), otherwise
  `python3` from `PATH`, and sets `PYTHONHOME` and `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH` to match.
  Keep the same interpreter for the whole run, or PyO3's build script reruns.

### 2. Get the benchmark kit and a dedicated checkout

```bash
git clone https://github.com/JiajunWan/nautilus_trader.git nt-kit
cd nt-kit && git checkout claude/eager-allen-rpc4c5
# A clean, separate checkout for the benchmark (the script refuses a dirty tree):
git worktree add --detach ../nt-bench "$(cat scripts/compile-bench/BASE_COMMIT)"
```

### 3. Run

Run inside `tmux` (or `nohup`) on remote machines. Pick a short `MACHINE_LABEL`.

```bash
cd nt-kit/scripts/compile-bench
export REPO_DIR=$(cd ../../../nt-bench && pwd) MACHINE_LABEL=<e.g. mac-m4max or ec2-c7i.8xlarge>
RES=$PWD/results/$MACHINE_LABEL-$(date +%Y%m%d)

# (a) Adapter dev loop: the scenarios that matter day to day (about 1 hour).
RESULTS_DIR=$RES SCENARIOS=test-crate-incremental,test-crate-tuned,debug-crate,test-crate-sccache,test-crate-cold \
  ./bench_nautilus.sh 2>&1 | tee $RES-a.log

# (b) The same adapter loop with the bin-gating variant (separate results folder).
#     PKG_FEATURES must be given explicitly, or crate-test-features.sh would enable `bins`.
RESULTS_DIR=$RES-gate-bins MACHINE_LABEL=$MACHINE_LABEL+gate-bins \
  VARIANT_PATCH=$PWD/variant-hyperliquid-gate-bins.patch \
  PKG_FEATURES=arrow,examples,high-precision,nautilus-serialization,pyo3,pyo3-async-runtimes,pyo3-stub-gen,python \
  SCENARIOS=test-crate-incremental,test-crate-tuned,debug-crate ./bench_nautilus.sh 2>&1 | tee $RES-b.log

# (c) Workspace-wide edit loop (each run first rebuilds the base state, untimed; hours).
rm -rf "$REPO_DIR/target"
RESULTS_DIR=$RES SCENARIOS=test-ws-incremental,test-ws-sccache RUNS_TEST=3 ./bench_nautilus.sh 2>&1 | tee $RES-c.log
rm -rf "$REPO_DIR/target"
RESULTS_DIR=$RES SCENARIOS=debug-ws RUNS_DEBUG=3 ./bench_nautilus.sh 2>&1 | tee $RES-d.log

# (d) Clean release build. -j 1 takes many hours; drop "1," to skip it.
rm -rf "$REPO_DIR/target"
RESULTS_DIR=$RES SCENARIOS=release JOBS_LIST=1,$(getconf _NPROCESSORS_ONLN) RUNS_RELEASE=1 \
  ./bench_nautilus.sh 2>&1 | tee $RES-e.log
```

The cloud run used `RUNS_TEST=5 RUNS_DEBUG=5` for (a) and (b), `RUNS_TEST=3`/`RUNS_DEBUG=3` for
(c), and `JOBS_LIST=4 RUNS_RELEASE=1` for (d). Use the same counts, or more, so the comparison
is fair. Every knob is listed in `./bench_nautilus.sh help`.

### 4. Compare and save

```bash
python3 compare_results.py results/cloud-4vcpu-xeon-20261011 results/<your-dir> results/<your-dir>-gate-bins \
  --out results/compare-<your-label>.md
git add -f results && git commit -m "Add compile benchmark results for <machine>" && git push
```

The first folder is the reference: every row shows "N× faster/slower" relative to it.

## Results: cloud dev box (4 vCPU Xeon @ 2.1 GHz, 15.7 GiB RAM, KVM, no swap)

Raw data: `results/cloud-4vcpu-xeon-20261011/` and `results/cloud-4vcpu-xeon-gate-bins-20261011/`.
Means over 5 runs unless noted. "Build / tests" is cargo's build time and nextest's run time.

### Adapter edit → build → test loop (the thing to optimize)

| Setup | Build | Tests | Total | vs `make` default |
|---|---|---|---|---|
| `test-crate-sccache` (sccache + `CARGO_INCREMENTAL=0`, what `make cargo-test-crate-*` does when sccache is installed) | 43.3 s | 49.4 s | 93.6 s | 1.00× |
| `test-crate-incremental` (no sccache, incremental) | 24.4 s | 49.1 s | 74.4 s | 1.26× faster |
| `test-crate-tuned` (+ `--test-threads 16`) | 28.1 s | 21.5 s | 50.6 s | 1.85× faster |
| `test-crate-tuned` + bin-gating variant | 13.8 s | 21.5 s | **36.2 s** | **2.59× faster** |

| Other adapter scenarios | Result |
|---|---|
| `debug-crate` (`cargo build -p`, dev) | 19.9 s at `-j 4`, 33.5 s at `-j 1` |
| `debug-crate` + bin-gating variant | **7.7 s** at `-j 4` (2.6× faster), 8.5 s at `-j 1` |
| `test-crate-cold` (empty `target/`, warm sccache) | 2 m 44 s (build 1 m 55 s, 99.9% sccache hits) |
| First build from nothing, no sccache (untimed warm-up) | 12 m 56 s for the same targets |
| Test run alone vs `--test-threads` (build already done, 3 runs) | 4: 50.6 s, 8: 29.0 s, 16: 23.1 s, 32: 21.0 s; no failures or retries |

Peak RSS of any single process stayed at about 2.2 GiB in all adapter scenarios.

### Workspace-wide scenarios

This sandbox could not run them. That is itself a sizing data point for the other machines:

| Scenario | Outcome on 4 vCPU / 15.7 GiB / ~40 GB disk |
|---|---|
| `test-ws-incremental`, `test-ws-sccache` | `No space left on device` while building the workspace test binaries (27 GB free at start) |
| `debug-ws` | `No space left on device` during the first (untimed) workspace dev build |
| `release` at `-j 4` and `-j 2` | rustc killed by the OOM killer: two concurrent fat-LTO links (`nautilus` CLI, `python-stub-gen`, about 7 GB each) exceeded the 13.4 GiB memory cgroup |
| `release` at `-j 1` | RELEASE_J1_RESULT |

Logs are in `results/cloud-4vcpu-xeon-20261011/failed-attempts/`. For the workspace scenarios,
plan on 100 GB+ free disk. For a release build at `-j N`, plan on about 7–8 GB of RAM per
concurrent final LTO link, on top of the normal 1–2 GB per job.

## Findings

1. **For the edit loop, sccache makes things slower.** With sccache installed, the Makefile sets
   `CARGO_INCREMENTAL=0`. After an adapter edit, sccache cannot help: in `test-crate-sccache`
   only 1 of 14 rustc calls was cacheable (the edited library, a guaranteed miss), and the 13
   test and bin crates are not cacheable by sccache at all. Meanwhile the edited crate loses
   incremental compilation, so its rebuild goes from 24 s to 43 s.
2. **sccache pays off for a cold `target/`** (new worktree, `cargo clean`, branch switches that
   touch dependencies, CI): 1 m 55 s instead of about 13 min to build the adapter's test
   targets, with 99.9% cache hits.
3. **Test scheduling dominates the adapter's test time.** Hyperliquid's integration tests mostly
   wait on timers, reconnects and mock sockets: 49 s at one test per core, 21–23 s at
   16–32 threads, with no failures or retries. The floor is the slowest single test (about
   11.5 s, `exec_client::test_reconnect_reads_history_after_subscription_wait_times_out`).
   `.config/nextest.toml` already retries the scheduling-sensitive tests, and the `ci` profile
   keeps 2 threads for 4-vCPU CI runners, so this is a local-only setting.
4. **The adapter's 11 CLI binaries sit on the critical path.** Cargo builds every `[[bin]]` of a
   package before running its integration tests, and `cargo build -p` builds them too, even
   though they are `test = false` and no test uses `CARGO_BIN_EXE_*`. Each takes 3.7–6 s to
   link, and the integration-test binary waits for all of them (`--timings`). Gating them
   behind a feature (`variant-hyperliquid-gate-bins.patch`: `required-features = ["bins"]` on
   each `[[bin]]`, plus `bins = []`) takes the build part of the loop from 24–28 s to 14 s and
   `cargo build -p` from 20 s to 8 s.
5. **Peak memory is not the constraint** for the adapter loop (about 2.2 GiB per process). Disk
   is: `target/` reached 25 GB after the adapter scenarios, and a workspace-wide test or dev
   build did not fit in 27 GB.
6. **Fat-LTO release builds are memory-bound when run in parallel.** The final links of the
   binaries and the Python extension take about 7 GB each. Several can run at once at `-j ≥ 2`,
   which got the build OOM-killed on a 13.4 GiB limit.

## Recommendations for the edit → build → test loop

No repository change needed (1–3):

1. **Build and test only the adapter you changed**, including its integration tests:
   ```bash
   cargo nextest run -p nautilus-hyperliquid --lib --tests --cargo-profile nextest \
     --features "$(scripts/crate-test-features.sh nautilus-hyperliquid)" --test-threads $(( $(getconf _NPROCESSORS_ONLN) * 4 ))
   ```
   (`make cargo-test-crate-<pkg>` runs only `--lib`, so it skips the integration tests.)
2. **Keep incremental compilation on for the loop: disable sccache for it.** Either
   `make cargo-test-crate-nautilus-hyperliquid SCCACHE=`, or run cargo directly with
   `RUSTC_WRAPPER= CARGO_INCREMENTAL=1`. Keep sccache for cold builds: fresh worktrees, CI,
   `cargo clean`.
3. **Run I/O-bound adapter tests with more threads than cores**: `--test-threads` at about 4× the
   logical CPUs (`NEXTEST_TEST_THREADS=16` on 4 vCPUs). Leave the `ci` profile alone.

Optional adapter change (4):

4. **Gate the adapter's CLI binaries behind a feature**, as in
   `variant-hyperliquid-gate-bins.patch`. It is a small, adapter-local `Cargo.toml` change:
   users would then run `cargo run -p nautilus-hyperliquid --features bins --bin <name>`.
   Caveat: `scripts/crate-test-features.sh` enables every feature except `default` and
   `extension-module`, so `make cargo-test-crate-<pkg>` would still build the binaries unless
   that script also excludes `bins`.

The result is 36 s per adapter edit → build → test cycle on 4 vCPUs, versus 94 s with the
Makefile's default sccache setup.

## Hand-off for another agent session

- Branch `claude/eager-allen-rpc4c5` of `JiajunWan/nautilus_trader`; everything is in
  `scripts/compile-bench/`. Production code is unchanged; the only adapter change is the
  optional `variant-hyperliquid-gate-bins.patch`, applied only inside the benchmark checkout.
- To benchmark a new machine, follow "Procedure" steps 1–4 above exactly, with the same
  `BASE_COMMIT`, patches and run counts, then compare against `results/cloud-4vcpu-xeon-20261011`.
- The script refuses to start on a dirty `REPO_DIR` and always restores it on exit. If a run is
  killed, run `git -C $REPO_DIR checkout -- .` before restarting.
- Gotchas met so far:
  - hyperfine 2.x needs `--shell=default` for ad-hoc commands with redirects (`-S` alone takes
    the next word as the shell). The script's own commands avoid shell syntax.
  - EC2 metadata (169.254.169.254) is only used for the machine label. Behind a proxy it can
    return an error page; the script now ignores anything that is not an instance-type string.
  - Don't `pkill -f bench_nautilus.sh` from a shell whose own command line contains that string.
