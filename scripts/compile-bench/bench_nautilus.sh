#!/usr/bin/env bash
#
# bench_nautilus.sh - benchmark NautilusTrader's Rust compile performance with hyperfine.
#
# Runs identically on macOS (stock bash 3.2 is fine) and Linux (e.g. an EC2 instance).
# Run it on each machine with the SAME base commit, patch and settings, then compare the
# result folders with compare_results.py. See README.md in this folder for the full procedure.
#
# Quick start (defaults: base commit from ./BASE_COMMIT, edit = ./hyperliquid-edit.patch):
#   REPO_DIR=~/nt-bench ./bench_nautilus.sh                     # every scenario
#   REPO_DIR=~/nt-bench SCENARIOS=test-crate-incremental ./bench_nautilus.sh
#   ./bench_nautilus.sh find-commits                            # other real upstream edits
#
# Scenarios (SCENARIOS is a comma-separated subset; order of execution is fixed, see main):
#   release                 Clean `cargo build --release --workspace`. Fat LTO + codegen-units=1, so
#                           the tail is single-threaded. A cargo --timings HTML report is saved.
#   debug-ws                Edit -> incremental dev build of the whole workspace.
#   debug-crate             Edit -> incremental dev build of only $PACKAGE (`cargo build -p`).
#   test-ws-incremental     Edit -> build every workspace test binary (incremental), run $PACKAGE's tests.
#   test-ws-sccache         Same, configured like `make cargo-test` with sccache installed
#                           (RUSTC_WRAPPER=sccache, CARGO_INCREMENTAL=0).
#   test-crate-incremental  Edit -> build only $PACKAGE's lib + integration tests (incremental), run them.
#   test-crate-tuned        test-crate-incremental with --test-threads $TUNED_TEST_THREADS (default 4 x cores):
#                           most adapter integration tests wait on timers and sockets, not CPU.
#   test-crate-sccache      Same as test-crate-incremental, sccache + CARGO_INCREMENTAL=0 (what `make cargo-test-crate-<pkg>`
#                           does when sccache is installed, plus the integration tests).
#   test-crate-cold         Edit -> EMPTY target dir, sccache warm from the base commit
#                           (fresh worktree / CI runner) -> build and run $PACKAGE's tests.
#
# Every test scenario runs the same tests ($PACKAGE's unit + integration tests), so differences
# between them are build cost only.
#
# How "the edit" works: PATCH_FILE is applied to the working tree before each timed run and
# reverted afterwards. HEAD never moves, because nautilus-common's build script embeds the git
# commit hash and rebuilds when HEAD changes. A unique comment is appended to every edited .rs
# file on each run, so neither sccache nor the incremental cache can replay a previous run.
#
# Settings (environment variables):
#   REPO_DIR          clean NautilusTrader checkout dedicated to benchmarking (default: ./nautilus_trader)
#   BASE_REF          commit to benchmark from          (default: contents of ./BASE_COMMIT)
#   PATCH_FILE        the edit, applied on BASE_REF     (default: ./hyperliquid-edit.patch)
#   CHANGE_COMMIT     alternative edit: the diff of an upstream commit (BASE_REF defaults to its parent)
#   PACKAGE           crate the -crate scenarios build/test, and whose tests every test scenario runs
#                     (default: nautilus-hyperliquid)
#   PKG_FEATURES      features for the -crate scenarios (default: scripts/crate-test-features.sh $PACKAGE,
#                     i.e. the same set `make cargo-test-crate-<pkg>` uses)
#   FEATURES          workspace features for release / -ws scenarios, or "all" (default: Makefile base set)
#   SCENARIOS         default: all nine above
#   JOBS_LIST         -j values for release / debug-* scenarios (default: 1,<cores>)
#   TEST_JOBS_LIST    -j values for test-* scenarios           (default: <cores>)
#   RUNS_RELEASE, RUNS_DEBUG, RUNS_TEST                         (default: 3, 5, 5)
#   TUNED_TEST_THREADS nextest --test-threads for test-crate-tuned       (default: 4 x cores)
#   VARIANT_PATCH     optional patch applied as part of the BASE state (e.g. an adapter build tweak);
#                     use a separate RESULTS_DIR / MACHINE_LABEL for each variant
#   FREE_DISK         1 = delete target/debug and target/nextest before the release scenario (default: 1)
#   RESULTS_DIR       where JSON/Markdown/logs go           (default: ./results/<machine>-<timestamp>)
#   MACHINE_LABEL     name shown by compare_results.py      (default: auto-detected)
#   SCCACHE_BENCH_DIR isolated sccache cache for the benchmark (default: ~/.cache/nautilus-bench-sccache)

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
HERE="$(dirname "$SELF")"
OS="$(uname -s)"

ALL_SCENARIOS="release,debug-ws,debug-crate,test-ws-incremental,test-ws-sccache,test-crate-incremental,test-crate-tuned,test-crate-sccache,test-crate-cold"

log()  { printf '\n==> %s\n' "$*" >&2; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

ncpu() {
  getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1
}

# ------------------------------------------------------------------------------------------------
# Configuration. Everything is exported so the helper subcommands hyperfine calls see the same values.
# ------------------------------------------------------------------------------------------------
load_config() {
  NCPU="${NCPU:-$(ncpu)}"
  REPO_DIR="${REPO_DIR:-$PWD/nautilus_trader}"
  FEATURES="${FEATURES:-arrow,ffi,python,high-precision,streaming,defi}"
  PACKAGE="${PACKAGE:-nautilus-hyperliquid}"
  SCENARIOS="${SCENARIOS:-$ALL_SCENARIOS}"
  JOBS_LIST="${JOBS_LIST:-1,$NCPU}"
  TEST_JOBS_LIST="${TEST_JOBS_LIST:-$NCPU}"
  RUNS_RELEASE="${RUNS_RELEASE:-3}"
  RUNS_DEBUG="${RUNS_DEBUG:-5}"
  RUNS_TEST="${RUNS_TEST:-5}"
  FREE_DISK="${FREE_DISK:-1}"
  TUNED_TEST_THREADS="${TUNED_TEST_THREADS:-$((NCPU * 4))}"
  VARIANT_PATCH="${VARIANT_PATCH:-}"
  SCCACHE_BENCH_DIR="${SCCACHE_BENCH_DIR:-$HOME/.cache/nautilus-bench-sccache}"
  TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_DIR/target}"
  export NCPU REPO_DIR FEATURES PACKAGE SCENARIOS JOBS_LIST TEST_JOBS_LIST RUNS_RELEASE RUNS_DEBUG \
    RUNS_TEST FREE_DISK TUNED_TEST_THREADS VARIANT_PATCH SCCACHE_BENCH_DIR TARGET_DIR
}

# Cargo arguments shared by every scenario, so untimed preparation builds and timed builds
# always agree (any difference would make cargo rebuild more than the edit requires).
cargo_arg_arrays() {
  if [ "$FEATURES" = "all" ]; then
    FEATURE_ARGS=(--all-features)
  else
    FEATURE_ARGS=(--features "$FEATURES")
  fi
  PKG_FEATURE_ARGS=()
  [ -n "${PKG_FEATURES:-}" ] && PKG_FEATURE_ARGS=(--features "$PKG_FEATURES")
  local nextest_common=(--cargo-profile nextest --status-level fail --final-status-level flaky)
  BUILD_WS=(--locked --workspace "${FEATURE_ARGS[@]}")
  BUILD_CRATE=(--locked -p "$PACKAGE" ${PKG_FEATURE_ARGS[@]+"${PKG_FEATURE_ARGS[@]}"})
  TEST_WS=(--locked --workspace --lib --tests "${FEATURE_ARGS[@]}" "${nextest_common[@]}"
           -E "package($PACKAGE)")
  TEST_CRATE=(--locked -p "$PACKAGE" --lib --tests ${PKG_FEATURE_ARGS[@]+"${PKG_FEATURE_ARGS[@]}"}
              "${nextest_common[@]}")
}

machine_label() {
  if [ -n "${MACHINE_LABEL:-}" ]; then
    printf '%s' "$MACHINE_LABEL"; return
  fi
  if [ "$OS" = "Darwin" ]; then
    printf '%s %s' "$(sysctl -n hw.model 2>/dev/null)" \
      "$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"; return
  fi
  local itype
  itype="$(ec2_instance_type)"
  if [ -n "$itype" ]; then printf 'EC2 %s' "$itype"; else printf 'linux-%scpu' "$NCPU"; fi
}

ec2_instance_type() {
  have curl || return 0
  local token
  token="$(curl -s -m 1 --noproxy '*' -X PUT 'http://169.254.169.254/latest/api/token' \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' 2>/dev/null || true)"
  case "$token" in ''|*[!A-Za-z0-9_=+/-]*) return 0 ;; esac
  local itype
  itype="$(curl -s -m 1 --noproxy '*' -H "X-aws-ec2-metadata-token: $token" \
    'http://169.254.169.254/latest/meta-data/instance-type' 2>/dev/null || true)"
  case "$itype" in *[!a-z0-9.-]*|'') return 0 ;; *) printf '%s' "$itype" ;; esac
}

# ------------------------------------------------------------------------------------------------
# The edit: apply / revert without moving HEAD
# ------------------------------------------------------------------------------------------------
reset_to_base() {
  git -C "$REPO_DIR" checkout -q -- .
  # Remove only files the patch creates; never touch other untracked or ignored files.
  local f
  while IFS= read -r f; do
    [ -n "$f" ] || continue
    if [ -e "$REPO_DIR/$f" ] && ! git -C "$REPO_DIR" ls-files --error-unmatch -- "$f" >/dev/null 2>&1; then
      rm -f -- "$REPO_DIR/$f"
    fi
  done < "$CHANGE_FILES"
  if [ -n "$VARIANT_PATCH" ]; then git -C "$REPO_DIR" apply --whitespace=nowarn "$VARIANT_PATCH"; fi
}

apply_change() {
  git -C "$REPO_DIR" apply --whitespace=nowarn "$PATCH_FILE"
  # A unique trailing comment per run: same compile work, but a cache key no earlier run produced.
  local nonce f
  nonce="$(date +%s)-$$-$RANDOM"
  while IFS= read -r f; do
    case "$f" in
      *.rs) [ -f "$REPO_DIR/$f" ] && printf '\n// bench-nonce %s\n' "$nonce" >> "$REPO_DIR/$f" ;;
    esac
  done < "$CHANGE_FILES"
}

# ------------------------------------------------------------------------------------------------
# Helper subcommands invoked by hyperfine (--setup / --prepare / --cleanup / timed command)
# ------------------------------------------------------------------------------------------------
untimed_build_base() {  # $1 = scenario
  cd "$REPO_DIR"
  case "$1" in
    debug-ws)     cargo build "${BUILD_WS[@]}" ;;
    debug-crate)  cargo build "${BUILD_CRATE[@]}" ;;
    test-ws-*)    cargo nextest run "${TEST_WS[@]}" --no-run ;;
    test-crate-*) cargo nextest run "${TEST_CRATE[@]}" --no-run ;;
    *)            echo "no base build for $1" >&2; exit 2 ;;
  esac
}

sub_prep_release() {
  {
    reset_to_base
    cd "$REPO_DIR" && cargo clean --release
  } >> "$LOG" 2>&1
}

sub_prep_incremental() {  # $1 = scenario
  {
    echo "--- prepare ($1): base build, then apply edit"
    reset_to_base
    untimed_build_base "$1"
    apply_change
    if [ "${RUSTC_WRAPPER:-}" = sccache ]; then sccache --zero-stats || true; fi
  } >> "$LOG" 2>&1
}

sub_setup_cold() {  # $1 = scenario
  {
    echo "--- setup: populate sccache from the base commit"
    reset_to_base
    cd "$REPO_DIR" && cargo clean --profile nextest
    untimed_build_base "$1"
    sccache --show-stats || true
  } >> "$LOG" 2>&1
}

sub_prep_cold() {
  {
    echo "--- prepare: empty target dir (nextest profile), apply edit"
    reset_to_base
    cd "$REPO_DIR" && cargo clean --profile nextest
    apply_change
    sccache --zero-stats || true
  } >> "$LOG" 2>&1
}

sub_cleanup() {  # $1 = scenario, $2 = jobs
  {
    case "$1" in
      *-sccache|*-cold) sccache --show-stats > "$RESULTS_DIR/$1-j$2-sccache-stats.txt" 2>&1 || true ;;
    esac
    reset_to_base
    if [ "$1" = "release" ]; then
      local report
      # shellcheck disable=SC2012  # cargo controls these file names
      report="$(ls -t "$TARGET_DIR"/cargo-timings/cargo-timing*.html 2>/dev/null | head -n 1 || true)"
      [ -n "$report" ] && cp "$report" "$RESULTS_DIR/release-j$2-cargo-timing.html"
    fi
  } >> "$LOG" 2>&1
}

sub_timed() {  # $1 = scenario, $2 = jobs   (this is what hyperfine measures)
  cd "$REPO_DIR"
  echo "--- timed: $1 -j $2" >> "$LOG"
  case "$1" in
    release)      cargo build "${BUILD_WS[@]}" --release --timings -j "$2" ;;
    debug-ws)     cargo build "${BUILD_WS[@]}" -j "$2" ;;
    debug-crate)  cargo build "${BUILD_CRATE[@]}" -j "$2" ;;
    test-ws-*)    cargo nextest run "${TEST_WS[@]}" --build-jobs "$2" --test-threads "$2" ;;
    test-crate-tuned) cargo nextest run "${TEST_CRATE[@]}" --build-jobs "$2" --test-threads "$TUNED_TEST_THREADS" ;;
    test-crate-*) cargo nextest run "${TEST_CRATE[@]}" --build-jobs "$2" --test-threads "$2" ;;
    *)            echo "unknown scenario $1" >&2; exit 2 ;;
  esac >> "$LOG" 2>&1
}

# ------------------------------------------------------------------------------------------------
# Environment per scenario
# ------------------------------------------------------------------------------------------------
use_plain_rustc() {
  export RUSTC_WRAPPER="" CARGO_BUILD_RUSTC_WRAPPER=""   # empty overrides any configured wrapper
  unset CARGO_INCREMENTAL                                # let the Cargo profile decide
}

use_sccache() {
  export RUSTC_WRAPPER=sccache CARGO_BUILD_RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0
  export SCCACHE_DIR="$SCCACHE_BENCH_DIR" SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-100G}"
  mkdir -p "$SCCACHE_DIR"
  sccache --stop-server >/dev/null 2>&1 || true   # restart so the server picks up SCCACHE_DIR
  sccache --start-server >/dev/null
  sccache --zero-stats >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------------------------------------
# Running one scenario through hyperfine
# ------------------------------------------------------------------------------------------------
run_scenario() {  # $1 = scenario, $2 = runs, $3 = jobs list, rest = extra hyperfine options
  local name="$1" runs="$2" jobs="$3"
  shift 3
  export LOG="$RESULTS_DIR/logs/$name.log"
  : > "$LOG"
  log "Scenario $name  (runs=$runs, -j in {$jobs})  log: $LOG"
  if ! hyperfine \
      --runs "$runs" \
      -L jobs "$jobs" \
      --command-name "$name -j {jobs}" \
      --cleanup "\"$SELF\" _cleanup $name {jobs}" \
      --export-json "$RESULTS_DIR/$name.json" \
      --export-markdown "$RESULTS_DIR/$name.md" \
      "$@" \
      "\"$SELF\" _timed $name {jobs}"; then
    echo "---- last 40 lines of $LOG ----" >&2
    tail -n 40 "$LOG" >&2 || true
    die "scenario $name failed"
  fi
  df -h "$REPO_DIR" | tail -n 1 > "$RESULTS_DIR/logs/$name.disk"
}

sanity_test_pass() {
  # One untimed pass on the edited tree, so a red test suite stops us before hours of timing.
  export LOG="$RESULTS_DIR/logs/sanity-tests.log"
  log "Sanity pass: building and running $PACKAGE's tests once on the edited tree (untimed)"
  use_plain_rustc
  if ! ( reset_to_base && apply_change && cd "$REPO_DIR" &&
         cargo nextest run "${TEST_CRATE[@]}" ) >> "$LOG" 2>&1; then
    reset_to_base
    tail -n 40 "$LOG" >&2 || true
    die "tests fail on the edited tree (see $LOG). Choose another PATCH_FILE / CHANGE_COMMIT."
  fi
  reset_to_base
}

# ------------------------------------------------------------------------------------------------
# Setup, checks and environment record
# ------------------------------------------------------------------------------------------------
scenario_selected() { case ",$SCENARIOS," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
any_selected() { local s; for s in "$@"; do scenario_selected "$s" && return 0; done; return 1; }

preflight() {
  local t s
  for s in $(printf '%s' "$SCENARIOS" | tr ',' ' '); do
    case ",$ALL_SCENARIOS," in *",$s,"*) ;; *) die "unknown scenario '$s'" ;; esac
  done
  for t in git cargo hyperfine jq; do have "$t" || die "$t is not installed"; done
  if any_selected test-ws-incremental test-ws-sccache test-crate-incremental test-crate-tuned test-crate-sccache test-crate-cold; then
    cargo nextest --version >/dev/null 2>&1 || die "cargo-nextest is not installed (cargo install cargo-nextest --locked)"
  fi
  if any_selected test-ws-sccache test-crate-sccache test-crate-cold; then
    have sccache || die "sccache is not installed (brew install sccache / cargo install sccache --locked)"
  fi
  if [ "$OS" = "Linux" ] && ! have ld.lld; then
    die "ld.lld not found: the repo's .cargo/config.toml links with lld on Linux (apt-get install lld clang)"
  fi
  case ",$FEATURES," in *",capnp,"*|",all,") have capnp || die "capnp not found (./scripts/install-capnp.sh)" ;; esac

  for t in RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_BUILD_RUSTFLAGS; do
    if [ -n "${!t:-}" ]; then
      warn "unsetting $t: it would override the repo's linker flags and change the measured build"
      unset "$t"
    fi
  done
  unset CARGO_BUILD_JOBS NEXTEST_TEST_THREADS CARGO_TARGET_DIR_OVERRIDE

  # PyO3 settings from the NautilusTrader environment-setup guide. Keep them constant across runs:
  # a changed PYO3_PYTHON makes pyo3's build script rerun and rebuilds a large part of the workspace.
  if [ -z "${PYO3_PYTHON:-}" ] && [ -x "$REPO_DIR/python/.venv/bin/python" ]; then
    export PYO3_PYTHON="$REPO_DIR/python/.venv/bin/python"
  fi
  if [ -z "${PYO3_PYTHON:-}" ] && have python3; then
    export PYO3_PYTHON="$(command -v python3)"
    warn "PYO3_PYTHON not set and python/.venv missing; using $PYO3_PYTHON"
  fi
  if [ -n "${PYO3_PYTHON:-}" ]; then
    export PYTHONHOME="${PYTHONHOME:-$("$PYO3_PYTHON" -c 'import sys; print(sys.base_prefix)')}"
    local libdir
    libdir="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')"
    if [ "$OS" = "Linux" ]; then
      export LD_LIBRARY_PATH="$libdir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    else
      export DYLD_LIBRARY_PATH="$libdir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
    fi
  fi
}

prepare_repo() {
  if [ ! -d "$REPO_DIR/.git" ] && [ ! -f "$REPO_DIR/.git" ]; then
    log "Cloning NautilusTrader into $REPO_DIR"
    git clone https://github.com/nautechsystems/nautilus_trader "$REPO_DIR"
  fi
  if [ -n "$VARIANT_PATCH" ]; then
    [ -f "$VARIANT_PATCH" ] || die "VARIANT_PATCH $VARIANT_PATCH not found"
    cp "$VARIANT_PATCH" "$RESULTS_DIR/variant.patch"
    VARIANT_PATCH="$RESULTS_DIR/variant.patch"
    export VARIANT_PATCH
  fi
  if [ -n "$(git -C "$REPO_DIR" status --porcelain --untracked-files=normal)" ]; then
    die "$REPO_DIR has local changes; use a clean checkout dedicated to benchmarking"
  fi

  if [ -n "${CHANGE_COMMIT:-}" ]; then
    git -C "$REPO_DIR" cat-file -e "$CHANGE_COMMIT^{commit}" 2>/dev/null ||
      git -C "$REPO_DIR" fetch -q origin || die "unknown CHANGE_COMMIT $CHANGE_COMMIT"
    CHANGE_COMMIT="$(git -C "$REPO_DIR" rev-parse "$CHANGE_COMMIT")"
    BASE_REF="${BASE_REF:-$CHANGE_COMMIT^}"
    PATCH_FILE="$RESULTS_DIR/change.patch"
    git -C "$REPO_DIR" diff --binary "$BASE_REF" "$CHANGE_COMMIT" > "$PATCH_FILE"
  else
    PATCH_FILE="${PATCH_FILE:-$HERE/hyperliquid-edit.patch}"
    [ -f "$PATCH_FILE" ] || die "PATCH_FILE $PATCH_FILE not found"
    cp "$PATCH_FILE" "$RESULTS_DIR/change.patch"
    PATCH_FILE="$RESULTS_DIR/change.patch"
    if [ -z "${BASE_REF:-}" ]; then
      [ -f "$HERE/BASE_COMMIT" ] || die "set BASE_REF (no $HERE/BASE_COMMIT file)"
      BASE_REF="$(tr -d '[:space:]' < "$HERE/BASE_COMMIT")"
    fi
  fi
  if ! git -C "$REPO_DIR" cat-file -e "$BASE_REF^{commit}" 2>/dev/null; then
    log "Fetching $BASE_REF"
    git -C "$REPO_DIR" fetch -q origin "$BASE_REF" 2>/dev/null || git -C "$REPO_DIR" fetch -q origin ||
      die "base commit $BASE_REF is not available in $REPO_DIR"
  fi
  BASE_SHA="$(git -C "$REPO_DIR" rev-parse "$BASE_REF^{commit}")"

  log "Checking out base commit $BASE_SHA (HEAD stays here for every timed run)"
  git -C "$REPO_DIR" checkout -q --detach "$BASE_SHA"

  CHANGE_FILES="$RESULTS_DIR/change.files"
  git -C "$REPO_DIR" apply --numstat "$PATCH_FILE" | cut -f3 | grep -v ' => ' > "$CHANGE_FILES" || true
  if [ -n "$VARIANT_PATCH" ]; then
    git -C "$REPO_DIR" apply "$VARIANT_PATCH" || die "VARIANT_PATCH does not apply on $BASE_SHA"
  fi
  git -C "$REPO_DIR" apply --check "$PATCH_FILE" || die "the edit does not apply cleanly on $BASE_SHA (+ variant)"
  export PATCH_FILE CHANGE_FILES BASE_SHA

  if [ -z "${PKG_FEATURES+x}" ]; then
    PKG_FEATURES="$(cd "$REPO_DIR" && ./scripts/crate-test-features.sh "$PACKAGE")"
  fi
  export PKG_FEATURES

  log "Installing the pinned toolchain and fetching crates (untimed)"
  ( cd "$REPO_DIR" && cargo --version && cargo fetch --locked )
  export CARGO_NET_OFFLINE=true   # timed runs never touch the network
}

record_environment() {
  local f="$RESULTS_DIR/env.txt" rs_files rs_lines
  rs_files="$(grep -c '\.rs$' "$CHANGE_FILES" || true)"
  rs_lines="$(git -C "$REPO_DIR" apply --numstat "$PATCH_FILE" | awk -F'\t' '$3 ~ /\.rs$/ {s += $1 + $2} END {print s + 0}')"
  {
    echo "label=$(machine_label)"
    echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname)"
    echo "uname=$(uname -a)"
    echo "logical_cpus=$NCPU"
    if [ "$OS" = "Darwin" ]; then
      echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
      echo "perf_cores=$(sysctl -n hw.perflevel0.physicalcpu 2>/dev/null || echo '?')"
      echo "efficiency_cores=$(sysctl -n hw.perflevel1.physicalcpu 2>/dev/null || echo 0)"
      echo "memory_bytes=$(sysctl -n hw.memsize)"
      echo "macos=$(sw_vers -productVersion 2>/dev/null)"
    else
      echo "cpu=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')"
      echo "lscpu_threads_per_core=$(lscpu 2>/dev/null | awk -F: '/Thread\(s\) per core/ {gsub(/ /,"",$2); print $2}')"
      echo "memory_kb=$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
      echo "ec2_instance_type=$(ec2_instance_type)"
      echo "ld_lld=$(ld.lld --version 2>/dev/null | head -n 1)"
    fi
    echo "target_dir=$TARGET_DIR"
    echo "target_fs=$(df -h "$REPO_DIR" | tail -n 1)"
    echo "rustc=$(cd "$REPO_DIR" && rustc -vV | tr '\n' ';')"
    echo "cargo=$(cd "$REPO_DIR" && cargo --version)"
    echo "hyperfine=$(hyperfine --version)"
    echo "nextest=$(cargo nextest --version 2>/dev/null | head -n 1 || echo n/a)"
    echo "sccache=$(sccache --version 2>/dev/null || echo n/a)"
    echo "repo_base=$BASE_SHA"
    echo "change_commit=${CHANGE_COMMIT:-patch $(basename "${PATCH_FILE}")}"
    echo "change_rs_files=$rs_files"
    echo "change_rs_lines=$rs_lines"
    echo "package=$PACKAGE"
    echo "pkg_features=$PKG_FEATURES"
    echo "features=$FEATURES"
    echo "variant_patch=${VARIANT_PATCH:+$(basename "$VARIANT_PATCH") $( (sha256sum "$VARIANT_PATCH" 2>/dev/null || shasum -a 256 "$VARIANT_PATCH") | cut -c1-12)}"
    echo "tuned_test_threads=$TUNED_TEST_THREADS"
    echo "pyo3_python=${PYO3_PYTHON:-}"
    echo "cmd_release=cargo build ${BUILD_WS[*]} --release --timings -j {jobs}"
    echo "cmd_debug_ws=cargo build ${BUILD_WS[*]} -j {jobs}"
    echo "cmd_debug_crate=cargo build ${BUILD_CRATE[*]} -j {jobs}"
    echo "cmd_test_ws=cargo nextest run ${TEST_WS[*]} --build-jobs {jobs} --test-threads {jobs}"
    echo "cmd_test_crate=cargo nextest run ${TEST_CRATE[*]} --build-jobs {jobs} --test-threads {jobs}"
  } > "$f"
  log "Environment recorded in $f"
  cat "$f" >&2
}

# ------------------------------------------------------------------------------------------------
# find-commits: real upstream commits that touch 3-10 .rs files and a few hundred lines,
# without dependency or build-config changes (those would not apply as a plain source edit).
# ------------------------------------------------------------------------------------------------
find_commits() {
  local ref="${1:-origin/develop}"
  [ -d "$REPO_DIR/.git" ] || git clone https://github.com/nautechsystems/nautilus_trader "$REPO_DIR"
  git -C "$REPO_DIR" fetch -q origin 2>/dev/null || true
  git -C "$REPO_DIR" -c gc.auto=0 -c maintenance.auto=false log --no-merges -n "${SCAN:-400}" --date=short \
    --format='@%h|%ad|%s' --numstat "$ref" |
  awk -F'\t' -v minf="${MIN_FILES:-3}" -v maxf="${MAX_FILES:-10}" \
             -v minl="${MIN_LINES:-100}" -v maxl="${MAX_LINES:-800}" '
    function flush() {
      if (sha != "" && !bad && nrs >= minf && nrs <= maxf && lines >= minl && lines <= maxl)
        printf "%s  %s  %2d .rs files  %4d lines  crates: %s\n           %s\n", sha, date, nrs, lines, crates, subj
    }
    /^@/ {
      flush()
      n = split(substr($0, 2), a, "|")
      sha = a[1]; date = a[2]; subj = substr($0, length(a[1]) + length(a[2]) + 4)
      nrs = 0; lines = 0; bad = 0; crates = ""; split("", seen)
      next
    }
    NF == 3 {
      path = $3
      if (path ~ /(^|\/)(Cargo\.toml|Cargo\.lock|build\.rs|rust-toolchain\.toml)$/ || path ~ /^\.cargo\//) bad = 1
      if ($1 == "-" || path !~ /\.rs$/) next
      nrs++; lines += $1 + $2
      split(path, p, "/")
      c = (p[1] == "crates") ? ((p[2] == "adapters") ? p[3] : p[2]) : "(other)"
      if (!(c in seen)) { seen[c] = 1; crates = crates (crates == "" ? "" : ",") c }
    }
    END { flush() }'
}

# ------------------------------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------------------------------
main() {
  load_config

  local label
  label="$(machine_label | tr -cs 'A-Za-z0-9._-' '-' | sed 's/-*$//')"
  RESULTS_DIR="${RESULTS_DIR:-$PWD/results/${label}-$(date +%Y%m%d-%H%M%S)}"
  mkdir -p "$RESULTS_DIR/logs"
  RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
  case "$RESULTS_DIR/" in
    "$REPO_DIR"/*) die "RESULTS_DIR must be outside REPO_DIR" ;;
  esac
  export RESULTS_DIR

  if [ "$OS" = "Darwin" ] && have caffeinate; then
    caffeinate -dimsu -w $$ &   # keep the Mac awake for the whole run
  fi

  preflight
  prepare_repo
  cargo_arg_arrays
  record_environment
  # On exit leave the checkout exactly at BASE_SHA (without the variant), so the next run starts clean.
  trap 'VARIANT_PATCH="" reset_to_base >/dev/null 2>&1 || true' EXIT

  # Fast, per-crate scenarios first (the inner development loop), then workspace-wide ones,
  # then the clean release build. sccache-cold runs last among the tests: it empties target/nextest.
  if any_selected test-ws-incremental test-ws-sccache test-crate-incremental test-crate-tuned test-crate-sccache test-crate-cold; then
    sanity_test_pass
  fi

  local s
  for s in test-crate-incremental test-crate-tuned debug-crate test-crate-sccache test-ws-incremental debug-ws test-ws-sccache; do
    scenario_selected "$s" || continue
    case "$s" in *-sccache) use_sccache ;; *) use_plain_rustc ;; esac
    case "$s" in
      debug-*) run_scenario "$s" "$RUNS_DEBUG" "$JOBS_LIST" --prepare "\"$SELF\" _prep_incremental $s" ;;
      test-*)  run_scenario "$s" "$RUNS_TEST" "$TEST_JOBS_LIST" --prepare "\"$SELF\" _prep_incremental $s" ;;
    esac
  done

  if scenario_selected test-crate-cold; then
    use_sccache
    run_scenario test-crate-cold "$RUNS_TEST" "$TEST_JOBS_LIST" \
      --setup "\"$SELF\" _setup_cold test-crate-cold" \
      --prepare "\"$SELF\" _prep_cold"
  fi

  if scenario_selected release; then
    use_plain_rustc
    if [ "$FREE_DISK" = "1" ]; then
      log "Freeing disk before the release build (target/debug, target/nextest)"
      ( cd "$REPO_DIR" && cargo clean --profile dev && cargo clean --profile nextest ) >/dev/null 2>&1 || true
    fi
    run_scenario release "$RUNS_RELEASE" "$JOBS_LIST" \
      --prepare "\"$SELF\" _prep_release"
  fi

  log "Done. Results in $RESULTS_DIR"
  cat "$RESULTS_DIR"/*.md 2>/dev/null || true
  echo
  echo "Compare machines with: python3 $HERE/compare_results.py <results-dir-1> <results-dir-2> ..."
}

# Helper subcommands run inside hyperfine; they inherit the exported configuration.
case "${1:-run}" in
  run)          main ;;
  find-commits) load_config; shift; find_commits ${1+"$@"} ;;   # ${1+"$@"}: safe under set -u in bash 3.2
  _prep_release|_prep_incremental|_setup_cold|_prep_cold|_cleanup|_timed)
    cmd="$1"; shift
    cargo_arg_arrays
    case "$cmd" in
      _prep_release)     sub_prep_release ;;
      _prep_incremental) sub_prep_incremental "$@" ;;
      _setup_cold)       sub_setup_cold "$@" ;;
      _prep_cold)        sub_prep_cold ;;
      _cleanup)          sub_cleanup "$@" ;;
      _timed)            sub_timed "$@" ;;
    esac
    ;;
  -h|--help|help) sed -n '2,57p' "$SELF" | sed 's/^# \{0,1\}//' ;;
  *) die "unknown command '$1' (try: run, find-commits, help)" ;;
esac
