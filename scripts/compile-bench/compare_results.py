#!/usr/bin/env python3
"""Compare bench_nautilus.sh results across machines (e.g. a MacBook, EC2 and a cloud dev box).

Usage:
    python3 compare_results.py RESULTS_DIR [RESULTS_DIR ...] [--out compare.md]

The first directory is the reference machine. For each scenario the table shows, per machine
and -j value: wall time, CPU time, how many cores were busy on average (CPU / wall), the
multi-core speed-up over -j 1 on that machine, and the ratio against the reference machine.
"""

from __future__ import annotations

import argparse
import json
import re
import pathlib
import sys

SCENARIOS = [
    ("test-crate-incremental", "Edit -> build + run one crate's tests, incremental (recommended dev loop)"),
    ("test-crate-tuned", "test-crate-incremental with nextest --test-threads = 4 x cores (I/O-bound integration tests)"),
    ("test-crate-sccache", "Edit -> build + run one crate's tests, sccache + CARGO_INCREMENTAL=0 (make cargo-test-crate-<pkg> style)"),
    ("test-crate-cold", "Edit -> empty target dir, warm sccache (fresh worktree / CI), one crate's tests"),
    ("test-ws-incremental", "Edit -> build every workspace test binary, incremental, run the crate's tests"),
    ("test-ws-sccache", "Edit -> build every workspace test binary, sccache + CARGO_INCREMENTAL=0 (make cargo-test style)"),
    ("debug-crate", "Edit -> incremental dev build of one crate (cargo build -p)"),
    ("debug-ws", "Edit -> incremental dev build of the whole workspace"),
    ("release", "Clean release build of the workspace (fat LTO, codegen-units=1)"),
]

# The sccache server does the compiling in its own process, which is not a child of cargo,
# so hyperfine's user/system time misses most of the work in those scenarios.
CPU_TIME_UNRELIABLE = {"test-crate-sccache", "test-crate-cold", "test-ws-sccache"}


def read_env(directory: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    path = directory / "env.txt"
    if path.exists():
        for line in path.read_text().splitlines():
            key, sep, value = line.partition("=")
            if sep:
                env[key.strip()] = value.strip()
    return env


def parse_result(result: dict) -> dict:
    """Read one hyperfine result; supports the 1.x schema and the 2.x `summary` schema."""
    params = result.get("parameters") or {}
    jobs = params.get("jobs", "0")
    if isinstance(jobs, dict):
        jobs = jobs.get("value", "0")
    summary = result.get("summary")
    if summary:
        wall = summary["time_wall_clock"]
        cpu = summary.get("time_cpu", {}).get("mean")
        if cpu is None:
            cpu = summary.get("time_user", {}).get("mean", 0.0) + summary.get("time_system", {}).get("mean", 0.0)
        rss = summary.get("memory_peak_resident", {}).get("max")
        return {"jobs": int(jobs or 0), "mean": wall["mean"], "stddev": wall.get("stddev") or 0.0,
                "min": wall["min"], "cpu": cpu, "rss": rss, "runs": wall.get("count", 0)}
    return {"jobs": int(jobs or 0), "mean": result["mean"], "stddev": result.get("stddev") or 0.0,
            "min": result["min"], "cpu": (result.get("user") or 0.0) + (result.get("system") or 0.0),
            "rss": max(result["memory_usage_byte"]) if result.get("memory_usage_byte") else None,
            "runs": len(result.get("times", []))}


CARGO_FINISHED = re.compile(r"Finished `[^`]+` profile .* in (?:(\d+)m )?([\d.]+)s")
NEXTEST_SUMMARY = re.compile(r"Summary \[\s*([\d.]+)s\]")


def split_build_test(log: pathlib.Path) -> tuple[float | None, float | None]:
    """Mean cargo build time and nextest run time of the timed runs, read from the scenario log."""
    if not log.exists():
        return None, None
    builds, tests, timed = [], [], False
    for line in log.read_text(errors="replace").splitlines():
        if line.startswith("--- "):
            timed = line.startswith("--- timed")
            continue
        if not timed:
            continue
        if m := CARGO_FINISHED.search(line):
            builds.append(int(m.group(1) or 0) * 60 + float(m.group(2)))
        elif m := NEXTEST_SUMMARY.search(line):
            tests.append(float(m.group(1)))
    mean = lambda xs: sum(xs) / len(xs) if xs else None  # noqa: E731
    return mean(builds), mean(tests)


def load_machine(directory: pathlib.Path) -> dict:
    env = read_env(directory)
    label = env.get("label") or directory.name
    scenarios: dict[str, list[dict]] = {}
    for name, _ in SCENARIOS:
        path = directory / f"{name}.json"
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:  # scenario still running or aborted
            print(f"skipping unreadable {path}", file=sys.stderr)
            continue
        rows = [parse_result(result) for result in data["results"]]
        rows.sort(key=lambda r: r["jobs"])
        # The log interleaves -j values in hyperfine's order, so the split is only exact for one -j.
        if len(rows) == 1:
            rows[0]["build"], rows[0]["test"] = split_build_test(directory / "logs" / f"{name}.log")
        max_jobs = max(r["jobs"] for r in rows)
        for r in rows:
            r["key"] = "1" if r["jobs"] == 1 else ("all" if r["jobs"] == max_jobs else str(r["jobs"]))
        scenarios[name] = rows
    return {"label": label, "env": env, "scenarios": scenarios, "dir": directory}


def fmt_time(seconds: float) -> str:
    if seconds >= 90:
        minutes, secs = divmod(seconds, 60)
        return f"{int(minutes)}m {secs:04.1f}s"
    return f"{seconds:.1f}s"


def build_report(machines: list[dict]) -> str:
    out: list[str] = []
    out.append("# NautilusTrader compile benchmark comparison\n")
    out.append("## Machines\n")
    out.append("| Machine | Logical CPUs | CPU | rustc | Base commit | Package | Edit |")
    out.append("|---|---|---|---|---|---|---|")
    for m in machines:
        e = m["env"]
        rustc = e.get("rustc", "").split(";")[0]
        edit = f"{e.get('change_rs_files', '?')} .rs files, {e.get('change_rs_lines', '?')} lines"
        out.append(
            f"| {m['label']} | {e.get('logical_cpus', '?')} | {e.get('cpu', '?')} | {rustc} "
            f"| {e.get('repo_base', '?')[:10]} | {e.get('package', '?')} | {edit} |"
        )
    bases = {m["env"].get("repo_base") for m in machines}
    if len(bases) > 1:
        out.append("\n> **Warning:** the machines benchmarked different base commits, so the numbers "
                   "are not directly comparable.")

    ref = machines[0]
    for name, title in SCENARIOS:
        if not any(name in m["scenarios"] for m in machines):
            continue
        out.append(f"\n## {name}: {title}\n")
        out.append(f"| Machine | -j | Runs | Mean | ± σ | Min | CPU time | Cores busy | Peak RSS | Build / tests | Speed-up vs -j 1 | vs {ref['label']} |")
        out.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
        ref_rows = {r["key"]: r for r in ref["scenarios"].get(name, [])}
        for m in machines:
            rows = m["scenarios"].get(name, [])
            single = next((r for r in rows if r["jobs"] == 1), None)
            for r in rows:
                if name in CPU_TIME_UNRELIABLE:
                    cpu, busy = "n/a", "n/a"
                else:
                    cpu = fmt_time(r["cpu"])
                    busy = f"{r['cpu'] / r['mean']:.1f}" if r["mean"] > 0 else "-"
                if r.get("build") is not None:
                    split = fmt_time(r["build"]) + (f" / {fmt_time(r['test'])}" if r.get("test") else "")
                else:
                    split = "-"
                rss = f"{r['rss'] / 2**30:.1f} GiB" if r.get("rss") else "-"
                speedup = f"{single['mean'] / r['mean']:.2f}x" if single and r is not single else "-"
                other = ref_rows.get(r["key"])
                if m is ref or other is None:
                    versus = "-"
                else:
                    ratio = other["mean"] / r["mean"]
                    versus = f"{ratio:.2f}x faster" if ratio >= 1 else f"{1 / ratio:.2f}x slower"
                out.append(
                    f"| {m['label']} | {r['jobs']} | {r['runs']} | {fmt_time(r['mean'])} | {fmt_time(r['stddev'])} "
                    f"| {fmt_time(r['min'])} | {cpu} | {busy} | {rss} | {split} | {speedup} | {versus} |"
                )

    out.append("\n*Cores busy* = (user + system CPU time) / wall time. A value far below the -j count "
               "means the build was serialized somewhere (dependency chains, the fat-LTO step, linking). "
               "CPU time is not reported for sccache scenarios because the sccache server does the "
               "compiling outside cargo's process tree. *Peak RSS* is the largest resident set of a single "
               "process in the timed command's tree (hyperfine 2.x), not total machine memory. *Build / tests* "
               "splits the mean into cargo's reported build time and nextest's test-run time (from the logs).")
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+", type=pathlib.Path, help="results directories (first = reference)")
    parser.add_argument("--out", type=pathlib.Path, help="also write the report to this Markdown file")
    args = parser.parse_args()

    machines = []
    for directory in args.results:
        if not directory.is_dir():
            print(f"not a directory: {directory}", file=sys.stderr)
            return 1
        machines.append(load_machine(directory))

    report = build_report(machines)
    print(report)
    if args.out:
        args.out.write_text(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
