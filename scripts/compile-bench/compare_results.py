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
import pathlib
import sys

SCENARIOS = [
    ("test-crate-incremental", "Edit -> build + run one crate's tests, incremental (recommended dev loop)"),
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


def load_machine(directory: pathlib.Path) -> dict:
    env = read_env(directory)
    label = env.get("label") or directory.name
    scenarios: dict[str, list[dict]] = {}
    for name, _ in SCENARIOS:
        path = directory / f"{name}.json"
        if not path.exists():
            continue
        rows = []
        for result in json.loads(path.read_text())["results"]:
            jobs = int((result.get("parameters") or {}).get("jobs", "0") or 0)
            rows.append(
                {
                    "jobs": jobs,
                    "mean": result["mean"],
                    "stddev": result.get("stddev") or 0.0,
                    "min": result["min"],
                    "cpu": (result.get("user") or 0.0) + (result.get("system") or 0.0),
                    "runs": len(result.get("times", [])),
                }
            )
        rows.sort(key=lambda r: r["jobs"])
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
        out.append(f"| Machine | -j | Mean | ± σ | Min | CPU time | Cores busy | Speed-up vs -j 1 | vs {ref['label']} |")
        out.append("|---|---|---|---|---|---|---|---|---|")
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
                speedup = f"{single['mean'] / r['mean']:.2f}x" if single and r is not single else "-"
                other = ref_rows.get(r["key"])
                if m is ref or other is None:
                    versus = "-"
                else:
                    ratio = other["mean"] / r["mean"]
                    versus = f"{ratio:.2f}x faster" if ratio >= 1 else f"{1 / ratio:.2f}x slower"
                out.append(
                    f"| {m['label']} | {r['jobs']} | {fmt_time(r['mean'])} | {fmt_time(r['stddev'])} "
                    f"| {fmt_time(r['min'])} | {cpu} | {busy} | {speedup} | {versus} |"
                )

    out.append("\n*Cores busy* = (user + system CPU time) / wall time. A value far below the -j count "
               "means the build was serialized somewhere (dependency chains, the fat-LTO step, linking). "
               "CPU time is not reported for sccache scenarios because the sccache server does the "
               "compiling outside cargo's process tree.")
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
