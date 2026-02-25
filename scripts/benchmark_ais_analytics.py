#!/usr/bin/env python3
"""Run one benchmark program for ais_analytics.py and persist timestamped run artifacts."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of repetitions per mode",
    )
    parser.add_argument(
        "--modes",
        choices=["both", "cluster", "standalone"],
        default="both",
        help="Execution modes to benchmark",
    )
    parser.add_argument(
        "--base-dir",
        default="runs",
        help="Root directory for benchmark run artifacts",
    )
    parser.add_argument(
        "--cluster-master",
        default="spark://spark-master:7077",
        help="Spark master URL for cluster mode",
    )
    parser.add_argument(
        "--standalone-master",
        default="local[*]",
        help="Spark master URL for standalone mode",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2018,
        help="Year filter for the benchmark query",
    )
    parser.add_argument(
        "--group-by",
        default="month",
        help="Group-by columns for the benchmark query",
    )
    parser.add_argument(
        "--metrics",
        default="speed",
        help="Metrics columns for the benchmark query",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=12,
        help="Limit for stats output",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=0,
        help="Per-run timeout in seconds (0 = no timeout)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands and output paths without executing",
    )
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def safe_run_dir(base: Path, mode: str) -> Path:
    mode_dir = base / mode
    mode_dir.mkdir(parents=True, exist_ok=True)

    while True:
        candidate = mode_dir / utc_stamp()
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        time.sleep(0.001)


def spark_submit_cmd(master: str, args: argparse.Namespace) -> list[str]:
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "app",
        "/spark/bin/spark-submit",
        "--master",
        master,
        "/workspace/scripts/ais_analytics.py",
        "stats",
        "--year",
        str(args.year),
        "--group-by",
        args.group_by,
        "--metrics",
        args.metrics,
        "--order-by",
        "month",
        "--limit",
        str(args.limit),
    ]


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def run_single(
    cmd: list[str],
    run_dir: Path,
    iteration: int,
    timeout_sec: int,
    dry_run: bool,
) -> dict[str, Any]:
    start_ts = datetime.now(timezone.utc).isoformat()
    iter_tag = f"iter_{iteration:02d}"
    stdout_path = run_dir / f"{iter_tag}.stdout.log"
    stderr_path = run_dir / f"{iter_tag}.stderr.log"

    if dry_run:
        result = {
            "iteration": iteration,
            "command": cmd,
            "started_at_utc": start_ts,
            "finished_at_utc": start_ts,
            "duration_seconds": 0.0,
            "return_code": 0,
            "timed_out": False,
            "stdout_file": stdout_path.name,
            "stderr_file": stderr_path.name,
            "dry_run": True,
        }
        write_text(stdout_path, "[dry-run] command not executed\n")
        write_text(stderr_path, "")
        return result

    t0 = time.perf_counter()
    timed_out = False
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            check=False,
            timeout=None if timeout_sec == 0 else timeout_sec,
        )
        rc = proc.returncode
        stdout = proc.stdout
        stderr = proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        rc = 124
        stdout = exc.stdout or ""
        stderr = (exc.stderr or "") + f"\n[benchmark] timeout after {timeout_sec} seconds"

    duration = time.perf_counter() - t0
    end_ts = datetime.now(timezone.utc).isoformat()

    write_text(stdout_path, stdout)
    write_text(stderr_path, stderr)

    return {
        "iteration": iteration,
        "command": cmd,
        "started_at_utc": start_ts,
        "finished_at_utc": end_ts,
        "duration_seconds": duration,
        "return_code": rc,
        "timed_out": timed_out,
        "stdout_file": stdout_path.name,
        "stderr_file": stderr_path.name,
        "dry_run": False,
    }


def summarize(mode: str, run_dir: Path, results: list[dict[str, Any]]) -> None:
    ok = [r for r in results if r["return_code"] == 0]
    durations = [float(r["duration_seconds"]) for r in ok]

    summary: dict[str, Any] = {
        "mode": mode,
        "run_dir": str(run_dir),
        "iterations_total": len(results),
        "iterations_ok": len(ok),
        "iterations_failed": len(results) - len(ok),
        "durations_seconds_ok": durations,
        "duration_min_seconds_ok": min(durations) if durations else None,
        "duration_max_seconds_ok": max(durations) if durations else None,
        "duration_avg_seconds_ok": (sum(durations) / len(durations)) if durations else None,
    }
    write_json(run_dir / "summary.json", summary)


def run_mode(mode: str, master: str, args: argparse.Namespace, base_dir: Path) -> Path:
    run_dir = safe_run_dir(base_dir, mode)
    cmd = spark_submit_cmd(master, args)

    meta = {
        "mode": mode,
        "master": master,
        "benchmark_program": "ais_analytics.py stats",
        "query": {
            "year": args.year,
            "group_by": args.group_by,
            "metrics": args.metrics,
            "order_by": "month",
            "limit": args.limit,
        },
        "repeats": args.repeats,
        "timeout_sec": args.timeout_sec,
        "dry_run": args.dry_run,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_json(run_dir / "meta.json", meta)

    results: list[dict[str, Any]] = []
    for i in range(1, args.repeats + 1):
        result = run_single(cmd, run_dir, i, args.timeout_sec, args.dry_run)
        results.append(result)
        write_json(run_dir / "results.json", {"results": results})

    summarize(mode, run_dir, results)
    return run_dir


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)

    modes: list[tuple[str, str]]
    if args.modes == "both":
        modes = [("standalone", args.standalone_master), ("cluster", args.cluster_master)]
    elif args.modes == "standalone":
        modes = [("standalone", args.standalone_master)]
    else:
        modes = [("cluster", args.cluster_master)]

    output: dict[str, str] = {}
    for mode, master in modes:
        run_dir = run_mode(mode, master, args, base_dir)
        output[mode] = str(run_dir)

    print(json.dumps({"run_directories": output}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
