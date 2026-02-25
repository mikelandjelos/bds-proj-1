#!/usr/bin/env python3
"""Aggregate benchmark run summaries into timestamped report artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", default="runs", help="Root runs directory")
    parser.add_argument(
        "--output-root",
        default="runs/reports",
        help="Root output directory for generated aggregation reports",
    )
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="Include only the latest run per mode in report files",
    )
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def safe_report_dir(output_root: Path) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    while True:
        candidate = output_root / utc_stamp()
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate


def fmt_seconds(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def scan_runs(runs_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mode in ("standalone", "cluster"):
        mode_dir = runs_dir / mode
        if not mode_dir.exists():
            continue

        for run_dir in sorted([p for p in mode_dir.iterdir() if p.is_dir()]):
            summary_path = run_dir / "summary.json"
            if not summary_path.exists():
                continue

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            meta_path = run_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

            rows.append(
                {
                    "mode": mode,
                    "run_timestamp": run_dir.name,
                    "run_dir": str(run_dir),
                    "benchmark_program": meta.get("benchmark_program"),
                    "master": meta.get("master"),
                    "repeats": meta.get("repeats"),
                    "iterations_total": summary.get("iterations_total"),
                    "iterations_ok": summary.get("iterations_ok"),
                    "iterations_failed": summary.get("iterations_failed"),
                    "duration_avg_seconds_ok": summary.get("duration_avg_seconds_ok"),
                    "duration_min_seconds_ok": summary.get("duration_min_seconds_ok"),
                    "duration_max_seconds_ok": summary.get("duration_max_seconds_ok"),
                }
            )

    rows.sort(key=lambda r: (r["mode"], r["run_timestamp"]))
    return rows


def latest_by_mode(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chosen: dict[str, dict[str, Any]] = {}
    for row in rows:
        mode = row["mode"]
        prev = chosen.get(mode)
        if prev is None or row["run_timestamp"] > prev["run_timestamp"]:
            chosen[mode] = row
    return [chosen[m] for m in sorted(chosen.keys())]


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=False), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "mode",
        "run_timestamp",
        "benchmark_program",
        "master",
        "repeats",
        "iterations_total",
        "iterations_ok",
        "iterations_failed",
        "duration_avg_seconds_ok",
        "duration_min_seconds_ok",
        "duration_max_seconds_ok",
        "run_dir",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def rows_to_markdown(rows: list[dict[str, Any]], title: str) -> str:
    lines = [
        f"## {title}",
        "",
        "| mode | run_timestamp | ok/total | avg_s | min_s | max_s | run_dir |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        ok_total = f"{row['iterations_ok']}/{row['iterations_total']}"
        lines.append(
            f"| {row['mode']} | {row['run_timestamp']} | {ok_total} | {fmt_seconds(row['duration_avg_seconds_ok'])} | {fmt_seconds(row['duration_min_seconds_ok'])} | {fmt_seconds(row['duration_max_seconds_ok'])} | `{row['run_dir']}` |"
        )
    if len(rows) == 0:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    runs_dir = Path(args.runs_dir)
    all_rows = scan_runs(runs_dir)
    selected_rows = latest_by_mode(all_rows) if args.latest_only else all_rows
    latest_rows = latest_by_mode(all_rows)

    report_dir = safe_report_dir(Path(args.output_root))
    write_json(
        report_dir / "aggregate.json",
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "runs_dir": str(runs_dir),
            "rows_total": len(all_rows),
            "rows_included": len(selected_rows),
            "latest_by_mode": latest_rows,
            "rows": selected_rows,
        },
    )
    write_csv(report_dir / "aggregate.csv", selected_rows)

    md = [
        "# Benchmark aggregation",
        "",
        f"Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        f"Source runs dir: `{runs_dir}`",
        f"Report dir: `{report_dir}`",
        "",
        rows_to_markdown(latest_rows, "Latest by mode"),
        "",
        rows_to_markdown(selected_rows, "Included rows"),
        "",
    ]
    (report_dir / "aggregate.md").write_text("\n".join(md), encoding="utf-8")

    print(f"Report directory: {report_dir}")
    print()
    print(rows_to_markdown(latest_rows, "Latest by mode"))


if __name__ == "__main__":
    main()
