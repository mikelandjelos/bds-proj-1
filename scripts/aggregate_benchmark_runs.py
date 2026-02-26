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
                    "benchmark_name": meta.get("benchmark_name"),
                    "benchmark_program": meta.get("benchmark_program"),
                    "benchmark_command_shell": meta.get("benchmark_command_shell"),
                    "master": meta.get("master"),
                    "raw_input_path": ((meta.get("data_sizes") or {}).get("raw") or {}).get("path"),
                    "raw_content_size_bytes": ((meta.get("data_sizes") or {}).get("raw") or {}).get("content_size_bytes"),
                    "raw_content_size_human": ((meta.get("data_sizes") or {}).get("raw") or {}).get("content_size_human"),
                    "processed_input_path": ((meta.get("data_sizes") or {}).get("processed") or {}).get("path"),
                    "processed_content_size_bytes": ((meta.get("data_sizes") or {}).get("processed") or {}).get("content_size_bytes"),
                    "processed_content_size_human": ((meta.get("data_sizes") or {}).get("processed") or {}).get("content_size_human"),
                    "repeats": meta.get("repeats"),
                    "iterations_total": summary.get("iterations_total"),
                    "iterations_ok": summary.get("iterations_ok"),
                    "iterations_failed": summary.get("iterations_failed"),
                    "durations_seconds_ok": summary.get("durations_seconds_ok") or [],
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
        "benchmark_name",
        "benchmark_program",
        "benchmark_command_shell",
        "master",
        "raw_input_path",
        "raw_content_size_bytes",
        "raw_content_size_human",
        "processed_input_path",
        "processed_content_size_bytes",
        "processed_content_size_human",
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
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def rows_to_markdown(rows: list[dict[str, Any]], title: str) -> str:
    lines = [
        f"## {title}",
        "",
        "| mode | run_timestamp | benchmark_name | raw_size | processed_size | ok/total | avg_s | min_s | max_s | run_dir |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        ok_total = f"{row['iterations_ok']}/{row['iterations_total']}"
        lines.append(
            f"| {row['mode']} | {row['run_timestamp']} | {row.get('benchmark_name') or 'n/a'} | {row.get('raw_content_size_human') or 'n/a'} | {row.get('processed_content_size_human') or 'n/a'} | {ok_total} | {fmt_seconds(row['duration_avg_seconds_ok'])} | {fmt_seconds(row['duration_min_seconds_ok'])} | {fmt_seconds(row['duration_max_seconds_ok'])} | `{row['run_dir']}` |"
        )
    if len(rows) == 0:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    return "\n".join(lines)


def details_to_markdown(rows: list[dict[str, Any]], title: str) -> str:
    lines = [f"## {title}", ""]
    if len(rows) == 0:
        lines.append("No runs included.")
        return "\n".join(lines)

    for row in rows:
        lines.append(f"- `{row['mode']}` `{row['run_timestamp']}` `{row.get('benchmark_name') or 'n/a'}`")
        lines.append(f"  - raw: `{row.get('raw_input_path') or 'n/a'}` ({row.get('raw_content_size_human') or 'n/a'})")
        lines.append(
            f"  - processed: `{row.get('processed_input_path') or 'n/a'}` ({row.get('processed_content_size_human') or 'n/a'})"
        )
        lines.append(f"  - command: `{row.get('benchmark_command_shell') or 'n/a'}`")
    return "\n".join(lines)


def summary_by_group(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        benchmark_name = row.get("benchmark_name") or "n/a"
        key = (benchmark_name, row["mode"])
        if key not in grouped:
            grouped[key] = {
                "benchmark_name": benchmark_name,
                "mode": row["mode"],
                "runs_included": 0,
                "iterations_total": 0,
                "iterations_ok": 0,
                "iterations_failed": 0,
                "durations_seconds_ok": [],
                "raw_sizes": set(),
                "processed_sizes": set(),
            }

        out = grouped[key]
        out["runs_included"] += 1
        out["iterations_total"] += int(row.get("iterations_total") or 0)
        out["iterations_ok"] += int(row.get("iterations_ok") or 0)
        out["iterations_failed"] += int(row.get("iterations_failed") or 0)
        out["durations_seconds_ok"].extend([float(v) for v in (row.get("durations_seconds_ok") or [])])
        out["raw_sizes"].add(row.get("raw_content_size_human") or "n/a")
        out["processed_sizes"].add(row.get("processed_content_size_human") or "n/a")

    out_rows: list[dict[str, Any]] = []
    for key in sorted(grouped.keys(), key=lambda item: (item[0], item[1])):
        row = grouped[key]
        durs = row["durations_seconds_ok"]
        out_rows.append(
            {
                "benchmark_name": row["benchmark_name"],
                "mode": row["mode"],
                "runs_included": row["runs_included"],
                "iterations_ok_total": row["iterations_ok"],
                "iterations_total": row["iterations_total"],
                "duration_avg_seconds_ok": (sum(durs) / len(durs)) if durs else None,
                "duration_min_seconds_ok": min(durs) if durs else None,
                "duration_max_seconds_ok": max(durs) if durs else None,
                "raw_size": ", ".join(sorted(row["raw_sizes"])),
                "processed_size": ", ".join(sorted(row["processed_sizes"])),
            }
        )
    return out_rows


def summary_to_markdown(rows: list[dict[str, Any]], title: str) -> str:
    lines = [
        f"## {title}",
        "",
        "| benchmark_name | mode | runs_included | ok/total_iters | raw_size | processed_size | avg_s | min_s | max_s |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if len(rows) == 0:
        lines.append("| n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
        return "\n".join(lines)

    for row in rows:
        ok_total = f"{row['iterations_ok_total']}/{row['iterations_total']}"
        lines.append(
            f"| {row['benchmark_name']} | {row['mode']} | {row['runs_included']} | {ok_total} | {row['raw_size']} | {row['processed_size']} | {fmt_seconds(row['duration_avg_seconds_ok'])} | {fmt_seconds(row['duration_min_seconds_ok'])} | {fmt_seconds(row['duration_max_seconds_ok'])} |"
        )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    runs_dir = Path(args.runs_dir)
    all_rows = scan_runs(runs_dir)
    selected_rows = latest_by_mode(all_rows) if args.latest_only else all_rows
    grouped_summary = summary_by_group(selected_rows)

    report_dir = safe_report_dir(Path(args.output_root))
    write_json(
        report_dir / "aggregate.json",
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "runs_dir": str(runs_dir),
            "rows_total": len(all_rows),
            "rows_included": len(selected_rows),
            "rows": selected_rows,
            "summary_by_group": grouped_summary,
        },
    )
    write_csv(report_dir / "aggregate.csv", selected_rows)

    md = [
        "# Benchmark summary report",
        "",
        f"Generated at (UTC): `{datetime.now(timezone.utc).isoformat()}`",
        f"Source runs dir: `{runs_dir}`",
        f"Report dir: `{report_dir}`",
        "",
        rows_to_markdown(selected_rows, "Included runs"),
        "",
        details_to_markdown(selected_rows, "Included run details"),
        "",
        summary_to_markdown(grouped_summary, "Summary"),
        "",
    ]
    (report_dir / "aggregate.md").write_text("\n".join(md), encoding="utf-8")

    print(f"Report directory: {report_dir}")
    print()
    print(summary_to_markdown(grouped_summary, "Summary"))


if __name__ == "__main__":
    main()
