#!/usr/bin/env python3
"""Phase 4 step 1: basic analytics CLI over processed AIS data in HDFS."""

from __future__ import annotations

import argparse
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def csv_columns(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def add_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--vessel-id", help="Filter by vessel_id")
    parser.add_argument("--from-ts", type=int, help="Filter: timestamp >= this UNIX epoch milliseconds value")
    parser.add_argument("--to-ts", type=int, help="Filter: timestamp <= this UNIX epoch milliseconds value")
    parser.add_argument("--year", type=int, help="Filter by partition column year")
    parser.add_argument("--month", type=int, help="Filter by partition column month")
    parser.add_argument("--min-speed", type=float, help="Filter: speed >= value")
    parser.add_argument("--max-speed", type=float, help="Filter: speed <= value")
    parser.add_argument(
        "--where",
        action="append",
        default=[],
        help="Extra Spark SQL filter expression (repeatable)",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-name", default="bds-proj1-analytics-step1")
    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/bds/proj1/processed/clean",
        help="Input parquet path in HDFS",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master override (normally set via spark-submit --master)",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=200)

    subparsers = parser.add_subparsers(dest="command", required=True)

    count_parser = subparsers.add_parser("count", help="Count rows after filters")
    add_filter_args(count_parser)

    show_parser = subparsers.add_parser("show", help="Display rows after filters")
    add_filter_args(show_parser)
    show_parser.add_argument(
        "--select",
        default="timestamp,vessel_id,lon,lat,speed,year,month",
        help="Comma-separated columns to display, or '*'",
    )
    show_parser.add_argument("--sort-by", help="Column to sort by")
    show_parser.add_argument("--sort-desc", action="store_true", help="Sort descending")
    show_parser.add_argument("--limit", type=int, default=20, help="Maximum rows to display")

    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName(args.app_name).config(
        "spark.sql.shuffle.partitions", str(args.shuffle_partitions)
    )
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def require_columns(df: DataFrame, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing columns in input dataset: {', '.join(missing)}")


def apply_filters(df: DataFrame, args: argparse.Namespace) -> DataFrame:
    if args.vessel_id:
        require_columns(df, ["vessel_id"])
        df = df.filter(F.col("vessel_id") == F.lit(args.vessel_id))

    if args.from_ts is not None:
        require_columns(df, ["timestamp"])
        df = df.filter(F.col("timestamp") >= F.lit(args.from_ts))

    if args.to_ts is not None:
        require_columns(df, ["timestamp"])
        df = df.filter(F.col("timestamp") <= F.lit(args.to_ts))

    if args.year is not None:
        require_columns(df, ["year"])
        df = df.filter(F.col("year") == F.lit(args.year))

    if args.month is not None:
        require_columns(df, ["month"])
        df = df.filter(F.col("month") == F.lit(args.month))

    if args.min_speed is not None:
        require_columns(df, ["speed"])
        df = df.filter(F.col("speed") >= F.lit(args.min_speed))

    if args.max_speed is not None:
        require_columns(df, ["speed"])
        df = df.filter(F.col("speed") <= F.lit(args.max_speed))

    for expression in args.where:
        df = df.filter(F.expr(expression))

    return df


def run_count(df: DataFrame) -> None:
    print(df.count())


def run_show(df: DataFrame, args: argparse.Namespace) -> None:
    if args.sort_by:
        require_columns(df, [args.sort_by])
        order_expr = F.desc(args.sort_by) if args.sort_desc else F.asc(args.sort_by)
        df = df.orderBy(order_expr)

    selected_columns = None if args.select == "*" else csv_columns(args.select)
    if selected_columns:
        require_columns(df, selected_columns)
        df = df.select(*selected_columns)

    df.show(args.limit, truncate=False)


def main() -> None:
    args = parse_args()
    spark = build_spark(args)

    try:
        df = spark.read.parquet(args.input)
        df = apply_filters(df, args)

        if args.command == "count":
            run_count(df)
        else:
            run_show(df, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
