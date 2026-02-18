#!/usr/bin/env python3
"""Spark batch analytics for Piraeus AIS data."""

from __future__ import annotations

import argparse
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

DYNAMIC_SCHEMA = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("vessel_id", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("course", DoubleType(), True),
    ]
)

STATIC_SCHEMA = StructType(
    [
        StructField("vessel_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("shiptype", DoubleType(), True),
    ]
)


def csv_columns(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input", required=True, help="Input dataset path")
    parser.add_argument("--input-format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--static-path", help="Optional static vessel CSV path for enrichment")
    parser.add_argument("--from-ts", type=int, help="Filter: UNIX epoch milliseconds lower bound")
    parser.add_argument("--to-ts", type=int, help="Filter: UNIX epoch milliseconds upper bound")
    parser.add_argument("--vessel-id", help="Filter by vessel_id")
    parser.add_argument("--country", help="Filter by country")
    parser.add_argument("--shiptype", type=int, help="Filter by shiptype")
    parser.add_argument("--min-speed", type=float, help="Filter by minimum speed")
    parser.add_argument("--max-speed", type=float, help="Filter by maximum speed")
    parser.add_argument("--where", help="Extra Spark SQL filter expression")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument("--app-name", default="bds-proj1-spark-batch")
    parser.add_argument("--shuffle-partitions", type=int, default=200)

    subparsers = parser.add_subparsers(dest="command", required=True)

    query_parser = subparsers.add_parser("query", help="Filter/sort/count/display rows")
    add_common_args(query_parser)
    query_parser.add_argument("--select", default="*", help="Comma-separated list of columns to display")
    query_parser.add_argument("--sort-by", help="Column to sort by")
    query_parser.add_argument("--sort-desc", action="store_true", help="Sort descending")
    query_parser.add_argument("--count-only", action="store_true", help="Only print count")
    query_parser.add_argument("--limit", type=int, default=20)
    query_parser.add_argument("--output", help="Optional output path")
    query_parser.add_argument("--output-format", choices=["parquet", "csv"], default="parquet")

    stats_parser = subparsers.add_parser("stats", help="Grouped statistics")
    add_common_args(stats_parser)
    stats_parser.add_argument("--group-by", required=True, help="Comma-separated grouping columns")
    stats_parser.add_argument("--metrics", default="speed", help="Comma-separated numeric columns")
    stats_parser.add_argument("--order-by", help="Optional order-by column")
    stats_parser.add_argument("--order-desc", action="store_true")
    stats_parser.add_argument("--limit", type=int, default=50)
    stats_parser.add_argument("--output", help="Optional output path")
    stats_parser.add_argument("--output-format", choices=["parquet", "csv"], default="parquet")

    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    spark = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.input_format == "parquet":
        df = spark.read.parquet(args.input)
    else:
        df = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .option("nullValue", "")
            .schema(DYNAMIC_SCHEMA)
            .csv(args.input)
        )

    if "event_ts" not in df.columns and "timestamp" in df.columns:
        df = (
            df.withColumn("event_ts", F.to_timestamp((F.col("timestamp") / F.lit(1000)).cast("double")))
            .withColumn("year", F.year("event_ts"))
            .withColumn("month", F.month("event_ts"))
            .withColumn("day", F.dayofmonth("event_ts"))
        )

    if args.static_path and ("country" not in df.columns or "shiptype" not in df.columns):
        static_df = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .option("nullValue", "")
            .schema(STATIC_SCHEMA)
            .csv(args.static_path)
            .withColumn("shiptype", F.col("shiptype").cast("int"))
        )
        df = df.join(static_df, on="vessel_id", how="left")

    return df


def require_columns(df: DataFrame, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing columns in input dataset: {', '.join(missing)}")


def apply_common_filters(df: DataFrame, args: argparse.Namespace) -> DataFrame:
    if args.from_ts is not None:
        require_columns(df, ["timestamp"])
        df = df.filter(F.col("timestamp") >= F.lit(args.from_ts))

    if args.to_ts is not None:
        require_columns(df, ["timestamp"])
        df = df.filter(F.col("timestamp") <= F.lit(args.to_ts))

    if args.vessel_id:
        require_columns(df, ["vessel_id"])
        df = df.filter(F.col("vessel_id") == F.lit(args.vessel_id))

    if args.country:
        require_columns(df, ["country"])
        df = df.filter(F.col("country") == F.lit(args.country))

    if args.shiptype is not None:
        require_columns(df, ["shiptype"])
        df = df.filter(F.col("shiptype") == F.lit(args.shiptype))

    if args.min_speed is not None:
        require_columns(df, ["speed"])
        df = df.filter(F.col("speed") >= F.lit(args.min_speed))

    if args.max_speed is not None:
        require_columns(df, ["speed"])
        df = df.filter(F.col("speed") <= F.lit(args.max_speed))

    if args.where:
        df = df.filter(F.expr(args.where))

    return df


def write_output(df: DataFrame, output_path: str, output_format: str) -> None:
    writer = df.write.mode("overwrite")
    if output_format == "parquet":
        writer.parquet(output_path)
    else:
        writer.option("header", True).csv(output_path)


def run_query(df: DataFrame, args: argparse.Namespace) -> None:
    if args.count_only:
        print(df.count())
        return

    selected_columns = None if args.select == "*" else csv_columns(args.select)
    if selected_columns:
        require_columns(df, selected_columns)
        df = df.select(*selected_columns)

    if args.sort_by:
        require_columns(df, [args.sort_by])
        order_expr = F.desc(args.sort_by) if args.sort_desc else F.asc(args.sort_by)
        df = df.orderBy(order_expr)

    if args.output:
        write_output(df, args.output, args.output_format)
        return

    df.show(args.limit, truncate=False)


def run_stats(df: DataFrame, args: argparse.Namespace) -> None:
    group_by = csv_columns(args.group_by)
    metrics = csv_columns(args.metrics)

    require_columns(df, group_by + metrics)

    aggregations = [F.count(F.lit(1)).alias("records")]
    for metric in metrics:
        aggregations.extend(
            [
                F.min(metric).alias(f"{metric}_min"),
                F.max(metric).alias(f"{metric}_max"),
                F.avg(metric).alias(f"{metric}_avg"),
                F.stddev_pop(metric).alias(f"{metric}_stddev"),
            ]
        )

    out = df.groupBy(*group_by).agg(*aggregations)

    if args.order_by:
        require_columns(out, [args.order_by])
        order_expr = F.desc(args.order_by) if args.order_desc else F.asc(args.order_by)
        out = out.orderBy(order_expr)

    if args.output:
        write_output(out, args.output, args.output_format)
        return

    out.show(args.limit, truncate=False)


def main() -> None:
    args = parse_args()
    spark = build_spark(args)

    try:
        df = load_data(spark, args)
        df = apply_common_filters(df, args)

        if args.command == "query":
            run_query(df, args)
        else:
            run_stats(df, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
