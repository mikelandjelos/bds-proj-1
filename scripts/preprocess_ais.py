#!/usr/bin/env python3
"""Preprocess Piraeus AIS dynamic CSV files into partitioned Parquet."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", nargs="+", required=True, help="Input AIS dynamic CSV path(s)")
    parser.add_argument("--output", required=True, help="Output dataset path")
    parser.add_argument("--static-path", help="Optional static vessel CSV path for enrichment")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    parser.add_argument(
        "--mode",
        default="overwrite",
        choices=["overwrite", "append", "error", "ignore"],
    )
    parser.add_argument("--partitions", type=int, default=32, help="Output partition count before write")
    parser.add_argument("--from-ts", type=int, help="Filter: UNIX epoch milliseconds lower bound")
    parser.add_argument("--to-ts", type=int, help="Filter: UNIX epoch milliseconds upper bound")
    parser.add_argument("--min-speed", type=float, help="Filter: minimum speed")
    parser.add_argument("--max-speed", type=float, help="Filter: maximum speed")
    parser.add_argument("--drop-null-geo", action="store_true", help="Drop rows with null lon/lat")
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=200,
        help="spark.sql.shuffle.partitions",
    )
    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    spark = (
        SparkSession.builder.appName("bds-proj1-preprocess")
        .master(args.master)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_dynamic_csv(spark: SparkSession, paths: list[str]):
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(DYNAMIC_SCHEMA)
        .csv(paths)
    )


def enrich_with_static(df, spark: SparkSession, static_path: str):
    static_df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(STATIC_SCHEMA)
        .csv(static_path)
        .withColumn("shiptype", F.col("shiptype").cast("int"))
    )
    return df.join(static_df, on="vessel_id", how="left")


def main() -> None:
    args = parse_args()
    spark = build_spark(args)

    df = load_dynamic_csv(spark, args.input)

    df = df.filter(F.col("timestamp").isNotNull() & F.col("vessel_id").isNotNull())

    if args.drop_null_geo:
        df = df.filter(F.col("lon").isNotNull() & F.col("lat").isNotNull())

    if args.from_ts is not None:
        df = df.filter(F.col("timestamp") >= F.lit(args.from_ts))

    if args.to_ts is not None:
        df = df.filter(F.col("timestamp") <= F.lit(args.to_ts))

    if args.min_speed is not None:
        df = df.filter(F.col("speed") >= F.lit(args.min_speed))

    if args.max_speed is not None:
        df = df.filter(F.col("speed") <= F.lit(args.max_speed))

    df = (
        df.withColumn(
            "event_ts",
            F.to_timestamp((F.col("timestamp") / F.lit(1000)).cast("double")),
        )
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
    )

    if args.static_path:
        df = enrich_with_static(df, spark, args.static_path)

    (
        df.repartition(args.partitions, "year", "month")
        .write.mode(args.mode)
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()
