#!/usr/bin/env python3
"""Preprocess AIS CSV from HDFS into clean/quarantine/quality-report parquet outputs."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="hdfs://namenode:9000/bds/proj1/raw/*.csv",
        help="Input CSV path/glob",
    )
    parser.add_argument(
        "--output-base",
        default="hdfs://namenode:9000/bds/proj1/processed",
        help="Output base path",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master override (normally set via spark-submit --master)",
    )
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append", "ignore", "error"])
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument(
        "--exclude-file-regex",
        default=r".*sample.*",
        help="Exclude files whose full path matches regex",
    )
    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName("bds-proj1-preprocess-hdfs").config(
        "spark.sql.shuffle.partitions", str(args.shuffle_partitions)
    )
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def signature_expr(columns: list[str]):
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in columns],
        ),
        256,
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(args)

    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(DYNAMIC_SCHEMA)
        .csv(args.input)
        .withColumn("_source_file", F.input_file_name())
    )

    if args.exclude_file_regex:
        df = df.filter(~F.col("_source_file").rlike(args.exclude_file_regex))

    required_missing = (
        F.col("timestamp").isNull()
        | F.col("vessel_id").isNull()
        | (F.trim(F.col("vessel_id")) == "")
        | F.col("lon").isNull()
        | F.col("lat").isNull()
    )
    invalid_geo = (F.col("lon").isNotNull() & ((F.col("lon") < -180.0) | (F.col("lon") > 180.0))) | (
        F.col("lat").isNotNull() & ((F.col("lat") < -90.0) | (F.col("lat") > 90.0))
    )
    invalid_speed = F.col("speed").isNotNull() & (F.col("speed") < 0.0)
    invalid_heading = F.col("heading").isNotNull() & ((F.col("heading") < 0.0) | (F.col("heading") > 360.0))
    invalid_course = F.col("course").isNotNull() & ((F.col("course") < 0.0) | (F.col("course") > 360.0))

    columns_for_signature = ["timestamp", "vessel_id", "lon", "lat", "heading", "speed", "course"]
    df = df.withColumn("_signature", signature_expr(columns_for_signature))

    dup_window = Window.partitionBy("_signature").orderBy(F.col("timestamp").asc_nulls_last(), F.col("vessel_id"))
    df = (
        df.withColumn("_dup_count", F.count(F.lit(1)).over(Window.partitionBy("_signature")))
        .withColumn("_dup_rank", F.row_number().over(dup_window))
        .withColumn("duplicate_row", (F.col("_dup_count") > 1) & (F.col("_dup_rank") > 1))
    )

    df = (
        df.withColumn("missing_required", required_missing)
        .withColumn("invalid_geo", invalid_geo)
        .withColumn("invalid_speed", invalid_speed)
        .withColumn("invalid_heading", invalid_heading)
        .withColumn("invalid_course", invalid_course)
    )

    hard_reasons = F.array_remove(
        F.array(
            F.when(F.col("missing_required"), F.lit("missing_required")),
            F.when(F.col("invalid_geo"), F.lit("invalid_geo")),
            F.when(F.col("duplicate_row"), F.lit("duplicate_row")),
        ),
        None,
    )

    df = (
        df.withColumn("quarantine_reason_codes", hard_reasons)
        .withColumn("hard_fail", F.size(F.col("quarantine_reason_codes")) > 0)
        .withColumn(
            "quality_has_soft_issues", F.col("invalid_speed") | F.col("invalid_heading") | F.col("invalid_course")
        )
        .withColumn("event_ts", F.to_timestamp((F.col("timestamp") / F.lit(1000)).cast("double")))
        .withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
        .withColumn("hour", F.hour("event_ts"))
    )

    clean_df = (
        df.filter(~F.col("hard_fail"))
        .withColumn("speed", F.when(F.col("invalid_speed"), F.lit(None).cast("double")).otherwise(F.col("speed")))
        .withColumn("heading", F.when(F.col("invalid_heading"), F.lit(None).cast("double")).otherwise(F.col("heading")))
        .withColumn("course", F.when(F.col("invalid_course"), F.lit(None).cast("double")).otherwise(F.col("course")))
        .drop("_signature", "_dup_count", "_dup_rank", "hard_fail")
    )

    quarantine_df = (
        df.filter(F.col("hard_fail"))
        .withColumn("quarantine_reason_primary", F.element_at(F.col("quarantine_reason_codes"), 1))
        .drop("_signature", "_dup_count", "_dup_rank", "hard_fail")
    )

    total_count = df.count()
    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    summary_df = spark.createDataFrame(
        [
            {
                "total_records": total_count,
                "clean_records": clean_count,
                "quarantine_records": quarantine_count,
                "invalid_speed_records": df.filter(F.col("invalid_speed")).count(),
                "invalid_heading_records": df.filter(F.col("invalid_heading")).count(),
                "invalid_course_records": df.filter(F.col("invalid_course")).count(),
                "missing_required_records": df.filter(F.col("missing_required")).count(),
                "invalid_geo_records": df.filter(F.col("invalid_geo")).count(),
                "duplicate_records": df.filter(F.col("duplicate_row")).count(),
            }
        ]
    )

    reason_counts_df = (
        quarantine_df.select(F.explode(F.col("quarantine_reason_codes")).alias("reason"))
        .groupBy("reason")
        .agg(F.count(F.lit(1)).alias("records"))
        .orderBy(F.desc("records"), F.asc("reason"))
    )

    (
        clean_df.repartition(32, "year", "month")
        .write.mode(args.mode)
        .partitionBy("year", "month")
        .parquet(f"{args.output_base}/clean")
    )
    (
        quarantine_df.repartition(16, "quarantine_reason_primary")
        .write.mode(args.mode)
        .partitionBy("quarantine_reason_primary")
        .parquet(f"{args.output_base}/quarantine")
    )
    summary_df.write.mode(args.mode).parquet(f"{args.output_base}/quality_report/summary")
    reason_counts_df.write.mode(args.mode).parquet(f"{args.output_base}/quality_report/reason_counts")

    print(f"Total records: {total_count}")
    print(f"Clean records: {clean_count}")
    print(f"Quarantine records: {quarantine_count}")

    spark.stop()


if __name__ == "__main__":
    main()
