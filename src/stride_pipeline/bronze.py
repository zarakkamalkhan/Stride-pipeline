"""
Bronze layer: parse raw source files into typed Spark DataFrames.

Rule for this layer: 1:1 with the source, nothing deduplicated or
reconciled yet. That happens in silver, where the dedup/merge key and
tie-break rule are explicit and documented. Bronze just has to be a
faithful, idempotent parse of whatever files are currently on disk --
run it twice on the same files, get the same rows.
"""
import glob
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_device_sync_samples(spark: SparkSession, data_dir: str, drops: List[str]) -> DataFrame:
    """
    One row per raw sample, across the given drop_N folders.

    Two payload schemas coexist in this data, keyed by companion app
    version (see README hazard #11 -- this is the single biggest hazard
    in the dataset, silently affecting exactly half the season's batches):
      - app 5.1.3: flat {hr, cadence: "<string>", lat, lon}
      - app 5.2.0: {heart_rate, cadence: <int>, gps: {lat, lon, hdop}}
    Spark's multi-file JSON schema merge gives us both sets of columns
    unioned with nulls; we coalesce them here into single hr/lat/lon
    columns and record which schema each row came from, rather than
    silently dropping the "other" one the way a naive `row['hr']` read
    would (that's exactly the mistake that first surfaced this hazard --
    see README > AI usage).
    """
    paths = []
    for drop in drops:
        paths.extend(sorted(glob.glob(f"{data_dir}/device_sync/{drop}/*.json")))
    if not paths:
        return _empty_samples_df(spark)

    raw = spark.read.option("multiLine", True).json(paths)
    raw = raw.withColumn("_source_file", F.input_file_name())
    raw = raw.withColumn("drop", F.regexp_extract("_source_file", r"(drop_\d+)", 1))

    exploded = raw.select(
        "batch_id", "device_id", "firmware", "app_version", "uploaded_at", "drop",
        "sample_count",
        F.posexplode("samples").alias("sample_idx", "sample"),
    )

    has_gps_col = "gps" in raw.schema["samples"].dataType.elementType.fieldNames()
    has_heart_rate_col = "heart_rate" in raw.schema["samples"].dataType.elementType.fieldNames()

    out = exploded.select(
        "batch_id", "device_id", "firmware", "app_version",
        F.to_timestamp("uploaded_at").alias("uploaded_at"),
        "drop", "sample_count", "sample_idx",
        F.col("sample.t").alias("t_local_raw"),
        F.col("sample.hr").alias("hr_v1"),
        (F.col("sample.heart_rate") if has_heart_rate_col else F.lit(None).cast("long")).alias("hr_v2"),
        F.col("sample.cadence").alias("cadence_raw"),
        F.col("sample.lat").alias("lat_v1"),
        F.col("sample.lon").alias("lon_v1"),
        (F.col("sample.gps.lat") if has_gps_col else F.lit(None).cast("double")).alias("lat_v2"),
        (F.col("sample.gps.lon") if has_gps_col else F.lit(None).cast("double")).alias("lon_v2"),
        (F.col("sample.gps.hdop") if has_gps_col else F.lit(None).cast("double")).alias("hdop"),
    )

    out = out.withColumn(
        "payload_schema",
        F.when(F.col("hr_v2").isNotNull(), F.lit("v2_nested_gps")).otherwise(F.lit("v1_flat")),
    ).withColumn(
        "hr", F.coalesce(F.col("hr_v1"), F.col("hr_v2"))
    ).withColumn(
        "lat", F.coalesce(F.col("lat_v1"), F.col("lat_v2"))
    ).withColumn(
        "lon", F.coalesce(F.col("lon_v1"), F.col("lon_v2"))
    ).withColumn(
        "cadence", F.col("cadence_raw").cast("double")
    ).withColumn(
        "t_local", F.to_timestamp("t_local_raw")
    ).drop("hr_v1", "hr_v2", "lat_v1", "lat_v2", "cadence_raw", "t_local_raw")

    return out


def _empty_samples_df(spark: SparkSession) -> DataFrame:
    cols = [
        "batch_id", "device_id", "firmware", "app_version", "uploaded_at", "drop",
        "sample_count", "sample_idx", "t_local", "cadence", "lat", "lon", "hdop",
        "payload_schema", "hr",
    ]
    return spark.createDataFrame([], schema=", ".join(f"{c} string" for c in cols))


def read_vendor_sessions_raw(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    One row per (session_id, fetched_at) as it appeared on a vendor_api
    page. Deliberately NOT deduped here -- page_4.json and
    page_4_retry.json both get read, so downstream silver logic can see
    (and prove) that the vendor portal revised a session's duration
    between the two fetches (README hazard #5), instead of that fact
    disappearing at ingest time.
    """
    paths = sorted(glob.glob(f"{data_dir}/vendor_api/*.json"))
    raw = spark.read.option("multiLine", True).json(paths)
    raw = raw.withColumn("_source_file", F.input_file_name())

    out = raw.select(
        F.to_timestamp("fetched_at").alias("fetched_at"),
        "_source_file",
        F.explode("sessions").alias("s"),
    ).select(
        F.col("s.session_id").alias("session_id"),
        F.col("s.device_id").alias("device_id"),
        F.col("s.sport").alias("sport"),
        F.to_timestamp("s.start_time").alias("start_time"),
        F.to_timestamp("s.end_time").alias("end_time"),
        F.col("s.duration_s").alias("duration_s"),
        F.col("s.distance_m").alias("distance_m"),
        F.col("s.sync_state").alias("sync_state"),
        "fetched_at",
        "_source_file",
    )
    return out


def read_roster_raw(spark: SparkSession, data_dir: str, snapshot: str) -> DataFrame:
    return spark.read.option("header", True).csv(f"{data_dir}/roster/roster_{snapshot}.csv")


def read_coach_labels_raw(spark: SparkSession, data_dir: str) -> DataFrame:
    df = spark.read.option("header", True).csv(f"{data_dir}/coach_app/session_labels.csv")
    return df.withColumn("session_date", F.to_date("session_date")).withColumn(
        "entered_at", F.to_timestamp("entered_at")
    )
