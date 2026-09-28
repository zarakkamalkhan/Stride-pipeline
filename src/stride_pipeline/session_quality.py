"""
Builds:
  - session_quality: one row per (deduped) vendor session
  - sample_zone: one row per raw sample with its HR zone (intermediate,
    used to build hr_zone_minutes)

This is where device_sync (raw truth about what the sensor recorded) meets
vendor_api (the session boundaries the vendor portal thinks exist) meets
the crosswalk (who was wearing the device). All three can disagree; this
module is where that gets reconciled and where disagreement gets recorded
rather than silently resolved.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from . import config
from . import bronze
from . import silver
from .crosswalk import build_crosswalk


def _sample_interval_seconds():
    # Hazard #3: firmware 2.4.0 samples at 2Hz (0.5s), 2.3.1 at 1Hz (1s).
    return F.when(F.col("firmware") == "2.4.0", F.lit(0.5)).otherwise(F.lit(1.0))


def _hr_zone_expr(hr_col, max_hr_col):
    pct = hr_col / max_hr_col
    expr = None
    for zone, lo, hi in config.HR_ZONE_BOUNDS:
        cond = (pct >= F.lit(lo)) & (pct < F.lit(hi))
        expr = F.when(cond, F.lit(zone)) if expr is None else expr.when(cond, F.lit(zone))
    return expr.otherwise(F.lit(None))


def build_sample_zone(spark: SparkSession, data_dir: str, drops) -> DataFrame:
    """One row per raw sample: which session it belongs to, whose it is,
    and which HR zone it falls in. Samples that don't fall inside any
    known vendor session window are kept with session_id = null -- that's
    a discrepancy (raw data with no session envelope), not something to
    drop silently."""
    samples = silver.clean_device_samples(spark, data_dir, drops).withColumn(
        "interval_s", _sample_interval_seconds()
    )

    sessions_raw = bronze.read_vendor_sessions_raw(spark, data_dir)
    sessions = silver.dedupe_vendor_sessions(sessions_raw)

    cw = build_crosswalk(spark, data_dir)

    # Range join: a sample belongs to a session on the same device whose
    # [start_time, end_time) window contains its true UTC instant. Verified
    # in tests that no device ever has two vendor sessions with overlapping
    # windows, so this match is unambiguous by construction, not just by
    # luck of the current three drops.
    joined = samples.join(
        F.broadcast(sessions),
        (samples.device_id == sessions.device_id)
        & (samples.t_utc >= sessions.start_time)
        & (samples.t_utc < sessions.end_time),
        "left",
    ).select(
        samples["*"],
        sessions["session_id"],
        sessions["start_time"].alias("session_start_time"),
        sessions["end_time"].alias("session_end_time"),
        sessions["duration_s"].alias("session_duration_s"),
    )

    # Attribute each sample to the athlete who owned its device on that
    # local calendar date (effective-dated crosswalk -- hazard #1 / #10).
    with_athlete = joined.join(
        cw,
        (joined.device_id == cw.device_id)
        & (F.to_date(joined.t_athlete_local) >= cw.effective_start)
        & (
            cw.effective_end.isNull()
            | (F.to_date(joined.t_athlete_local) < cw.effective_end)
        ),
        "left",
    ).select(
        joined["*"],
        cw["athlete_id"],
        cw["name"].alias("athlete_name"),
        cw["squad"],
        cw["max_hr"],
    )

    zoned = with_athlete.withColumn("hr_zone", _hr_zone_expr(F.col("hr"), F.col("max_hr")))
    return zoned


def coverage_and_zone_minutes(sample_zone: DataFrame):
    """Given a sample_zone-shaped DataFrame (session_id, t_utc, interval_s,
    hr_zone, batch_id, payload_schema), return (coverage, zone_minutes).

    Coverage: distinct seconds of the session window actually backed by a
    sample. Two batches CAN legitimately both match one session
    (S0b21423f does, in this data -- a clean reconnect split, no overlap)
    -- but nothing guarantees a future batch won't overlap another in true
    wall-clock time. Summing interval_s per matched sample would silently
    double-count that overlap and could push completeness_pct above 100%.
    So we first collapse to one row per (session_id, t_utc) -- same instant
    seen twice (whatever the reason) counts once -- and only then sum
    interval_s. This costs nothing in the common case (all timestamps
    already unique) and makes the aggregation correct in the uncommon one
    instead of silently wrong. Same reasoning applies to zone_minutes,
    which has the identical risk. Factored out of build_session_quality
    specifically so this arithmetic is unit-testable on its own (see
    tests/test_gold.py::test_coverage_deduplicates_overlapping_batches).
    """
    dedup_instant = sample_zone.where(F.col("session_id").isNotNull()).dropDuplicates(
        ["session_id", "t_utc"]
    )

    coverage = dedup_instant.groupBy("session_id").agg(
        F.sum("interval_s").alias("device_coverage_s"),
        F.count("*").alias("n_samples"),
        F.countDistinct("batch_id").alias("n_batches"),
        F.countDistinct("payload_schema").alias("n_payload_schemas"),
        F.min("t_utc").alias("first_sample_utc"),
        F.max("t_utc").alias("last_sample_utc"),
    )

    zone_minutes = (
        dedup_instant.where(F.col("hr_zone").isNotNull())
        .groupBy("session_id", "hr_zone")
        .agg((F.sum("interval_s") / 60.0).alias("minutes"))
        .groupBy("session_id")
        .pivot("hr_zone", ["Z1", "Z2", "Z3", "Z4", "Z5"])
        .agg(F.first("minutes"))
        .na.fill(0.0)
    )
    return coverage, zone_minutes


def build_session_quality(spark: SparkSession, data_dir: str, drops) -> DataFrame:
    sample_zone = build_sample_zone(spark, data_dir, drops)

    sessions_raw = bronze.read_vendor_sessions_raw(spark, data_dir)
    sessions = silver.dedupe_vendor_sessions(sessions_raw)
    cw = build_crosswalk(spark, data_dir)

    coverage, zone_minutes = coverage_and_zone_minutes(sample_zone)

    # Attach athlete/squad to the SESSION (by its own local start date) --
    # same crosswalk logic as per-sample, but this is the copy that ends
    # up on session_quality even for a session with zero matching samples.
    sess_with_local = sessions.withColumn(
        "session_local_date",
        F.to_date(F.col("start_time") + F.expr(f"make_interval(0,0,0,0,{config.DEFAULT_LOCAL_UTC_OFFSET_HOURS},0,0)")),
    )
    sess_with_athlete = sess_with_local.join(
        cw,
        (sess_with_local.device_id == cw.device_id)
        & (sess_with_local.session_local_date >= cw.effective_start)
        & (cw.effective_end.isNull() | (sess_with_local.session_local_date < cw.effective_end)),
        "left",
    ).select(
        sess_with_local["*"],
        cw["athlete_id"],
        cw["name"].alias("athlete_name"),
        cw["squad"],
    )

    sq = (
        sess_with_athlete.join(coverage, "session_id", "left")
        .join(zone_minutes, "session_id", "left")
        .withColumn("device_coverage_s", F.coalesce(F.col("device_coverage_s"), F.lit(0.0)))
        .withColumn(
            "completeness_pct",
            F.when(F.col("duration_s") > 0, F.col("device_coverage_s") / F.col("duration_s")).otherwise(F.lit(None)),
        )
        .withColumn(
            "below_completeness_threshold",
            F.col("completeness_pct") < F.lit(config.COMPLETENESS_THRESHOLD),
        )
        .withColumn(
            "below_completeness_floor",
            F.col("completeness_pct") < F.lit(config.COMPLETENESS_FLOOR),
        )
        .withColumn("has_athlete", F.col("athlete_id").isNotNull())
    )

    for z in ["Z1", "Z2", "Z3", "Z4", "Z5"]:
        sq = sq.withColumn(z, F.coalesce(F.col(z), F.lit(0.0)))

    sq = sq.withColumn(
        "training_load",
        sum(F.col(z) * config.ZONE_LOAD_WEIGHTS[z] for z in ["Z1", "Z2", "Z3", "Z4", "Z5"]),
    )

    return sq.select(
        "session_id", "athlete_id", "athlete_name", "squad", "device_id", "sport",
        "session_local_date", "start_time", "end_time", "duration_s",
        "device_coverage_s", "completeness_pct",
        "below_completeness_threshold", "below_completeness_floor",
        "n_samples", "n_batches", "n_payload_schemas", "has_athlete",
        "Z1", "Z2", "Z3", "Z4", "Z5", "training_load",
        "distance_m", "sync_state",
    )
