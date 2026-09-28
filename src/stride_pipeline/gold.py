"""
Gold marts. Each function returns exactly one of the deliverables asked
for in the brief. Grain and definitions for each are restated briefly in
the docstring and spelled out fully in README > Grain and keys /
Definitions -- this module should never be the only place a definition
lives.
"""
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from . import config
from . import bronze
from . import silver
from .crosswalk import build_crosswalk, squad_snapshot_on
from .session_quality import build_session_quality


# ---------------------------------------------------------------------------
# Q1: Weekly training load per athlete
# Grain: one row per (athlete_id, week_start). week_start is the Monday of
# a Mon-Sun window in athlete local time (Asia/Karachi).
# load = sum of zone-weighted minutes across every session whose local
# start date falls in that week (see config.ZONE_LOAD_WEIGHTS).
# ---------------------------------------------------------------------------
def weekly_training_load(sq: DataFrame) -> DataFrame:
    with_week = sq.withColumn("week_start", F.date_trunc("week", F.col("session_local_date")).cast("date"))
    w_last = Window.partitionBy("athlete_id", "week_start").orderBy(F.col("session_local_date").desc())
    with_squad_rank = with_week.withColumn("_rk", F.row_number().over(w_last))
    squad_as_of_week_end = with_squad_rank.where(F.col("_rk") == 1).select(
        "athlete_id", "week_start", F.col("squad").alias("squad_as_of_week_end")
    )

    agg = with_week.where(F.col("athlete_id").isNotNull()).groupBy("athlete_id", "athlete_name", "week_start").agg(
        F.count("*").alias("session_count"),
        F.sum("Z1").alias("z1_minutes"),
        F.sum("Z2").alias("z2_minutes"),
        F.sum("Z3").alias("z3_minutes"),
        F.sum("Z4").alias("z4_minutes"),
        F.sum("Z5").alias("z5_minutes"),
        F.sum("training_load").alias("training_load"),
        F.avg("completeness_pct").alias("avg_completeness_pct"),
        F.sum(F.when(F.col("below_completeness_threshold"), 1).otherwise(0)).alias(
            "low_completeness_session_count"
        ),
    )
    return (
        agg.join(squad_as_of_week_end, ["athlete_id", "week_start"], "left")
        .withColumn("total_minutes", F.col("z1_minutes") + F.col("z2_minutes") + F.col("z3_minutes") + F.col("z4_minutes") + F.col("z5_minutes"))
        .select(
            "athlete_id", "athlete_name", "squad_as_of_week_end", "week_start",
            "session_count", "total_minutes",
            "z1_minutes", "z2_minutes", "z3_minutes", "z4_minutes", "z5_minutes",
            "training_load", "avg_completeness_pct", "low_completeness_session_count",
        )
        .orderBy("athlete_id", "week_start")
    )


# ---------------------------------------------------------------------------
# Q2: HR-zone minutes per session. Grain: one row per session_id.
# Just the relevant slice of session_quality -- kept as its own function so
# it's a clearly separate deliverable, even though session_quality is the
# table that actually computes it.
# ---------------------------------------------------------------------------
def hr_zone_minutes(sq: DataFrame) -> DataFrame:
    return sq.select(
        "session_id", "athlete_id", "athlete_name", "squad", "session_local_date",
        "Z1", "Z2", "Z3", "Z4", "Z5",
        (F.col("Z1") + F.col("Z2") + F.col("Z3") + F.col("Z4") + F.col("Z5")).alias("total_minutes"),
        "completeness_pct",
    ).orderBy("session_local_date", "athlete_id")


# ---------------------------------------------------------------------------
# Q3: Sessions below the completeness threshold. See config.py for the
# threshold and the full rationale.
# ---------------------------------------------------------------------------
def low_completeness_sessions(sq: DataFrame) -> DataFrame:
    return sq.where(F.col("below_completeness_threshold")).select(
        "session_id", "athlete_id", "athlete_name", "device_id", "session_local_date",
        "duration_s", "device_coverage_s", "completeness_pct", "n_batches",
    ).orderBy("completeness_pct")


# ---------------------------------------------------------------------------
# Q4: Squad roster snapshot on a given date.
# ---------------------------------------------------------------------------
def squad_snapshot(spark: SparkSession, data_dir: str, as_of):
    return squad_snapshot_on(spark, data_dir, as_of)


# ---------------------------------------------------------------------------
# Part 2: athlete_baseline -- rolling resting HR & typical cadence.
# Grain: one row per (athlete_id, session_id) = the baseline AS KNOWN
# immediately before that session (trailing window, current session
# excluded), so it can be used to flag the current session as anomalous
# without circularity.
# ---------------------------------------------------------------------------
def athlete_baseline(sq: DataFrame, sample_zone: DataFrame) -> DataFrame:
    per_session = (
        sample_zone.where(F.col("session_id").isNotNull() & F.col("athlete_id").isNotNull())
        .groupBy("session_id", "athlete_id", "session_start_time")
        .agg(
            F.min("hr").alias("session_min_hr"),
            F.expr("percentile_approx(cadence, 0.5)").alias("session_median_cadence"),
        )
    )
    with_date = per_session.withColumn(
        "session_local_date",
        F.to_date(F.col("session_start_time") + F.expr(f"make_interval(0,0,0,0,{config.DEFAULT_LOCAL_UTC_OFFSET_HOURS},0,0)")),
    )

    days = config.BASELINE_TRAILING_DAYS * 86400
    w = (
        Window.partitionBy("athlete_id")
        .orderBy(F.col("session_local_date").cast("timestamp").cast("long"))
        .rangeBetween(-days, -1)  # strictly before the current session
    )
    w_count = Window.partitionBy("athlete_id").orderBy(F.col("session_local_date").cast("timestamp").cast("long"))

    with_baseline = (
        with_date.withColumn("_n_prior", F.count("session_id").over(w))
        .withColumn(
            "baseline_resting_hr",
            F.when(F.col("_n_prior") >= config.BASELINE_MIN_SESSIONS, F.avg("session_min_hr").over(w)),
        )
        .withColumn(
            "baseline_typical_cadence",
            F.when(F.col("_n_prior") >= config.BASELINE_MIN_SESSIONS, F.avg("session_median_cadence").over(w)),
        )
    )

    return with_baseline.select(
        "athlete_id", "session_id", "session_local_date",
        "session_min_hr", "session_median_cadence",
        "baseline_resting_hr", "baseline_typical_cadence",
        F.col("_n_prior").alias("n_prior_sessions_in_window"),
    ).orderBy("athlete_id", "session_local_date")


# ---------------------------------------------------------------------------
# Q5: discrepancies. One row per finding, tagged by category, so the
# sports-science lead (and the flow map) can see where in the pipeline each
# one was caught.
# ---------------------------------------------------------------------------
def discrepancies(
    spark: SparkSession, data_dir: str, drops, sq: DataFrame, baseline: DataFrame
) -> DataFrame:
    rows_schema = "category string, session_id string, athlete_id string, detail string, severity string"
    parts = []

    # 1. Vendor portal revised a session's data between fetches.
    revisions = silver.session_revision_log(spark, data_dir)
    rev_rows = revisions.select(
        F.lit("vendor_session_revised").alias("category"),
        F.col("session_id"),
        F.lit(None).cast("string").alias("athlete_id"),
        F.concat(F.lit("session_id="), F.col("session_id"), F.lit(" had "), F.col("distinct_versions"), F.lit(" distinct versions across vendor_api fetches")).alias("detail"),
        F.lit("medium").alias("severity"),
    )
    parts.append(rev_rows)

    # 2. Sessions below the completeness threshold.
    low_c = sq.where(F.col("below_completeness_threshold")).select(
        F.lit("low_completeness").alias("category"),
        "session_id", "athlete_id",
        F.concat(
            F.lit("completeness "), F.round(F.col("completeness_pct") * 100, 1).cast("string"), F.lit("% ("),
            F.col("device_coverage_s").cast("string"), F.lit("s of "), F.col("duration_s").cast("string"), F.lit("s)"),
        ).alias("detail"),
        F.lit("medium").alias("severity"),
    )
    parts.append(low_c)

    # 3. Sessions/raw data with no resolvable athlete (crosswalk gap).
    orphans = sq.where(~F.col("has_athlete")).select(
        F.lit("unattributed_device").alias("category"),
        "session_id", F.lit(None).cast("string").alias("athlete_id"),
        F.concat(F.lit("device_id="), F.col("device_id"), F.lit(" has no crosswalk owner on "), F.col("session_local_date").cast("string")).alias("detail"),
        F.lit("high").alias("severity"),
    )
    parts.append(orphans)

    # 4. Firmware/app version combinations the release notes say shouldn't
    #    sync (2.4.0 requires companion app >=5.2.0) but which appear in
    #    device_sync anyway.
    samples = silver.clean_device_samples(spark, data_dir, drops)
    bad_combo = (
        samples.where((F.col("firmware") == "2.4.0") & (F.col("app_version") == "5.1.3"))
        .select("batch_id", "device_id")
        .distinct()
        .select(
            F.lit("unsupported_firmware_app_combo").alias("category"),
            F.lit(None).cast("string").alias("session_id"),
            F.lit(None).cast("string").alias("athlete_id"),
            F.concat(F.lit("batch "), F.col("batch_id"), F.lit(" on "), F.col("device_id"), F.lit(" synced on firmware 2.4.0 with app 5.1.3 (release notes require app >=5.2.0)")).alias("detail"),
            F.lit("low").alias("severity"),
        )
    )
    parts.append(bad_combo)

    # 5. Declared vs actual sample_count per batch. Zero mismatches found
    #    across the three drops we have, but this check is a standing part
    #    of the pipeline (not just a one-off script) specifically because
    #    the live follow-up session hands over an unseen fourth drop.
    count_mismatch = silver.sample_count_mismatches(spark, data_dir, drops).select(
        F.lit("sample_count_mismatch").alias("category"),
        F.lit(None).cast("string").alias("session_id"),
        F.lit(None).cast("string").alias("athlete_id"),
        F.concat(
            F.lit("batch "), F.col("batch_id"), F.lit(" on "), F.col("device_id"),
            F.lit(" declared "), F.col("declared_sample_count").cast("string"),
            F.lit(" samples, parsed "), F.col("actual_sample_count").cast("string"),
        ).alias("detail"),
        F.lit("high").alias("severity"),
    )
    parts.append(count_mismatch)

    # 6. Baseline anomalies: this session's resting-HR-like reading or
    #    cadence is well outside the athlete's own recent baseline --
    #    could be a sensor fault, a mis-attributed device, or a genuine
    #    physiological change worth a human look.
    HR_DELTA_BPM = 15
    CADENCE_DELTA_SPM = 20
    anomalies = baseline.where(
        F.col("baseline_resting_hr").isNotNull()
        & (
            (F.abs(F.col("session_min_hr") - F.col("baseline_resting_hr")) > HR_DELTA_BPM)
            | (F.abs(F.col("session_median_cadence") - F.col("baseline_typical_cadence")) > CADENCE_DELTA_SPM)
        )
    ).select(
        F.lit("baseline_anomaly").alias("category"),
        "session_id", "athlete_id",
        F.concat(
            F.lit("resting-HR-like reading "), F.round(F.col("session_min_hr"), 1).cast("string"),
            F.lit(" vs baseline "), F.round(F.col("baseline_resting_hr"), 1).cast("string"),
            F.lit(" bpm; cadence "), F.round(F.col("session_median_cadence"), 1).cast("string"),
            F.lit(" vs baseline "), F.round(F.col("baseline_typical_cadence"), 1).cast("string"),
        ).alias("detail"),
        F.lit("medium").alias("severity"),
    )
    parts.append(anomalies)

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.orderBy("category", "session_id")
