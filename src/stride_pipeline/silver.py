"""
Silver layer: the actual reconciliation logic. Every function here embeds
one specific hazard fix; each is named and commented after the hazard it
addresses in README > What I found, so the two documents cross-reference
each other instead of drifting apart.
"""
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from . import config
from . import bronze
from .crosswalk import build_crosswalk


# ---------------------------------------------------------------------------
# Hazard #7: client-side retry produced a byte-identical device_sync batch
# under a NEW batch_id, 38 seconds after the original upload. batch_id
# cannot be the dedup key; the actual recording window can.
# ---------------------------------------------------------------------------
def dedupe_device_sync_batches(samples: DataFrame) -> DataFrame:
    batch_meta = (
        samples.groupBy("batch_id", "device_id")
        .agg(
            F.min("t_local").alias("first_t_local"),
            F.max("t_local").alias("last_t_local"),
            F.count("*").alias("n_samples"),
        )
    )
    w = Window.partitionBy("device_id", "first_t_local", "last_t_local", "n_samples").orderBy(
        "batch_id"
    )
    canonical = (
        batch_meta.withColumn("_rk", F.row_number().over(w))
        .where(F.col("_rk") == 1)
        .select("batch_id")
    )
    return samples.join(canonical, on="batch_id", how="inner")


# ---------------------------------------------------------------------------
# Hazard #2: D014 (Zainab Mirza) reports local timestamps at UTC+3 for the
# entire season; every other device is UTC+5 (Asia/Karachi, no DST). We
# need TWO separate corrections that happen to diverge for D014:
#   t_utc            -- true UTC instant (uses each device's real offset)
#   t_athlete_local   -- Pakistan wall-clock time, for week/day bucketing
#                        (always UTC+5, since that's where the athlete and
#                        the programme actually are, regardless of what a
#                        misconfigured watch thinks its timezone is)
# For every device except D014 these are the same as the raw field, so the
# correction is a no-op everywhere except the one device that needs it.
# ---------------------------------------------------------------------------
def apply_timezone_correction(samples: DataFrame) -> DataFrame:
    offset_map = F.create_map(
        *sum(
            ([F.lit(k), F.lit(v)] for k, v in config.DEVICE_UTC_OFFSET_OVERRIDES.items()),
            [],
        )
    )
    return (
        samples.withColumn(
            "_device_utc_offset_h",
            F.coalesce(offset_map[F.col("device_id")], F.lit(config.DEFAULT_LOCAL_UTC_OFFSET_HOURS)),
        )
        .withColumn(
            "t_utc",
            (F.col("t_local").cast("double") - F.col("_device_utc_offset_h") * 3600).cast("timestamp"),
        )
        .withColumn(
            "t_athlete_local",
            (F.col("t_utc").cast("double") + F.lit(config.DEFAULT_LOCAL_UTC_OFFSET_HOURS) * 3600).cast("timestamp"),
        )
        .drop("_device_utc_offset_h")
    )

def sample_count_mismatches(spark: SparkSession, data_dir: str, drops) -> DataFrame:
    """Every batch where the header's declared `sample_count` doesn't match
    the actual number of samples parsed. This was checked by hand once
    across all 160 batches of the original three drops (0 mismatches) --
    but a hand check doesn't protect the pipeline against a FUTURE drop.
    The live follow-up session explicitly hands over a fourth drop, so
    this is exactly the kind of check that needs to be a standing part of
    the pipeline, not a one-off exploration script."""
    raw = bronze.read_device_sync_samples(spark, data_dir, drops)
    return (
        raw.groupBy("batch_id", "device_id", "drop")
        .agg(
            F.first("sample_count").alias("declared_sample_count"),
            F.count("*").alias("actual_sample_count"),
        )
        .where(F.col("declared_sample_count") != F.col("actual_sample_count"))
    )


def clean_device_samples(spark: SparkSession, data_dir: str, drops) -> DataFrame:
    raw = bronze.read_device_sync_samples(spark, data_dir, drops)
    deduped = dedupe_device_sync_batches(raw)
    return apply_timezone_correction(deduped)


# ---------------------------------------------------------------------------
# Hazard #6 (pagination overlap) + Hazard #5 (vendor session mutated between
# fetches): dedupe by session_id, latest fetched_at wins. This is a
# deliberate "latest wins" policy, not "first wins" -- the vendor portal is
# a live, mutable system, not an immutable log, so the most recent read is
# the best available truth. The superseded value is NOT discarded, though:
# see gold.discrepancies, which surfaces exactly this kind of revision.
# ---------------------------------------------------------------------------
def dedupe_vendor_sessions(sessions_raw: DataFrame) -> DataFrame:
    w = Window.partitionBy("session_id").orderBy(F.col("fetched_at").desc())
    return (
        sessions_raw.withColumn("_rk", F.row_number().over(w))
        .where(F.col("_rk") == 1)
        .drop("_rk", "_source_file")
    )


def session_revision_log(spark: SparkSession, data_dir: str) -> DataFrame:
    """Every session_id that was fetched more than once with different
    field values -- the evidence trail behind hazard #5."""
    raw = bronze.read_vendor_sessions_raw(spark, data_dir)
    grouped = raw.groupBy("session_id").agg(
        F.countDistinct(
            F.concat_ws("|", "start_time", "end_time", "duration_s", "distance_m", "sync_state")
        ).alias("distinct_versions"),
        F.collect_list(
            F.struct("fetched_at", "start_time", "end_time", "duration_s", "distance_m", "sync_state")
        ).alias("versions"),
    )
    return grouped.where(F.col("distinct_versions") > 1)


# ---------------------------------------------------------------------------
# Hazard #8: coach-app athlete_name is free text -- mixed case, doubled
# whitespace, and "First L." initials. Normalize and match against every
# roster spelling variant of every athlete's name. Verified separately
# (see tests/test_silver.py) that no two athletes on this roster collide
# under normalization -- with a 6-person roster today that holds, but it's
# exactly the kind of join that breaks silently on a bigger roster, so the
# collision check is a real test, not a formality.
# ---------------------------------------------------------------------------
def _normalize_name_expr(col):
    return F.lower(F.trim(F.regexp_replace(F.regexp_replace(col, r"\.", ""), r"\s+", " ")))


def build_name_lookup(spark: SparkSession, data_dir: str) -> DataFrame:
    cw = build_crosswalk(spark, data_dir).select("athlete_id", "name").distinct()
    full = cw.withColumn("variant", _normalize_name_expr(F.col("name")))
    first = F.split(F.col("name"), " ")[0]
    last = F.element_at(F.split(F.col("name"), " "), -1)
    initials_last = cw.withColumn(
        "variant", _normalize_name_expr(F.concat(first, F.lit(" "), F.substring(last, 1, 1), F.lit(".")))
    )
    first_initials = cw.withColumn(
        "variant", _normalize_name_expr(F.concat(F.substring(first, 1, 1), F.lit(". "), last))
    )
    return full.select("athlete_id", "variant").unionByName(
        initials_last.select("athlete_id", "variant")
    ).unionByName(first_initials.select("athlete_id", "variant")).distinct()


def clean_coach_labels(spark: SparkSession, data_dir: str) -> DataFrame:
    raw = bronze.read_coach_labels_raw(spark, data_dir)
    lookup = build_name_lookup(spark, data_dir)
    normalized = raw.withColumn("_variant", _normalize_name_expr(F.col("athlete_name")))
    joined = normalized.join(lookup, normalized["_variant"] == lookup["variant"], "left")
    return joined.select(
        "label_id", "athlete_id", "athlete_name", "session_date", "squad", "label", "rpe", "entered_at"
    )
