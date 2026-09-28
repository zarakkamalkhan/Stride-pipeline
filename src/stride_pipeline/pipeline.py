"""
Orchestrates bronze -> silver -> gold and writes output.

Design decision on incremental/idempotent behaviour (see README > How to
run and > What I would do with another day for the full reasoning):

  - bronze/silver are a STATELESS RECOMPUTE from whatever raw files are
    currently under data/device_sync/<drops>/. At this data volume that's
    the simplest correct design and it is trivially idempotent (same
    files in -> same rows out) and trivially correct when drops arrive
    one at a time (each run just sees more files, and a session whose
    device_sync data is split across two drops correctly shows partial
    completeness until the second drop lands -- see README).

  - gold tables that are naturally date-grained (session_quality,
    hr_zone_minutes, low_completeness_sessions) are written PARTITIONED
    by session_local_date with Spark's dynamic partition overwrite mode,
    so writing a subset of dates only touches those dates' files on disk.
    weekly_training_load is partitioned by week_start for the same reason.
    This is what makes "reprocess a single day without touching the
    others" provable at the storage level (see reprocess_day.py).

  - squad_snapshot, discrepancies and athlete_baseline are not
    date-partitioned: they either aren't date-grained (squad_snapshot is
    a single as-of query) or legitimately depend on cross-day history
    (athlete_baseline's trailing window, discrepancies' revision log). We
    say so plainly rather than claim partition isolation we don't have.
"""
import glob
import os
import shutil
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from . import config
from . import bronze
from . import silver
from . import gold
from .session_quality import build_sample_zone, build_session_quality


PARTITIONED_MARTS = {
    "session_quality": "session_local_date",
    "hr_zone_minutes": "session_local_date",
    "low_completeness_sessions": "session_local_date",
    "weekly_training_load": "week_start",
}


def build_all(spark: SparkSession, data_dir: str, drops):
    sample_zone = build_sample_zone(spark, data_dir, drops).cache()
    sq = build_session_quality(spark, data_dir, drops).cache()

    marts = {
        "session_quality": sq,
        "hr_zone_minutes": gold.hr_zone_minutes(sq),
        "low_completeness_sessions": gold.low_completeness_sessions(sq),
        "weekly_training_load": gold.weekly_training_load(sq),
        "squad_snapshot_2026_08_14": gold.squad_snapshot(spark, data_dir, date(2026, 8, 14)),
        "athlete_baseline": gold.athlete_baseline(sq, sample_zone),
    }
    marts["discrepancies"] = gold.discrepancies(
        spark, data_dir, drops, sq, marts["athlete_baseline"]
    )
    return marts


def _flatten_single_csv(path_csv: str, final_name: str):
    """Spark writes part-00000-<uuid>-c000.csv + _SUCCESS + .crc files.
    Rename the one real part file to a fixed name so two runs are
    byte-identical AND filename-identical, and `diff` works directly
    without needing to know Spark's random uuid."""
    part_files = glob.glob(os.path.join(path_csv, "part-*.csv"))
    if not part_files:
        return
    target = os.path.join(path_csv, final_name)
    if os.path.exists(target):
        os.remove(target)
    shutil.move(part_files[0], target)
    for junk in glob.glob(os.path.join(path_csv, "*.crc")) + glob.glob(os.path.join(path_csv, "_SUCCESS")):
        os.remove(junk)


def _write_mart(df: DataFrame, name: str, output_dir: str, only_partition_values=None):
    path_parquet = os.path.join(output_dir, "parquet", name)
    path_csv = os.path.join(output_dir, "csv", name)

    partition_col = PARTITIONED_MARTS.get(name)
    if partition_col:
        if only_partition_values is not None:
            df = df.where(F.col(partition_col).isin(only_partition_values))
        (
            df.repartition(1)
            .write.mode("overwrite")
            .partitionBy(partition_col)
            .option("partitionOverwriteMode", "dynamic")
            .parquet(path_parquet)
        )
    else:
        df.repartition(1).write.mode("overwrite").parquet(path_parquet)

    # Deterministic-order CSV copy for easy human diffing / grading without Spark.
    order_cols = [c for c in ["session_local_date", "week_start", "session_id", "athlete_id", "category"] if c in df.columns]
    csv_df = df.orderBy(*order_cols) if order_cols else df
    csv_df.coalesce(1).write.mode("overwrite").option("header", True).csv(path_csv)
    _flatten_single_csv(path_csv, f"{name}.csv")


def run(spark: SparkSession, data_dir: str, drops, output_dir: str, only_date: date = None):
    marts = build_all(spark, data_dir, drops)

    for name, df in marts.items():
        if only_date is not None and name not in PARTITIONED_MARTS:
            continue  # not date-partitioned; a single-day run shouldn't touch these
        vals = None
        if only_date is not None:
            if PARTITIONED_MARTS.get(name) == "session_local_date":
                vals = [only_date]
            elif PARTITIONED_MARTS.get(name) == "week_start":
                import datetime as _dt
                monday = only_date - _dt.timedelta(days=only_date.weekday())
                vals = [monday]
        _write_mart(df, name, output_dir, only_partition_values=vals)

    return marts
