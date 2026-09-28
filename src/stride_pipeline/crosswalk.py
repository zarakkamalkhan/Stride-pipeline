"""
Builds an EFFECTIVE-DATED crosswalk of athlete <-> squad <-> device.

Why this exists at all: device_id is not a stable key for an athlete over
the season (see README hazard #1), and squad is not stable either (hazard
#10). The two roster CSVs are just two point-in-time snapshots; they bracket
the changes but don't date them. We reconstruct the missing dates from
evidence inside the season data itself (vendor session dates per device,
coach-app squad labels per date) and hard-code the two resulting cutover
dates in config.py, with the reasoning recorded there.

Every other row in this crosswalk (the four athletes who never changed
device or squad) needs no reconstruction -- it's read straight off either
roster file, since they agree outside the two changed rows.

Output grain: one row per (athlete_id, effective_start, effective_end)
with squad and device_id valid for that window. effective_end is exclusive;
NULL/open-ended means "still current at season end".
"""
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)

from . import config

CROSSWALK_SCHEMA = StructType([
    StructField("athlete_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("squad", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("max_hr", StringType(), False),   # cast later; keep source-typed here
    StructField("rest_hr", StringType(), False),
    StructField("effective_start", DateType(), False),
    StructField("effective_end", DateType(), True),  # null = open-ended
])


def build_crosswalk(spark: SparkSession, data_dir: str) -> DataFrame:
    roster_jul = (
        spark.read.option("header", True).csv(f"{data_dir}/roster/roster_2026-07-01.csv")
    )
    roster_sep = (
        spark.read.option("header", True).csv(f"{data_dir}/roster/roster_2026-09-01.csv")
    )

    jul = {r["athlete_id"]: r for r in roster_jul.collect()}
    sep = {r["athlete_id"]: r for r in roster_sep.collect()}

    rows = []
    season_start = config.SEASON_START
    season_end = config.SEASON_END

    for athlete_id, sep_row in sep.items():
        jul_row = jul.get(athlete_id)

        if athlete_id == "A06":
            # Mehak Noor: not on the July roster at all -- joined mid-season.
            # First evidence of her in the data is her own device (D013,
            # freed up when Hamza moved to D017) and coach-app entries
            # starting 2026-08-14 (config.MEHAK_DEVICE_START_DATE).
            rows.append((
                athlete_id, sep_row["name"], sep_row["squad"], sep_row["device_id"],
                sep_row["max_hr"], sep_row["rest_hr"],
                config.MEHAK_DEVICE_START_DATE, None,
            ))
            continue

        if jul_row is None:
            # Defensive: any other future new athlete with no July row.
            rows.append((
                athlete_id, sep_row["name"], sep_row["squad"], sep_row["device_id"],
                sep_row["max_hr"], sep_row["rest_hr"], season_start, None,
            ))
            continue

        if athlete_id == "A03":
            # Hamza Tariq: device changes D013 -> D017 on
            # config.HAMZA_DEVICE_CHANGE_DATE. Squad unchanged (Speed).
            rows.append((
                athlete_id, jul_row["name"], jul_row["squad"], jul_row["device_id"],
                jul_row["max_hr"], jul_row["rest_hr"],
                season_start, config.HAMZA_DEVICE_CHANGE_DATE,
            ))
            rows.append((
                athlete_id, sep_row["name"], sep_row["squad"], sep_row["device_id"],
                sep_row["max_hr"], sep_row["rest_hr"],
                config.HAMZA_DEVICE_CHANGE_DATE, None,
            ))
            continue

        if athlete_id == "A05":
            # Bilal Ahmed: squad changes Endurance -> Speed on
            # config.BILAL_SQUAD_CHANGE_DATE. Device unchanged (D015).
            rows.append((
                athlete_id, jul_row["name"], jul_row["squad"], jul_row["device_id"],
                jul_row["max_hr"], jul_row["rest_hr"],
                season_start, config.BILAL_SQUAD_CHANGE_DATE,
            ))
            rows.append((
                athlete_id, sep_row["name"], sep_row["squad"], sep_row["device_id"],
                sep_row["max_hr"], sep_row["rest_hr"],
                config.BILAL_SQUAD_CHANGE_DATE, None,
            ))
            continue

        # Everyone else: unchanged all season (both snapshots agree).
        assert jul_row["squad"] == sep_row["squad"], athlete_id
        assert jul_row["device_id"] == sep_row["device_id"], athlete_id
        rows.append((
            athlete_id, jul_row["name"], jul_row["squad"], jul_row["device_id"],
            jul_row["max_hr"], jul_row["rest_hr"], season_start, None,
        ))

    # D013 is idle for two days (Aug 12-13) between Hamza leaving and
    # Mehak arriving -- it deliberately has no owning row in that window.
    # (No vendor session ever falls in that gap, so the exact placement of
    # HAMZA_DEVICE_CHANGE_DATE within Aug 11-13 doesn't change any
    # attributed session -- only how the gap itself would be explained if
    # a future drop ever puts data there.)
    # Any device_sync/vendor data for D013 in that gap surfaces as an
    # unattributable-device discrepancy (see gold.discrepancies), which is
    # the correct behaviour: we don't guess an owner we have no evidence for.

    df = spark.createDataFrame(rows, schema=CROSSWALK_SCHEMA)
    df = df.withColumn("max_hr", F.col("max_hr").cast("int"))
    df = df.withColumn("rest_hr", F.col("rest_hr").cast("int"))
    return df


def squad_snapshot_on(spark: SparkSession, data_dir: str, as_of: date) -> DataFrame:
    """Question 4: who was in which squad, on which device, on `as_of`."""
    cw = build_crosswalk(spark, data_dir)
    return (
        cw.where(
            (F.col("effective_start") <= F.lit(as_of))
            & (F.col("effective_end").isNull() | (F.col("effective_end") > F.lit(as_of)))
        )
        .select("athlete_id", "name", "squad", "device_id", "max_hr", "rest_hr")
        .orderBy("athlete_id")
    )
