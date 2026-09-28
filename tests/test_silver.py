"""
Each test below is named after the hazard it protects against, and is
built from small synthetic rows rather than the real season data, so it
stays fast and fails for exactly one clear reason.
"""
import datetime as dt
import os
import shutil

from pyspark.sql import functions as F

from stride_pipeline import silver


# ---------------------------------------------------------------------------
# Test 1: retransmitted device_sync batches must collapse to one, keyed by
# recording window (device_id, first_t, last_t, n_samples), NOT batch_id.
# Protects against: hazard #7 -- a client retry re-uploads byte-identical
# samples under a brand new batch_id, 38 seconds later. If dedup ever keys
# on batch_id (the "obvious" natural key), every retried session silently
# gets double-counted -- doubled HR-zone minutes, doubled training load,
# for exactly the sessions where the phone's connection was flakiest.
# ---------------------------------------------------------------------------
def test_retransmitted_batch_collapses_to_one(spark):
    rows = []
    base_t = dt.datetime(2026, 7, 22, 6, 20, 0)
    for i in range(5):
        t = (base_t + dt.timedelta(seconds=i)).isoformat()
        # Same device, same window, same sample count, TWO different
        # batch_ids -- exactly the real B...aea8e6fa-8 / B...1c77ecc9-7 case.
        rows.append(("BATCH_A", "D011", "2.3.1", "5.1.3", t, 51 + i, "120", 33.68, 73.05))
        rows.append(("BATCH_B", "D011", "2.3.1", "5.1.3", t, 51 + i, "120", 33.68, 73.05))

    df = spark.createDataFrame(
        rows,
        "batch_id string, device_id string, firmware string, app_version string, "
        "t_local string, hr int, cadence string, lat double, lon double",
    ).withColumn("t_local", F.to_timestamp("t_local"))

    deduped = silver.dedupe_device_sync_batches(df)
    assert deduped.select("batch_id").distinct().count() == 1
    assert deduped.count() == 5  # not 10


# ---------------------------------------------------------------------------
# Test 2: the D014 timezone override must shift ONLY D014, and it must
# produce the correct t_utc (uses the device's real offset) while leaving
# t_athlete_local on Pakistan wall-clock time (not the device's misreading).
# Protects against: hazard #2 -- a device-level clock/timezone bug that,
# if missed, silently shifts every one of D014's ~30 sessions onto the
# wrong UTC instant (breaking the vendor-session join entirely) and onto
# the wrong local calendar day/week (breaking weekly_training_load).
# ---------------------------------------------------------------------------
def test_timezone_override_applies_only_to_d014(spark):
    rows = [
        ("D011", "2026-07-22T06:20:00"),  # regular device, UTC+5
        ("D014", "2026-07-22T06:20:00"),  # same raw local clock reading
    ]
    df = spark.createDataFrame(rows, "device_id string, t_local string").withColumn(
        "t_local", F.to_timestamp("t_local")
    )
    out = (
        silver.apply_timezone_correction(df)
        .withColumn("t_utc_str", F.date_format("t_utc", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("t_athlete_local_str", F.date_format("t_athlete_local", "yyyy-MM-dd HH:mm:ss"))
        .collect()
    )
    by_device = {r["device_id"]: r for r in out}

    assert by_device["D011"]["t_utc_str"] == "2026-07-22 01:20:00"
    assert by_device["D014"]["t_utc_str"] == "2026-07-22 03:20:00"
    assert by_device["D011"]["t_athlete_local_str"] == "2026-07-22 06:20:00"
    assert by_device["D014"]["t_athlete_local_str"] == "2026-07-22 08:20:00"


# ---------------------------------------------------------------------------
# Test 3: vendor session dedup keeps the LATEST fetched_at version, not the
# first one seen.
# Protects against: hazard #5 -- the vendor portal is a live, mutable
# system. sessions_page_4_retry revised Se06bc0e7's duration upward by
# 360s six hours after the original fetch. "First wins" (a very natural
# default for a naive `dropDuplicates(["session_id"])`) would silently
# keep the STALE value and miss that the portal ever changed its mind --
# which is itself evidence we surface as a discrepancy, not just data to
# discard.
# ---------------------------------------------------------------------------
def test_session_dedup_keeps_latest_fetch(spark):
    rows = [
        ("S1", "D011", "run", "2026-08-21T01:45:00", "2026-08-21T02:19:00", 2040, 5918, "complete",
         "2026-09-01T09:12:28"),
        ("S1", "D011", "run", "2026-08-21T01:45:00", "2026-08-21T02:25:00", 2400, 5918, "complete",
         "2026-09-01T15:12:00"),  # later fetch, corrected duration
    ]
    df = (
        spark.createDataFrame(
            rows,
            "session_id string, device_id string, sport string, start_time string, end_time string, "
            "duration_s int, distance_m int, sync_state string, fetched_at string",
        )
        .withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("end_time", F.to_timestamp("end_time"))
        .withColumn("fetched_at", F.to_timestamp("fetched_at"))
    )
    out = silver.dedupe_vendor_sessions(df).collect()
    assert len(out) == 1
    assert out[0]["duration_s"] == 2400  # the retry's value, not the original 2040


# ---------------------------------------------------------------------------
# Test 4: coach-app name normalization must not silently collide two
# different athletes onto the same athlete_id.
# Protects against: hazard #8 -- coach notes use free-text names with
# mixed case and "First L." initials. On today's 6-person roster this is
# safe (checked explicitly here), but it is exactly the kind of join that
# breaks the moment two athletes share an initial ("Bilal Ahmed" and
# "Bilal Aziz" would both normalize toward "bilal a") -- this test proves
# the CURRENT roster doesn't hit that case, and is meant to be the first
# thing that fails if a same-initial athlete is ever added without
# updating the matching logic.
# ---------------------------------------------------------------------------
def test_name_variants_do_not_collide_across_roster(spark, tmp_path):
    data_dir = tmp_path / "data"
    (data_dir / "roster").mkdir(parents=True)
    real_roster = os.path.join(os.path.dirname(__file__), "..", "data", "roster", "roster_2026-09-01.csv")
    shutil.copy(real_roster, data_dir / "roster" / "roster_2026-07-01.csv")
    shutil.copy(real_roster, data_dir / "roster" / "roster_2026-09-01.csv")

    lookup = silver.build_name_lookup(spark, str(data_dir))
    collisions = (
        lookup.groupBy("variant")
        .agg(F.countDistinct("athlete_id").alias("n"))
        .where("n > 1")
        .collect()
    )
    assert collisions == [], f"name variants map to more than one athlete_id: {collisions}"
