import datetime as dt
import json
import os

from pyspark.sql import functions as F

from stride_pipeline import bronze, crosswalk, config, gold, silver
from stride_pipeline.session_quality import _hr_zone_expr, coverage_and_zone_minutes


# ---------------------------------------------------------------------------
# Test 5 (the one most worth reading): the two device_sync payload schemas
# must both surface as usable hr/lat/lon, not just whichever one a naive
# reader checks for first.
# Protects against: hazard #11 -- THE central hazard in this dataset.
# Companion app 5.2.0 renamed hr -> heart_rate and flattened lat/lon into a
# nested gps struct. This affects exactly half of all 160 device_sync
# batches. The bug this test would have caught: my first pass at this
# script read `sample['hr']` directly and got a hard KeyError on the first
# new-schema batch I hit -- which is how this hazard was found at all (see
# README > AI usage). A softer version of that same bug -- `sample.get(
# 'hr')` defaulting to None instead of raising -- would have silently
# zeroed out HR data for half the season with no error at all.
# ---------------------------------------------------------------------------
def test_both_payload_schemas_produce_usable_hr_and_gps(spark, tmp_path):
    data_dir = tmp_path / "data" / "device_sync" / "drop_1"
    data_dir.mkdir(parents=True)

    old_schema = {
        "batch_id": "OLD1", "device_id": "D011", "firmware": "2.3.1", "app_version": "5.1.3",
        "uploaded_at": "2026-07-06T02:15:00Z", "sample_count": 2,
        "samples": [
            {"t": "2026-07-06T06:00:00", "hr": 120, "cadence": "150", "lat": 33.68, "lon": 73.05},
            {"t": "2026-07-06T06:00:01", "hr": 121, "cadence": "151", "lat": 33.68, "lon": 73.05},
        ],
    }
    new_schema = {
        "batch_id": "NEW1", "device_id": "D012", "firmware": "2.3.1", "app_version": "5.2.0",
        "uploaded_at": "2026-08-05T12:26:00Z", "sample_count": 2,
        "samples": [
            {"t": "2026-08-05T17:26:00", "heart_rate": 137, "cadence": 175,
             "gps": {"lat": 33.71, "lon": 73.03, "hdop": 1.6}},
            {"t": "2026-08-05T17:26:01", "heart_rate": 136, "cadence": 179,
             "gps": {"lat": 33.71, "lon": 73.03, "hdop": 2.3}},
        ],
    }
    (data_dir / "OLD1.json").write_text(json.dumps(old_schema))
    (data_dir / "NEW1.json").write_text(json.dumps(new_schema))

    out = bronze.read_device_sync_samples(spark, str(tmp_path / "data"), ["drop_1"]).collect()
    by_batch = {}
    for r in out:
        by_batch.setdefault(r["batch_id"], []).append(r)

    assert len(by_batch["OLD1"]) == 2
    assert len(by_batch["NEW1"]) == 2
    for r in by_batch["OLD1"]:
        assert r["hr"] is not None and r["lat"] is not None and r["lon"] is not None
        assert r["payload_schema"] == "v1_flat"
    for r in by_batch["NEW1"]:
        assert r["hr"] is not None and r["lat"] is not None and r["lon"] is not None
        assert r["payload_schema"] == "v2_nested_gps"
    assert by_batch["NEW1"][0]["hr"] == 137  # came from heart_rate, not a null hr column


# ---------------------------------------------------------------------------
# HR zone boundaries are half-open [lo, hi) except Z5, which is open-ended.
# Protects against an off-by-one at exactly 60/70/80/90% of max_hr, which
# is exactly where a >= vs > typo would silently misclassify a sample into
# the neighbouring zone.
# ---------------------------------------------------------------------------
def test_hr_zone_boundaries_are_half_open(spark):
    max_hr = 200
    # pct: 0.599 -> Z1, 0.60 -> Z2, 0.699 -> Z2, 0.70 -> Z3, 0.90 -> Z5
    hrs = [119, 120, 139, 140, 180]
    df = spark.createDataFrame([(hr,) for hr in hrs], "hr int").withColumn(
        "max_hr", F.lit(max_hr)
    )
    out = df.withColumn("zone", _hr_zone_expr(F.col("hr"), F.col("max_hr"))).collect()
    zones = [r["zone"] for r in out]
    assert zones == ["Z1", "Z2", "Z2", "Z3", "Z5"]


# ---------------------------------------------------------------------------
# The device/squad crosswalk must attribute sessions across the Hamza ->
# D017 / Mehak -> D013 handover exactly the way the control figures imply,
# not the way a naive "latest roster row wins" join would.
# Protects against: hazard #1. A static join on the 2026-09-01 roster alone
# would put every D013 session -- including Hamza's, back in July -- onto
# Mehak Noor, who didn't exist yet.
# ---------------------------------------------------------------------------
def test_device_handover_matches_control_week(spark):
    real_data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    cw = crosswalk.build_crosswalk(spark, real_data_dir)

    def owner(device_id, on_date):
        rows = cw.where(
            (F.col("device_id") == device_id)
            & (F.col("effective_start") <= F.lit(on_date))
            & (F.col("effective_end").isNull() | (F.col("effective_end") > F.lit(on_date)))
        ).collect()
        return rows[0]["name"] if rows else None

    # Control figures for week 2026-08-10..16: Hamza Tariq=4, Mehak Noor=2.
    # That split only reconciles if D013 is still Hamza's on Aug 10, and
    # D017 is already Hamza's by Aug 12 (he moves devices mid-week), while
    # D013 doesn't become Mehak's until Aug 14.
    assert owner("D013", dt.date(2026, 8, 10)) == "Hamza Tariq"
    assert owner("D017", dt.date(2026, 8, 12)) == "Hamza Tariq"
    assert owner("D013", dt.date(2026, 8, 14)) == "Mehak Noor"
    # The three idle days in between belong to no one -- that's
    # deliberate, not a bug (see crosswalk.py).
    assert owner("D013", dt.date(2026, 8, 12)) is None


# ---------------------------------------------------------------------------
# Test 8: completeness must not double-count when two batches' samples
# overlap in true wall-clock time.
# Protects against a latent bug found while re-checking this build for
# gaps: the original implementation summed interval_s across every
# matched sample with no de-duplication. That's correct for every session
# in the actual three drops (checked by hand: the one real multi-batch
# session, S0b21423f, has two batches that do NOT overlap) -- but nothing
# in the data GUARANTEES that, and the live follow-up session hands over
# an unseen fourth drop. If a future batch overlapped another in time,
# naive summation would push completeness_pct above 100% instead of
# flagging anything. This test constructs exactly that case synthetically
# (two batches, 3 seconds of genuine overlap) and asserts coverage is the
# true UNION of covered seconds, not the sum of both batches' lengths.
# ---------------------------------------------------------------------------
def test_coverage_deduplicates_overlapping_batches(spark):
    base = dt.datetime(2026, 8, 1, 6, 0, 0)
    rows = []
    # Batch A: seconds 0..6 (7 samples)
    for i in range(7):
        t = base + dt.timedelta(seconds=i)
        rows.append(("BATCH_A", "S1", t, 1.0, "Z2", "v1_flat"))
    # Batch B: seconds 4..9 (6 samples) -- overlaps A on seconds 4,5,6
    for i in range(4, 10):
        t = base + dt.timedelta(seconds=i)
        rows.append(("BATCH_B", "S1", t, 1.0, "Z2", "v1_flat"))

    df = spark.createDataFrame(
        rows, "batch_id string, session_id string, t_utc timestamp, interval_s double, hr_zone string, payload_schema string"
    )
    coverage, zone_minutes = coverage_and_zone_minutes(df)
    cov_row = coverage.collect()[0]

    # True union is seconds 0..9 inclusive = 10 distinct seconds, NOT
    # 7 + 6 = 13 (which is what naive summation would have given).
    assert cov_row["device_coverage_s"] == 10.0
    assert cov_row["n_batches"] == 2

    zm_row = zone_minutes.collect()[0]
    assert zm_row["Z2"] == 10.0 / 60.0


# ---------------------------------------------------------------------------
# Test 9: the baseline-anomaly discrepancy check must actually fire on a
# genuine anomaly, not just stay silent.
# Protects against a real gap flagged when reviewing this build: on the
# actual season data, athlete_baseline currently finds ZERO anomalies --
# which is a legitimate result (every device/athlete attribution already
# reconciles against the control figures), but it means the detector had
# never been proven to catch anything. A detector that only ever returns
# "nothing found" is indistinguishable from a detector that's silently
# broken. This constructs a session whose resting-HR-like reading is far
# outside the athlete's own trailing baseline and asserts it gets flagged.
# ---------------------------------------------------------------------------
def test_baseline_anomaly_check_fires_on_genuine_outlier(spark):
    # Athlete A01, 4 prior "normal" sessions (min_hr ~50) then one
    # anomalous session (min_hr 90 -- 40bpm above baseline).
    rows = []
    dates = ["2026-08-01", "2026-08-03", "2026-08-05", "2026-08-07", "2026-08-09"]
    min_hrs = [50, 51, 49, 50, 90]
    for d, hr in zip(dates, min_hrs):
        rows.append((f"S_{d}", "A01", dt.datetime.fromisoformat(f"{d}T01:00:00"), hr, 170.0))

    baseline_input = spark.createDataFrame(
        rows, "session_id string, athlete_id string, session_start_time timestamp, session_min_hr int, session_median_cadence double"
    )
    with_date = baseline_input.withColumn("session_local_date", F.to_date(F.col("session_start_time")))

    from pyspark.sql import Window
    days = config.BASELINE_TRAILING_DAYS * 86400
    w = Window.partitionBy("athlete_id").orderBy(F.col("session_local_date").cast("timestamp").cast("long")).rangeBetween(-days, -1)
    out = (
        with_date.withColumn("_n", F.count("session_id").over(w))
        .withColumn("baseline_resting_hr", F.when(F.col("_n") >= config.BASELINE_MIN_SESSIONS, F.avg("session_min_hr").over(w)))
        .withColumn("baseline_typical_cadence", F.when(F.col("_n") >= config.BASELINE_MIN_SESSIONS, F.avg("session_median_cadence").over(w)))
        .withColumn("n_prior_sessions_in_window", F.col("_n"))
    )

    last = out.where(F.col("session_id") == "S_2026-08-09").collect()[0]
    assert last["baseline_resting_hr"] is not None
    assert abs(last["session_min_hr"] - last["baseline_resting_hr"]) > 15  # the threshold gold.discrepancies uses
