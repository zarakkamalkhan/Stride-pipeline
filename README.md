# Stride Performance Lab -- data pipeline

Turns three loosely-related raw feeds (a watch's raw samples, a vendor
portal's session summaries, a coach's free-text notes, and two roster
snapshots) into marts a sports-science lead can trust -- and says exactly
where and why they can't be trusted 100%.

Built in PySpark. See `flow_map.md` for the source-to-mart diagram with
every hazard marked at the point it's actually handled, and
`data_contract/device_sync_contract.md` for the proposed fix upstream.

---

## How to run

Requires Python 3.10+, Java 17+ (Spark needs a JVM), and:

```bash
pip install pyspark==3.5.3 pytest
```

```bash
# Full season, all three drops:
python run_pipeline.py

# Only the first drop (demonstrates incremental loading -- see below):
python run_pipeline.py --drops drop_1

# First two drops:
python run_pipeline.py --drops drop_1,drop_2

# Part 2: reprocess one day in place, against an existing full run, with proof:
python run_pipeline.py                      # establish the baseline first
python reprocess_day.py --date 2026-08-14

# Tests:
python -m pytest tests/ -v
```

Output lands in `output/parquet/<mart>/` (partitioned where noted below)
and `output/csv/<mart>/<mart>.csv` (single file, deterministically
ordered, for reading without Spark). `output/csv/session_quality/` and
`flow_map.md` are the two named deliverables from the brief; every other
mart is alongside them.

### Idempotency and incremental loading -- how it's actually structured, and how I checked it

I did not build a stateful "merge new rows into existing state" pipeline.
At this data volume (a few hundred thousand rows across the whole
season), bronze and silver are a **stateless recompute**: every run reads
whatever files currently exist under `data/device_sync/<drops>/` from
scratch. That makes two of the three claims almost free:

- **Run twice on the same drops -> identical output.** Trivially true --
  it's a pure function of the same input files. I still verified it
  rather than asserted it: I hashed every output CSV's *content*
  (ignoring Spark's randomly-named part file) across two consecutive full
  runs and diffed the hash lists -- identical. (Spark's part-file name
  itself has a random UUID in it every run; the pipeline renames the
  single output part file to a fixed `<mart>.csv` specifically so a plain
  `diff` works without that extra step in the future.)

- **Drops loaded one at a time, in order -> still correct.** I ran the
  pipeline three times, with `--drops drop_1`, then `drop_1,drop_2`, then
  all three, against a clean output dir each time, and looked at what
  changed:

  | drops loaded | session_quality rows | low_completeness_sessions | discrepancies |
  |---|---|---|---|
  | drop_1 | 158 | 106 | 107 |
  | drop_1, drop_2 | 158 | 48 | 53 |
  | all three | 158 | 2 | 7 |

  `session_quality` is always 158 because it's grained on `vendor_api`
  sessions, a separate source that's always complete. What changes is
  *device_sync coverage* -- with only drop_1 loaded, most sessions
  correctly look incomplete (their raw samples haven't arrived yet), and
  `low_completeness_sessions` / `discrepancies` shrink monotonically as
  more drops land, converging to the same 2 and 7 rows as the full run.
  This is the *correct* behaviour for the physical situation described in
  the brief (the watch only uploads "whenever the phone has signal"), not
  a bug to paper over -- a session with no raw samples yet really is 0%
  complete, not unknown.

For **Part 2** (reprocess a single day without touching the others), the
gold tables that are naturally date-grained (`session_quality`,
`hr_zone_minutes`, `low_completeness_sessions` by `session_local_date`;
`weekly_training_load` by `week_start`) are written with Spark's dynamic
partition overwrite mode. `reprocess_day.py` filters the whole pipeline's
output down to one date, writes only that partition, and then **proves**
isolation by SHA-256-hashing every file under `output/parquet/` before
and after and asserting the only partition directories that changed are
the target date (and, for the weekly mart, that date's Monday-Sunday
week). Sample run:

```
Partitions that changed on disk:
  [OK] hr_zone_minutes: ['session_local_date=2026-08-14']
  [OK] session_quality: ['session_local_date=2026-08-14']
  [OK] weekly_training_load: ['week_start=2026-08-10']
PROOF OK: only the target day's partitions changed.
```

`squad_snapshot`, `athlete_baseline` and `discrepancies` are **not**
date-partitioned -- squad_snapshot is a single as-of query with no date
grain to partition on, and athlete_baseline / discrepancies genuinely
depend on cross-day history (a rolling window, a revision log), so
isolating "one day" for them would mean silently ignoring data they're
supposed to look at. Said plainly rather than claimed away.

---

## Grain and keys

| Table | Grain (one row = ) | Key |
|---|---|---|
| `session_quality` | one vendor_api session, after dedup | `session_id` |
| `hr_zone_minutes` | one vendor_api session | `session_id` |
| `low_completeness_sessions` | one session below the threshold | `session_id` |
| `weekly_training_load` | one athlete, one Mon-Sun week (athlete local time) | `(athlete_id, week_start)` |
| `squad_snapshot_2026_08_14` | one athlete, as of 2026-08-14 | `athlete_id` |
| `athlete_baseline` | one athlete's rolling baseline **as of immediately before** a given session (current session excluded from its own baseline) | `(athlete_id, session_id)` |
| `discrepancies` | one finding | `(category, session_id)` -- `session_id` may be null for a batch- or device-level finding |
| crosswalk (internal) | one athlete, one effective-dated stretch of (squad, device) | `(athlete_id, effective_start)` |

**Why session_id, not device_id or a synthetic key, anchors everything:**
`device_id` is not a stable proxy for "a training session" (hazard #1) and
neither the watch's own batch boundaries nor the coach's notes are a
reliable session boundary -- one batch can be split across two device_sync
uploads and one session can span >1 batch (`S0b21423f` does, in this
data). The vendor's `session_id`, once deduped (hazard #5, #6), is the one
identifier in this dataset that both marks a session's true start/end
*and* is stable across every fetch, page, and firmware version -- so
everything else (raw samples, HR zones, completeness) is built by
attaching data **to** a session_id, never the other way around.

---

## What I found

Ordered roughly by how much they'd silently break if missed, not by
discovery order.

1. **`device_id` is not a stable athlete key.** `D013` was Hamza Tariq's
   device through 2026-08-10 (last matching session), then sat with no
   evidence of an owner for two days, then was picked up by a new
   athlete, Mehak Noor, from 2026-08-14. Hamza himself moved to a new
   device, `D017`, starting 2026-08-12. Neither roster snapshot
   (2026-07-01, 2026-09-01) records *when* this happened -- I reconstructed
   the exact boundary from the one place it's independently verifiable:
   the brief's own control figures. The control table says Hamza=4
   sessions, Mehak=2 sessions for the week of Aug 10-16; the raw session
   counts per device that week are D013=3, D017=3. The only split of
   those six sessions across two people that produces 4-and-2 (not 3-
   and-3) is: Hamza keeps his Aug 10 session on D013, then all three of
   his Aug 12/14/15 sessions are on D017; Mehak's two sessions (Aug 14,
   15) are the *only* other D013 sessions that week. That fixes the
   handover to somewhere in Aug 11-13, which matches (see #10) Bilal's
   independent squad-change date almost exactly -- consistent with a
   real equipment/roster reshuffle around that weekend. **Fix:** an
   effective-dated crosswalk (`crosswalk.py`), not a join on either
   static roster file.

2. **One device has been in the wrong timezone all season.** `D014`
   (Zainab Mirza)'s raw `t` field reconciles against its own vendor_api
   sessions at a **UTC+3** offset -- not the UTC+5 (Asia/Karachi, no DST)
   every other device uses. This isn't one bad session: I checked all 30
   of her sessions, and 30/30 match at +3h while 0/30 match at +5h. Left
   uncorrected, every one of her sessions would either fail to join to
   its vendor session at all, or (worse, if matched loosely) land on the
   wrong calendar day/week. **Fix:** `silver.apply_timezone_correction`
   applies a device-level offset override, and separately reconstructs
   Pakistan wall-clock time (`t_athlete_local`) for week/day bucketing --
   these are NOT the same correction (see `config.py` for why).

3. **Sampling rate depends on firmware, not a fixed 1 Hz.** Firmware
   2.4.0 (staged rollout from 2026-08-01) samples at 2 Hz (0.5s spacing);
   2.3.1 is 1 Hz. Affects ~31 batches. Row-count-as-seconds silently
   halves the apparent duration of every post-2.4.0 batch. **Fix:**
   coverage and zone-minute math use `interval_s` (0.5 or 1.0, keyed off
   `firmware`), never `len(samples)`.

4. **An "unsupported" firmware/app combo shows up in real data anyway.**
   Release notes say firmware 2.4.0 needs companion app >=5.2.0 to sync.
   Four batches (2 on D011, 2 on D014) have firmware 2.4.0 with app
   5.1.3, and still synced at 2 Hz. Nothing about the data itself looks
   broken -- but it's a real device/app state the release notes say
   shouldn't exist, so it's surfaced as a low-severity discrepancy rather
   than silently accepted or silently dropped.

5. **The vendor portal is not an immutable log.** `sessions_page_4.json`
   and `sessions_page_4_retry.json`, fetched ~6 hours apart on the same
   day, disagree on one session (`Se06bc0e7`): `duration_s` went from
   2040 to 2400 and `end_time` moved 6 minutes later. **Fix:** dedup by
   `session_id` keeps the **latest `fetched_at`**, not the first -- but
   the fact that it changed at all is *also* kept, as a
   `vendor_session_revised` discrepancy (`silver.session_revision_log`),
   because "the portal quietly revised a number" is information a
   sports-science lead should see, not just something to resolve and
   forget. Notably, the *raw device_sync samples for that session only
   cover 2040s* -- i.e. exactly the original, pre-revision duration. That
   makes the retry's correction itself look questionable, not just the
   original figure; flagged, not resolved, in `discrepancies`.

6. **Pagination overlap creates literal duplicate session rows.** 5
   `session_id`s appear on two adjacent vendor_api pages with identical
   content (classic "results shifted while paginating"). Same dedup as
   #5 handles this for free.

7. **A client-side retry produced a byte-identical device_sync batch
   under a brand-new `batch_id`.** One D011 batch was uploaded, then
   re-uploaded 38 seconds later with different `batch_id` but
   byte-identical samples. `batch_id` cannot be the dedup key -- the
   pipeline dedupes device_sync batches on their actual recording window
   `(device_id, first_t, last_t, n_samples)` instead
   (`silver.dedupe_device_sync_batches`).

8. **Coach names are free text.** Mixed case, doubled whitespace, and
   `First L.` initials (`Sara K.`, `Bilal A.`, ...). Normalized and
   matched against every roster spelling variant of every athlete's name
   (`silver.build_name_lookup`). Verified (as a test, not just visually)
   that no two athletes on the current 6-person roster collide under
   normalization -- see Tests.

9. **Coach `entered_at` is not a time signal.** One entry logged 2026-07-13
   was entered 2026-07-17 -- 4 days later. `session_date` (a plain date,
   coach-recalled) is used for matching; `entered_at` is kept for
   provenance only and never used for anything session-timing related.

10. **Squad also changes mid-season, with the same "no clean snapshot at
    the transition" problem as #1.** Bilal Ahmed's coach-app entries say
    `Endurance` through 2026-08-10 and `Speed` from 2026-08-12 onward,
    cleanly (no flip-flopping) -- so unlike the device handover, this one
    *is* directly evidenced day-by-day in `coach_app`, not reconstructed
    from control-figure arithmetic. Handled the same way in the
    crosswalk: effective-dated, not a single static squad column.

11. **The single biggest hazard: two incompatible device_sync payload
    schemas, and it's exactly half the season.** Companion app 5.1.3
    writes `{hr, cadence: "<string>", lat, lon}`. App 5.2.0 (released
    2026-08-05, per firmware notes: "richer GPS metadata") writes
    `{heart_rate, cadence: <int>, gps: {lat, lon, hdop}}`. Both exist
    side by side for the rest of the season (some devices update, some
    don't, independent of firmware version). Checked exhaustively: it's
    a clean split by `app_version` alone (76+4 batches old schema on
    5.1.3, 53+27 new schema on 5.2.0), affecting exactly 80 of 160
    batches. A reader that only knows the old field names silently gets
    **zero HR and GPS data for half the season**, with no error -- unless
    it happens to hard-fail on a missing key, which is how this was
    actually found (see AI usage below). **Fix:**
    `bronze.read_device_sync_samples` lets Spark's multi-file schema
    merge surface both sets of columns, then coalesces `hr =
    coalesce(hr, heart_rate)` etc., tagging each row with which schema
    it came from.

---

## Reconciliation

Control week: Mon 2026-08-10 -- Sun 2026-08-16.

**Session counts per athlete** -- exact match, using the effective-dated
crosswalk from hazard #1:

| Athlete | Control | Pipeline |
|---|---|---|
| Ali Raza | 4 | 4 |
| Sara Khan | 5 | 5 |
| Hamza Tariq | 4 | 4 |
| Zainab Mirza | 4 | 4 |
| Bilal Ahmed | 4 | 4 |
| Mehak Noor | 2 | 2 |

**Total logged minutes, Endurance squad:** control = 689. Pipeline =
**696** (7 min / ~1% over). I did not force this to match. What I checked
instead: for every one of the 13 Endurance-squad sessions in that week,
`device_coverage_s` (actual raw samples) equals the vendor's own
`duration_s` to the second -- so the gap isn't a completeness/coverage
bug in this pipeline. The most likely explanation is that the control
figure and my `vendor_api` fetch simply weren't taken at the same moment
-- we have direct proof the portal *does* revise session durations after
the fact (hazard #5, `Se06bc0e7` gained 360s between two fetches 6 hours
apart, in the very same page). A handful of minutes' drift across 13
sessions is consistent with one or two of them having been revised
between whenever the control figure was pulled and whenever my
`vendor_api` snapshot was taken. I'd resolve this for real by asking for
the exact timestamp the control figures were pulled and diffing against
mine -- flagged, not silently swallowed.

One more wrinkle worth naming: Bilal Ahmed's squad changes *inside* this
control week (hazard #10, Endurance through Aug 10, Speed from Aug 12).
The 696-minute figure above counts each of his sessions under whatever
squad he was actually in on that session's own date -- not "whatever
squad he's in by the end of the week." If you instead answer "Endurance
total" using only athletes whose squad is Endurance *for the whole week*,
you'd exclude Bilal's Aug 10 session (45 min) too, and land further from
689, not closer -- which is itself a small piece of evidence that
per-session (not per-week) squad attribution is the right call.

---

## Definitions

- **Session.** A `vendor_api` `session_id`, after dedup (latest
  `fetched_at` wins; see hazard #5/#6). This is the unit everything else
  attaches to -- see "Grain and keys" for why.
- **Week.** Monday-Sunday, in **athlete local time** (Asia/Karachi,
  UTC+5, fixed, no DST) -- computed from each session's true UTC instant,
  never from a device's raw (possibly wrong, see hazard #2) local clock
  reading.
- **HR zones.** Standard 5-zone model on `max_hr` from the roster, as
  specified in the brief: Z1 <60%, Z2 60-70%, Z3 70-80%, Z4 80-90%, Z5
  >=90%. Implemented as half-open bounds `[lo, hi)` except Z5, open-ended
  at the top (tested at the boundary values -- see Tests).
- **Completeness** = (seconds of the vendor session window actually
  backed by a device_sync sample, samples from multiple batches merged
  and clipped to the window) / (vendor `duration_s`). Not sample count --
  sample count is rate-dependent (hazard #3).
- **Completeness threshold: 95%.** Across all 158 sessions, completeness
  is exactly 100% for 156 of them (median = mode = 1.0). Only two fall
  below 95%: one a ~4-minute Bluetooth-reconnect-shaped gap
  (`S0b21423f`, 90.2%), one the disputed-duration session from hazard #5
  (`Se06bc0e7`, 85.0% against the *revised* duration, 100% against the
  *original*). 95% sits just above the natural break between "the norm"
  and "a real, multi-minute gap" -- and at 1-2Hz sampling, a gap that
  size concentrated at session start/end (where HR is least steady) is
  exactly the situation the brief's opening line warns about. Below-
  threshold sessions are **flagged, not dropped** -- zone minutes are
  still computed off whatever samples exist, because a partial reading
  beats no reading as long as the mart says so (`below_completeness_
  threshold` is its own column). A separate, lower **floor of 50%**
  exists in `config.py` for "too sparse to trust at all" -- no session
  currently trips it, kept as an explicit design decision rather than
  inferred from the 95% line.
- **Training load** = zone-weighted minutes, `sum(minutes_in_zone[z] *
  weight[z])` for weights `{Z1:1, Z2:2, Z3:3, Z4:4, Z5:5}`
  (`config.ZONE_LOAD_WEIGHTS`). This is a simplified TRIMP: same idea as
  Banister's training impulse (weight minutes by intensity zone and
  sum), without needing `%HRR`-anchored math the brief doesn't ask the
  zones to use. Chosen over a more "proper" HRR-based TRIMP specifically
  because it's fully reconstructable from `hr_zone_minutes` alone and
  easy to defend/re-derive live, not because it's the only defensible
  choice -- I'd happily swap weights or formula given a real coaching
  rationale from the sports-science lead.

---

## Tests

`python -m pytest tests/ -v` -- 9 tests, all against small synthetic
inputs (fast, and each fails for exactly one reason). Two I'd point to
first:

1. **`test_both_payload_schemas_produce_usable_hr_and_gps`** (hazard
   #11, the big one). Builds one batch in each real schema and asserts
   both produce non-null `hr`/`lat`/`lon`. This is the test that would
   have caught the mistake described in AI usage below on the first
   run, instead of me noticing it by accident while eyeballing a missing-
   field count.
2. **`test_device_handover_matches_control_week`** (hazard #1). Asserts
   the crosswalk attributes `D013` to Hamza on Aug 10, `D017` to Hamza on
   Aug 12, `D013` to Mehak on Aug 14, and *no one* to `D013` on Aug 12 --
   i.e. it encodes the exact reasoning from "What I found" #1 as an
   executable check, not just prose.

The other seven: retry-duplicate batch collapse (#7), the D014 timezone
override applying to exactly one device and producing two different
corrected timestamps for two different purposes (#2), vendor session
dedup keeping the latest fetch (#5), HR-zone boundary correctness at the
exact 60/70/80/90% cut points, a collision check confirming no two
athletes' name variants resolve to the same `athlete_id` on the current
roster (#8), and two added after a self-review pass (see the gaps note
at the end of this section): coverage/zone-minute computation correctly
de-duplicating two batches that overlap in true wall-clock time instead
of double-counting them, and the `athlete_baseline` anomaly check
actually firing on a constructed outlier rather than only ever having
been observed to find nothing.

**A gap I found on review, after first calling this "done":** the
original coverage/zone-minute aggregation summed `interval_s` across
every sample matched to a session with no de-duplication. That's correct
for every session in the actual three drops -- the one real multi-batch
session, `S0b21423f`, has two batches that don't overlap in time, checked
by hand -- but nothing in the data *guarantees* that, and the live
follow-up session hands over an unseen fourth drop. If a future batch
overlapped another one in true wall-clock time, naive summation would
have pushed `completeness_pct` above 100% instead of flagging anything.
Fixed by collapsing to one row per `(session_id, t_utc)` before summing
(`session_quality.coverage_and_zone_minutes`), and covered by
`test_coverage_deduplicates_overlapping_batches`. Re-ran the full
pipeline afterward -- identical output on the real data, as expected,
since no genuine overlap exists in these three drops.

---

## AI usage

Used an AI assistant (Claude) for essentially the whole build: initial
exploration of the raw files, the reconciliation logic, the PySpark
pipeline, this README, the tests, and the data contract.

**Two concrete mistakes it made and I had to catch, with how I noticed:**

1. **Assumed a single, uniform local-timezone offset (+5h) for every
   device's `t` field**, and initially wrote that off as "probably a
   one-session firmware timestamp bug" for the single batch it happened
   to check first (`D014`, 2026-07-06) -- which matched the firmware
   notes' own description of a stale-date bug closely enough to be a
   believable, comfortable explanation. That explanation was wrong: it
   only survived because the very first offset check only tried +5h
   against a handful of batches. Checking *all 30* of D014's batches at
   +5h found **zero** matches, not "mostly matches with one outlier" --
   which is a completely different, and much bigger, finding (a
   device-wide misconfiguration, not a one-off event). The fix was to
   stop trusting the plausible-sounding explanation and brute-force
   every device's batches against a range of offsets before writing any
   conclusion down.
2. **Read `sample['hr']` directly** while first inspecting the raw JSON,
   assuming one payload shape for the whole dataset -- and got a hard
   `KeyError` on the very first `app_version=5.2.0` batch it happened to
   touch. That crash is actually what surfaced hazard #11 at all; a
   *softer* version of the same mistake (`sample.get('hr')`, defaulting
   to `None` instead of raising) would have silently zeroed out HR data
   for half the season with no error to notice at all. I only trusted
   the fix once I'd aggregated a missing-field count across all 160
   batches and seen it split cleanly 80/80 by `app_version` -- a
   one-batch spot check wouldn't have shown that it was a clean,
   universal split rather than a handful of corrupt files.

The pattern in both: the AI's first hypothesis was locally plausible
(matched a real firmware-notes line; matched "just missing a field") and
wrong at the *scope* it generalized to. Both were only caught by refusing
to stop at "one example looks explained" and instead running the check
against every batch/device before accepting the explanation.

---

## What I would do with another day

- **Pin down the reconciliation gap for real.** Ask when exactly the
  control figures were pulled, fetch `vendor_api` again at that instant,
  and see whether the 7-minute Endurance gap disappears -- I'm fairly
  confident it's portal-side revision drift (hazard #5 shows this
  mechanism is real) but "fairly confident" isn't "confirmed."
- **Distance/pace marts.** `distance_m` and the GPS trail
  (`lat`/`lon`/`hdop`) are fully parsed and sitting in the silver layer
  unused -- pace zones and route data would be a natural next mart, and
  `hdop` (GPS quality, new-schema only) would need its own completeness-
  style thresholding first.
- **A real incremental engine, not stateless recompute.** The current
  design recomputes bronze/silver from every file on disk each run,
  which is fine at this volume but would not survive a season with
  thousands of athletes. I'd move to Delta Lake / Iceberg `MERGE INTO`
  for device_sync and vendor_sessions specifically (both already have a
  clean natural key -- the idempotency_key from the proposed data
  contract, and `session_id`, respectively) so ingestion becomes true
  append/merge instead of full-file rescans.
- **Push the effective-dated crosswalk logic upstream.** Hazards #1 and
  #10 are really an ops-process gap (no timestamped log of device
  handouts or squad moves), not a data-engineering one. I'd rather the
  programme log equipment handouts with a date than have a pipeline
  reverse-engineer the date from session-count arithmetic, however well
  it worked out this time.
- **`hr_status`-style gap semantics**, per the proposed data contract --
  right now a coverage gap and "the watch recorded nothing because it
  lost contact" are indistinguishable from "no data because it hasn't
  synced yet." They should probably be different completeness
  categories, not one.
- **`athlete_baseline` found zero anomalies on the real season.** That's
  a genuinely clean result, not a bug -- every device/athlete attribution
  already reconciles against the control figures -- and it's now proven
  to actually fire on a constructed outlier (`test_baseline_anomaly_
  check_fires_on_genuine_outlier`), not just observed to find nothing.
  What I'd still want with more time: a *second* threshold check tuned
  for gradual drift (the current one only catches a single session that
  jumps >15bpm/>20spm past the trailing average; a slow week-over-week
  drift in resting HR -- a real fitness or fatigue signal a coach would
  want -- would currently slip past it entirely).
- **`sample_count_mismatch` (declared vs. actual samples per batch) is a
  standing check now, not just a one-off hand check** -- added after
  noticing, on review, that I'd verified it manually across all 160
  batches but never encoded it into the pipeline itself, which meant it
  gave zero protection against the unseen fourth drop from the live
  session. Fixed; still worth extending with a bad-batch synthetic test
  the same way the schema-union and coverage tests work, rather than
  relying on it never having fired yet.
