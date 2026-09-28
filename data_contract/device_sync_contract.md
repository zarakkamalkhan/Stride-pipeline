# Data contract: `device_sync` batch feed

**Producer:** StrideBand companion app, on upload.
**Consumer:** this pipeline's bronze ingestion (`bronze.read_device_sync_samples`).
**Status:** proposed -- written against the two real hazards it would have
prevented, not a generic template.

## 1. Envelope (one JSON object per batch, one batch per file/message)

```
{
  "schema_version":   "2",                 // REQUIRED, string, explicit
  "batch_id":         "B0092074e-5",       // REQUIRED, globally unique, producer-generated
  "idempotency_key":  "D013:2026-07-22T06:20:00Z:2026-07-22T06:56:59Z:2160",
                                            // REQUIRED. Deterministic hash/composite of
                                            // (device_id, first_sample_utc, last_sample_utc,
                                            // sample_count). Two uploads of the same
                                            // recording -- retry or not -- MUST produce
                                            // the same idempotency_key.
  "device_id":        "D013",              // REQUIRED
  "firmware":         "2.3.1",             // REQUIRED
  "app_version":       "5.1.3",            // REQUIRED
  "sample_rate_hz":    1.0,                // REQUIRED, float, explicit per batch
  "uploaded_at":       "2026-07-22T02:25:00Z",  // REQUIRED, UTC, RFC3339, trailing 'Z'
  "sample_count":      2160,               // REQUIRED, MUST equal len(samples)
  "samples":           [ ... ]             // REQUIRED, non-empty
}
```

## 2. Sample object (fixed shape, all fields required, no aliases)

```
{
  "t":        "2026-07-22T06:20:00Z",  // REQUIRED. UTC. RFC3339. Trailing 'Z' mandatory.
  "hr_bpm":   55,                       // REQUIRED, int, 20-240 inclusive, or null with hr_status set
  "hr_status":"ok",                     // REQUIRED, enum: ok | no_contact | error
  "cadence_spm": 116,                   // REQUIRED, int (not string), steps/min, >= 0
  "gps": {                              // REQUIRED object, MAY be null if no fix
     "lat": 33.6796, "lon": 73.04158, "hdop": 1.6
  }
}
```

## 3. Hard rules

- **R1 -- timestamps are UTC on the wire, always, no exceptions.** `t` and
  `uploaded_at` are RFC3339 with an explicit `Z` or numeric offset. A
  producer MUST NOT emit a bare local time with no offset.
- **R2 -- one canonical field name per concept, versioned, not aliased.**
  If the payload shape changes (new fields, renamed fields, restructured
  nesting), `schema_version` increments and the OLD field names are never
  silently replaced under the same schema_version. Consumers key their
  parsing logic on `schema_version`, not on inferring the shape from
  `app_version`.
- **R3 -- `idempotency_key` is deterministic and content-based**, not
  `batch_id` (which the producer is free to regenerate on every retry).
  Consumers dedupe on `idempotency_key`.
- **R4 -- `sample_rate_hz` travels with the data**, not with a
  firmware-version lookup table the consumer has to maintain and keep in
  sync with release notes.
- **R5 -- `hr_bpm` is null only together with a non-"ok" `hr_status`.** No
  silent zeros, no missing key.

## 4. What this contract would have prevented, of the hazards we actually found

1. **Hazard #11 (the payload schema change, `hr`/`lat`/`lon` -> `heart_rate`/
   `gps.{lat,lon}`, affecting half the season) -- prevented by R2.** With an
   explicit `schema_version` field, the consumer branches on a value the
   producer is contractually required to bump, instead of the pipeline
   discovering the new shape by a `KeyError` on `sample['hr']` (which is
   literally how we found it -- see README > AI usage). A contract doesn't
   stop the vendor from changing their payload; it stops that change from
   being invisible to consumers.
2. **Hazard #2 (D014 silently recording local time at UTC+3 instead of
   UTC+5 for the entire season) -- prevented by R1.** If the wire format
   never allows a bare, offset-less local timestamp, there is no "local
   time, trust the device's clock" field for a misconfigured device to
   get wrong in a way that only shows up when cross-referenced against a
   second system. The device can still have its SYSTEM clock wrong (see
   below), but it can't emit ambiguous timezone data.

Also meaningfully reduced by this contract, though not one of the two
required:
- **Hazard #7 (retry duplicate under a new batch_id) -- reduced by R3.**
  `idempotency_key` makes dedup a direct equality check instead of the
  window-matching heuristic `silver.dedupe_device_sync_batches` currently
  has to do.
- **Hazard #3 (firmware-dependent sample rate) -- reduced by R4.** Removes
  the need for `_sample_interval_seconds()` to hardcode "2.4.0 -> 0.5s" at
  all; the pipeline just reads what the batch declares.

## 5. What this contract explicitly CANNOT prevent

- **A device's system clock being wrong**, even in UTC. R1 forces the
  *format* to be unambiguous UTC; it does nothing if the watch's internal
  clock itself has drifted or was never set correctly. That failure mode
  would still need a plausibility check downstream (e.g. "device_sync
  window is nowhere near any vendor_api session for this device" -- which
  is exactly the check `session_quality`'s `has_athlete`/completeness
  logic already does as a safety net).
- **Anything about who was wearing the device.** The device/athlete/squad
  crosswalk (hazards #1 and #10) is a roster and equipment-handout
  process fact, not a device_sync payload fact. No schema on this feed
  can encode "Hamza handed D013 to Mehak on this date" -- that has to
  come from the roster/ops system having its own effective-dated record
  of assignments, which is a separate, upstream contract problem (on
  `roster`, not `device_sync`).
- **The vendor portal changing its mind after the fact** (hazard #5). That
  data lives entirely on the vendor's side, fetched via `vendor_api`, a
  completely different feed this contract has no authority over.
- **Coach-app free-text name entry** (hazard #8) or **late data entry**
  (hazard #9) -- again, a different source (`coach_app`), and a UX/process
  problem (pick-from-roster instead of free text) more than a schema one.
- **Sensor gaps from a real disconnect** (e.g. `S0b21423f`'s ~4 minute
  Bluetooth gap). A contract can require the device to report *that it
  lost contact* (which is what `hr_status: no_contact` is for above,
  and which the current 2.3.1 payload has no way to express at all) but
  it cannot stop the disconnect from happening. This is the difference
  between preventing a hazard and making a hazard legible -- this
  contract mostly does the latter, which is the realistic ceiling for a
  schema-level fix.
