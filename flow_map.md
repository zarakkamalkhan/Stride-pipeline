# Flow map: source -> mart, with hazards marked where they're actually handled

Hazard numbers match README > What I found. Each hazard is placed at the
exact node where the pipeline deals with it, not where it was first
noticed -- e.g. #11 is *found* by looking at raw samples, but it's *fixed*
in bronze parsing, so that's where it's marked.

```mermaid
flowchart TD
    subgraph SRC["Raw sources"]
        DS["device_sync/drop_1..3\n(160 JSON batches)"]
        VA["vendor_api/sessions_page_1..4\n+ page_4_retry"]
        RO["roster_2026-07-01.csv\nroster_2026-09-01.csv"]
        CA["coach_app/session_labels.csv"]
        FW["firmware/release_notes.md\n(reference only)"]
    end

    subgraph BRONZE["Bronze -- parse, 1:1 with source"]
        B_DS["bronze.read_device_sync_samples\n\u26A0\uFE0F #11 two payload schemas\n(hr/lat/lon vs heart_rate/gps.*)\ncoalesced here"]
        B_VA["bronze.read_vendor_sessions_raw\n(NOT deduped yet --\npage_4 + page_4_retry\nboth kept)"]
        B_RO["bronze.read_roster_raw"]
        B_CA["bronze.read_coach_labels_raw"]
    end

    subgraph SILVER["Silver -- reconcile"]
        S_DEDUP["silver.dedupe_device_sync_batches\n\u26A0\uFE0F #7 content-identical retry\nunder a new batch_id"]
        S_TZ["silver.apply_timezone_correction\n\u26A0\uFE0F #2 D014 is UTC+3, everyone\nelse is UTC+5, all season"]
        S_SESS["silver.dedupe_vendor_sessions\n\u26A0\uFE0F #6 pagination overlap dupes\n\u26A0\uFE0F #5 vendor revised Se06bc0e7's\nduration between fetches\n(latest fetched_at wins)"]
        S_NAME["silver.clean_coach_labels\n\u26A0\uFE0F #8 free-text names, case/\ninitials, normalized + matched\n\u26A0\uFE0F #9 entered_at can lag\nsession_date by days (unused\nfor any time logic)"]
        S_CW["crosswalk.build_crosswalk\n\u26A0\uFE0F #1 Hamza D013\u2192D017 handover\n\u26A0\uFE0F #10 Bilal squad change\nboth effective-dated from\nin-season evidence, not\neither roster snapshot alone"]
    end

    subgraph JOIN["session_quality construction"]
        SZ["session_quality.build_sample_zone\nrange-join samples \u2194 sessions \u2194 crosswalk\n\u26A0\uFE0F #3 sample interval is firmware-\ndependent (2.4.0=0.5s, else 1s) --\nused for coverage & zone minutes,\nnot row count"]
        SQ["session_quality.build_session_quality\none row per vendor session:\ncompleteness_pct, Z1-Z5 minutes,\ntraining_load, has_athlete"]
    end

    subgraph GOLD["Gold marts"]
        G_WTL["weekly_training_load\n(athlete_id x week_start)"]
        G_HZM["hr_zone_minutes\n(1 row / session)"]
        G_LOW["low_completeness_sessions\n(threshold: 95%, see config.py)"]
        G_SNAP["squad_snapshot_2026_08_14\n(as-of query on crosswalk)"]
        G_BASE["athlete_baseline\nrolling resting HR / cadence\n(Part 2)"]
        G_DISC["discrepancies\n\u26A0\uFE0F #4 unsupported firmware/app\ncombo (2.4.0 + app 5.1.3) surfaces\nhere, pulled straight from silver\nsamples\nalso folds in: #5 revisions,\nlow-completeness sessions,\nbaseline anomalies (Part 2),\nunattributed-device rows"]
    end

    DS --> B_DS --> S_DEDUP --> S_TZ --> SZ
    VA --> B_VA --> S_SESS --> SZ
    RO --> B_RO --> S_CW --> SZ
    CA --> B_CA --> S_NAME
    FW -. "informs constants in\nconfig.py + session_quality.py\n(sample rate, app compat)" .-> S_TZ
    FW -. .-> SZ

    S_CW --> SQ
    S_SESS --> SQ
    SZ --> SQ

    SQ --> G_WTL
    SQ --> G_HZM
    SQ --> G_LOW
    S_CW --> G_SNAP
    SQ --> G_BASE
    SZ --> G_BASE
    S_SESS --> G_DISC
    SQ --> G_DISC
    G_BASE --> G_DISC
    S_DEDUP -. "unsupported combo check\nreads silver samples directly" .-> G_DISC
```

Note: `coach_app` (`S_NAME`) is matched to athletes and kept as reference
context (see README > Grain and keys) but does not feed any of the five
required marts directly -- none of the brief's questions ask for RPE or
session labels, and coach `session_date`/`entered_at` are never used as a
time source for anything sensor-derived (hazard #9). It's included in the
flow map because it's a real source with a real hazard in it, not because
a mart consumes it.
