#!/usr/bin/env python3
"""
Part 2: reprocess a single day without touching the others, and prove it.

Usage:
    python run_pipeline.py                       # establish a full baseline first
    python reprocess_day.py --date 2026-08-14

What "prove it" means here: every gold mart that is date-partitioned on
disk (session_quality, hr_zone_minutes, low_completeness_sessions by
session_local_date; weekly_training_load by week_start) is hashed file-by-
file before and after the reprocess. The script then asserts that the
only partition directories whose files changed are the ones that were
SUPPOSED to change -- the target date itself, and (for weekly_training_load
only) the Monday-Sunday week that date falls in. Any other partition
changing at all is treated as a failed proof and the script exits non-zero.
"""
import argparse
import datetime as dt
import hashlib
import os
import sys

sys.path.insert(0, "src")

from stride_pipeline.spark_session import get_spark
from stride_pipeline import pipeline


def snapshot(output_dir: str) -> dict:
    """relative_path -> sha256, for every file under output_dir/parquet/*."""
    root = os.path.join(output_dir, "parquet")
    out = {}
    if not os.path.isdir(root):
        return out
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, root)
            with open(full, "rb") as fh:
                out[rel] = hashlib.sha256(fh.read()).hexdigest()
    return out


def changed_partitions(before: dict, after: dict) -> dict:
    """mart -> set of partition-dir strings (e.g. 'session_local_date=2026-08-14')
    that had any file added, removed, or change hash."""
    all_paths = set(before) | set(after)
    changed = {}
    for p in all_paths:
        if before.get(p) != after.get(p):
            parts = p.split(os.sep)
            mart = parts[0]
            partition_dir = next((seg for seg in parts if "=" in seg), "(unpartitioned)")
            changed.setdefault(mart, set()).add(partition_dir)
    return changed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD, the single local date to reprocess")
    ap.add_argument("--drops", default="drop_1,drop_2,drop_3")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--output-dir", default="output")
    args = ap.parse_args()

    target_date = dt.date.fromisoformat(args.date)
    drops = [d.strip() for d in args.drops.split(",") if d.strip()]

    if not os.path.isdir(os.path.join(args.output_dir, "parquet")):
        print("No existing output/ found. Run `python run_pipeline.py` once first "
              "to establish a baseline, then reprocess a day against it.")
        sys.exit(1)

    print(f"Snapshotting {args.output_dir}/parquet before reprocess...")
    before = snapshot(args.output_dir)

    spark = get_spark("stride-pipeline-reprocess-day")
    spark.sparkContext.setLogLevel("WARN")
    print(f"Reprocessing session_local_date={target_date} only...")
    pipeline.run(spark, args.data_dir, drops, args.output_dir, only_date=target_date)
    spark.stop()

    print(f"Snapshotting {args.output_dir}/parquet after reprocess...")
    after = snapshot(args.output_dir)

    changed = changed_partitions(before, after)

    monday = target_date - dt.timedelta(days=target_date.weekday())
    expected = {
        "session_quality": {f"session_local_date={target_date}"},
        "hr_zone_minutes": {f"session_local_date={target_date}"},
        "low_completeness_sessions": {f"session_local_date={target_date}"},
        "weekly_training_load": {f"week_start={monday}"},
    }

    print("\nPartitions that changed on disk:")
    ok = True
    for mart, dirs in sorted(changed.items()):
        exp = expected.get(mart, set())
        unexpected = dirs - exp
        marker = "OK" if not unexpected else "FAIL"
        if unexpected:
            ok = False
        print(f"  [{marker}] {mart}: {sorted(dirs)}" + (f"  <-- unexpected: {sorted(unexpected)}" if unexpected else ""))

    untouched = [m for m in expected if m not in changed]
    if untouched:
        print(f"\n  (no bytes changed at all for: {untouched} -- fine if {target_date} wasn't in that mart's day-partitioned set already)")

    print("\n" + ("PROOF OK: only the target day's partitions changed." if ok else "PROOF FAILED: an untargeted partition changed -- see FAIL lines above."))
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
