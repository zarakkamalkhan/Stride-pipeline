#!/usr/bin/env python3
"""
Full pipeline run.

Usage:
    python run_pipeline.py [--drops drop_1,drop_2,drop_3] [--data-dir data] [--output-dir output]

Examples:
    python run_pipeline.py                              # all three drops (full season)
    python run_pipeline.py --drops drop_1                # first drop only
    python run_pipeline.py --drops drop_1,drop_2          # first two drops
"""
import argparse
import sys

sys.path.insert(0, "src")

from stride_pipeline.spark_session import get_spark
from stride_pipeline import pipeline


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--drops", default="drop_1,drop_2,drop_3")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--output-dir", default="output")
    args = ap.parse_args()

    drops = [d.strip() for d in args.drops.split(",") if d.strip()]
    spark = get_spark("stride-pipeline-full-run")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Running pipeline over drops={drops}, data_dir={args.data_dir!r}")
    marts = pipeline.run(spark, args.data_dir, drops, args.output_dir)

    for name, df in marts.items():
        print(f"  {name:32s} {df.count():5d} rows")

    spark.stop()


if __name__ == "__main__":
    main()
