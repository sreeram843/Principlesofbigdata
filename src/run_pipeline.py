#!/usr/bin/env python3
"""End-to-end pipeline: load Reddit JSONL -> NLP -> analytics -> outputs."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

from src.config import OUTPUT_DIR, SAMPLE_PATH
from src.pandas_analysis import analyze_pandas


def java_available() -> bool:
    return shutil.which("java") is not None and _java_version_ok()


def _java_version_ok() -> bool:
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
        output = result.stderr + result.stdout
        return "version" in output.lower()
    except OSError:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Reddit disaster analytics pipeline.")
    parser.add_argument(
        "--input",
        type=Path,
        default=SAMPLE_PATH,
        help="Path to Reddit JSON Lines input",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory for CSV/JSON outputs",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "pandas", "spark"],
        default="auto",
        help="Analytics engine (spark requires Java 17+)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(
            f"Input not found: {args.input}\n"
            "Use the bundled sample: data/sample/reddit_sample.jsonl"
        )

    use_spark = args.engine == "spark" or (args.engine == "auto" and java_available())

    if use_spark:
        try:
            from src.spark.analysis import analyze as analyze_spark

            result = analyze_spark(args.input, args.output)
            result["engine"] = "spark"
        except Exception as exc:
            if args.engine == "spark":
                raise
            print(f"Spark unavailable ({exc}); falling back to pandas engine.")
            result = analyze_pandas(args.input, args.output)
    else:
        result = analyze_pandas(args.input, args.output)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
