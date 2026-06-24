#!/usr/bin/env python3
"""Download MASH Reddit annotations and hydrate post text via PullPush (no Reddit API keys)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import DATA_DIR

MASH_REPO = "YRC10/MASH"
ANNOTATIONS_FILE = "reddit_anno_publish.csv"
PULLPUSH_URL = "https://api.pullpush.io/reddit/search/submission/"
DEFAULT_OUTPUT = DATA_DIR / "sample" / "mash_reddit.jsonl"

HUMANITARIAN_COLS = [
    "Casualty",
    "Evacuation",
    "Damage",
    "Advice",
    "Request",
    "Assistance",
    "Recovery",
]


def download_annotations() -> pd.DataFrame:
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(MASH_REPO, ANNOTATIONS_FILE, repo_type="dataset")
    return pd.read_csv(path)


def _humanitarian_labels(row: pd.Series) -> list[str]:
    return [col for col in HUMANITARIAN_COLS if bool(row.get(col))]


def _normalize_submission(submission: dict, annotations: dict) -> dict:
    title = submission.get("title") or ""
    selftext = submission.get("selftext") or ""
    text = f"{title}\n{selftext}".strip() if selftext else title.strip()
    humanitarian = _humanitarian_labels(pd.Series(annotations))

    return {
        "id": submission.get("id") or annotations.get("id"),
        "source": "reddit",
        "platform": "reddit",
        "dataset": "MASH",
        "subreddit": submission.get("subreddit"),
        "author": submission.get("author") or "[deleted]",
        "title": title,
        "selftext": selftext,
        "text": text,
        "score": int(submission.get("score") or 0),
        "num_comments": int(submission.get("num_comments") or 0),
        "created_utc": int(submission.get("created_utc") or 0),
        "permalink": submission.get("permalink") or submission.get("full_link"),
        "is_self": bool(submission.get("is_self", True)),
        "humanitarian_labels": humanitarian,
        "information_integrity": annotations.get("Information_Integrity"),
        "linguistic_bias": bool(annotations.get("Linguistic_Bias")),
        "political_bias": bool(annotations.get("Political_Bias")),
        "gender_bias": bool(annotations.get("Gender_Bias")),
        "hate_speech": bool(annotations.get("Hate_Speech")),
        "racial_bias": bool(annotations.get("Racial_Bias")),
        **{col: bool(annotations.get(col)) for col in HUMANITARIAN_COLS},
    }


def fetch_submissions(post_ids: list[str], session: requests.Session) -> dict[str, dict]:
    """Fetch submissions from PullPush; returns mapping id -> submission dict."""
    if not post_ids:
        return {}

    response = session.get(
        PULLPUSH_URL,
        params={"ids": ",".join(post_ids)},
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(f"PullPush error: {payload['error']}")

    return {item["id"]: item for item in payload.get("data", []) if item.get("id")}


def build_mash_dataset(
    output_path: Path,
    limit: int | None = None,
    batch_size: int = 100,
    sleep_seconds: float = 0.5,
    resume: bool = False,
) -> dict:
    annotations_df = download_annotations()
    if limit:
        annotations_df = annotations_df.head(limit)

    annotation_map = annotations_df.set_index("id").to_dict(orient="index")
    post_ids = list(annotation_map.keys())

    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_ids: set[str] = set()
    if resume and output_path.exists():
        with output_path.open(encoding="utf-8") as infile:
            for line in infile:
                line = line.strip()
                if line:
                    existing_ids.add(json.loads(line)["id"])
        post_ids = [pid for pid in post_ids if pid not in existing_ids]
        print(f"Resuming: {len(existing_ids)} already saved, {len(post_ids)} remaining")

    session = requests.Session()
    session.headers.update({"User-Agent": "principles-of-bigdata-mash/1.0"})

    written = len(existing_ids)
    missing = 0
    mode = "a" if resume and output_path.exists() else "w"

    with output_path.open(mode, encoding="utf-8") as outfile:
        for start in range(0, len(post_ids), batch_size):
            batch_ids = post_ids[start : start + batch_size]
            try:
                submissions = fetch_submissions(batch_ids, session)
            except requests.RequestException as exc:
                print(f"Batch failed at {start}: {exc}; retrying after pause...")
                time.sleep(5)
                submissions = fetch_submissions(batch_ids, session)

            for post_id in batch_ids:
                submission = submissions.get(post_id)
                if not submission:
                    missing += 1
                    continue
                record = _normalize_submission(submission, annotation_map[post_id])
                if not record["text"]:
                    missing += 1
                    continue
                outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
                written += 1

            done = min(start + batch_size, len(post_ids))
            print(f"Hydrated {done}/{len(post_ids)} remaining IDs ({written} total saved)...")
            time.sleep(sleep_seconds)

    return {
        "output": str(output_path),
        "annotation_rows": len(annotation_map),
        "hydrated_posts": written,
        "missing_or_empty": missing,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build MASH Reddit JSONL (~10k Helene/Milton posts) from Hugging Face + PullPush."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=None, help="Limit rows for testing (default: all ~9928)")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--resume", action="store_true", help="Append to existing output, skip saved IDs")
    args = parser.parse_args()

    stats = build_mash_dataset(
        output_path=args.output,
        limit=args.limit,
        batch_size=args.batch_size,
        resume=args.resume,
    )
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
