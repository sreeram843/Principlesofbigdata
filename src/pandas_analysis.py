"""Pandas + scikit-learn analytics engine (no Java/Spark required)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline as SklearnPipeline

from src.config import HURRICANE_KEYWORDS, MODEL_DIR, OUTPUT_DIR
from src.nlp.classification import weak_label
from src.nlp.emotion import score_emotion
from src.nlp.sentiment import score_sentiment


def _load_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    if "text" not in df.columns:
        df["text"] = (df.get("title", "").fillna("") + "\n" + df.get("selftext", "").fillna("")).str.strip()
    return df


def _write_csv(df: pd.DataFrame, name: str, output_dir: Path) -> None:
    path = output_dir / f"{name}.csv"
    df.to_csv(path, index=False)


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    sentiment_rows = df["text"].apply(score_sentiment).apply(pd.Series)
    emotion_rows = df["text"].apply(score_emotion).apply(pd.Series)
    enriched = pd.concat([df, sentiment_rows, emotion_rows], axis=1)
    enriched["weak_label"] = enriched["text"].apply(weak_label)
    return enriched


def run_descriptive_analytics(df: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        df.groupby("author").size().reset_index(name="count").sort_values("count", ascending=False).head(10),
        "top_authors",
        output_dir,
    )

    if "subreddit" in df.columns:
        _write_csv(
            df.groupby("subreddit").size().reset_index(name="count").sort_values("count", ascending=False).head(15),
            "subreddit_counts",
            output_dir,
        )

    if "score" in df.columns:
        _write_csv(
            df.groupby("author")
            .agg(max_score=("score", "max"), post_count=("score", "count"))
            .reset_index()
            .sort_values("max_score", ascending=False)
            .head(20),
            "top_scores_by_author",
            output_dir,
        )

    keyword_rows = []
    for keyword in HURRICANE_KEYWORDS:
        count = df["text"].str.lower().str.contains(keyword.lower(), regex=False).sum()
        keyword_rows.append({"keyword": keyword, "count": int(count)})
    _write_csv(pd.DataFrame(keyword_rows).sort_values("count", ascending=False), "hurricane_keyword_counts", output_dir)

    if "is_self" in df.columns:
        _write_csv(
            df.groupby("is_self").size().reset_index(name="count").sort_values("count", ascending=False),
            "post_type_counts",
            output_dir,
        )


def run_nlp_summaries(df: pd.DataFrame, output_dir: Path) -> None:
    _write_csv(
        df.groupby("sentiment_label").size().reset_index(name="count").sort_values("count", ascending=False),
        "sentiment_distribution",
        output_dir,
    )
    _write_csv(
        df.groupby("top_emotion").size().reset_index(name="count").sort_values("count", ascending=False),
        "emotion_distribution",
        output_dir,
    )
    _write_csv(
        df[["author", "subreddit", "sentiment_label", "top_emotion", "sentiment_compound"]]
        .sort_values("sentiment_compound", ascending=False)
        .head(50),
        "most_positive_posts",
        output_dir,
    )
    _write_csv(
        df[["author", "subreddit", "sentiment_label", "top_emotion", "sentiment_compound"]]
        .sort_values("sentiment_compound", ascending=True)
        .head(50),
        "most_negative_posts",
        output_dir,
    )


def run_ml_pipeline(df: pd.DataFrame, output_dir: Path) -> dict:
    labeled = df[df["weak_label"] != "other"].copy()
    if len(labeled) < 10:
        raise ValueError("Not enough labeled posts to train a classifier (need at least 10 non-'other' rows).")

    x_train, x_test, y_train, y_test = train_test_split(
        labeled["text"],
        labeled["weak_label"],
        test_size=0.2,
        random_state=42,
        stratify=labeled["weak_label"],
    )

    model = SklearnPipeline(
        [
            ("tfidf", TfidfVectorizer(max_features=4096, ngram_range=(1, 2))),
            ("clf", LogisticRegression(max_iter=1000)),
        ]
    )
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    metrics = {
        "f1": round(float(f1_score(y_test, predictions, average="weighted")), 4),
        "accuracy": round(float(accuracy_score(y_test, predictions)), 4),
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "engine": "sklearn",
    }

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / "disaster_classifier_sklearn.joblib"
    import joblib

    joblib.dump(model, model_path)
    metrics["model_path"] = str(model_path)

    df["ml_predicted_label"] = model.predict(df["text"])

    _write_csv(
        df.groupby("ml_predicted_label").size().reset_index(name="count").sort_values("count", ascending=False),
        "ml_classification_distribution",
        output_dir,
    )
    _write_csv(
        df.groupby(["weak_label", "ml_predicted_label"]).size().reset_index(name="count").sort_values("count", ascending=False),
        "ml_confusion_summary",
        output_dir,
    )

    metrics_path = output_dir / "ml_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def analyze_pandas(input_path: Path, output_dir: Path | None = None) -> dict:
    output_dir = output_dir or OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    df = _load_jsonl(input_path)
    enriched = enrich_dataframe(df)
    run_descriptive_analytics(enriched, output_dir)
    run_nlp_summaries(enriched, output_dir)
    ml_metrics = run_ml_pipeline(enriched, output_dir)

    enriched.to_json(output_dir / "enriched_posts.jsonl", orient="records", lines=True)

    return {
        "input": str(input_path),
        "output_dir": str(output_dir),
        "post_count": len(enriched),
        "engine": "pandas",
        "ml_metrics": ml_metrics,
    }
