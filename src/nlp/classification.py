"""Weak labeling and Spark ML classification for disaster-related Reddit posts."""

from __future__ import annotations

import re

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, StringIndexer, Tokenizer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.config import DISASTER_KEYWORDS, HURRICANE_KEYWORDS, MODEL_DIR

CATEGORY_RULES = [
    ("hurricane", HURRICANE_KEYWORDS),
    ("wildfire", ["wildfire", "fire", "smoke", "evacuation"]),
    ("flood", ["flood", "flooding", "storm surge", "inundation"]),
    ("earthquake", ["earthquake", "seismic", "tremor", "aftershock"]),
    ("tornado", ["tornado", "twister", "funnel cloud"]),
]


def normalize_text(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).lower()).strip()


def weak_label(text: str) -> str:
    """Assign a disaster category from keyword rules (training labels)."""
    normalized = normalize_text(text)
    if not normalized:
        return "other"

    for category, keywords in CATEGORY_RULES:
        if any(keyword in normalized for keyword in keywords):
            return category

    if any(keyword in normalized for keyword in DISASTER_KEYWORDS):
        return "disaster_general"

    return "other"


def add_weak_labels(df: DataFrame) -> DataFrame:
    label_udf = F.udf(weak_label, StringType())
    return df.withColumn("weak_label", label_udf(F.col("text")))


def train_classifier(
    spark: SparkSession,
    df: DataFrame,
    label_col: str = "weak_label",
    text_col: str = "text",
) -> tuple[Pipeline, dict]:
    """Train a Spark ML logistic regression classifier on TF-IDF features."""
    labeled = df.filter(F.col(text_col).isNotNull() & (F.length(F.col(text_col)) > 0))
    labeled = labeled.filter(F.col(label_col) != "other")

    if labeled.count() < 10:
        raise ValueError(
            "Not enough labeled posts to train a classifier (need at least 10 non-'other' rows)."
        )

    train_df, test_df = labeled.randomSplit([0.8, 0.2], seed=42)

    label_indexer = StringIndexer(inputCol=label_col, outputCol="label", handleInvalid="keep")
    tokenizer = Tokenizer(inputCol=text_col, outputCol="words")
    hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=2**16)
    idf = IDF(inputCol="raw_features", outputCol="features")
    classifier = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        regParam=0.01,
    )

    pipeline = Pipeline(stages=[label_indexer, tokenizer, hashing_tf, idf, classifier])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1",
    )
    f1 = evaluator.evaluate(predictions)
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy",
    )
    accuracy = accuracy_evaluator.evaluate(predictions)

    metrics = {
        "f1": round(f1, 4),
        "accuracy": round(accuracy, 4),
        "train_rows": train_df.count(),
        "test_rows": test_df.count(),
    }

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / "disaster_classifier"
    model.write().overwrite().save(str(model_path))
    metrics["model_path"] = str(model_path)

    return model, metrics


def apply_classifier(model: Pipeline, df: DataFrame) -> DataFrame:
    """Apply a trained pipeline and decode predicted labels."""
    predicted = model.transform(df)
    labels = model.stages[0].labels  # StringIndexer from training pipeline
    decode = F.udf(lambda idx: labels[int(idx)] if idx is not None and int(idx) < len(labels) else "unknown", StringType())
    return predicted.withColumn("ml_predicted_label", decode(F.col("prediction")))
