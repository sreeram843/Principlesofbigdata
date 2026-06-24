"""PySpark analytics: descriptive stats, NLP enrichment, and ML classification."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.config import HURRICANE_KEYWORDS, OUTPUT_DIR
from src.nlp.classification import add_weak_labels, apply_classifier, train_classifier
from src.nlp.emotion import score_emotion
from src.nlp.sentiment import score_sentiment


def create_spark(app_name: str = "RedditDisasterAnalysis") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_posts(spark: SparkSession, input_path: Path) -> DataFrame:
    df = spark.read.json(str(input_path))
    if "text" not in df.columns:
        df = df.withColumn(
            "text",
            F.concat_ws(
                "\n",
                F.coalesce(F.col("title"), F.lit("")),
                F.coalesce(F.col("selftext"), F.lit("")),
            ),
        )
    return df.withColumn("text", F.coalesce(F.col("text"), F.lit("")))


def _sentiment_udf():
  @F.udf(
      returnType=StructType(
          [
              StructField("sentiment_compound", FloatType()),
              StructField("sentiment_pos", FloatType()),
              StructField("sentiment_neu", FloatType()),
              StructField("sentiment_neg", FloatType()),
              StructField("sentiment_label", StringType()),
          ]
      )
  )
  def _score(text):
      result = score_sentiment(text)
      return (
          result["sentiment_compound"],
          result["sentiment_pos"],
          result["sentiment_neu"],
          result["sentiment_neg"],
          result["sentiment_label"],
      )

  return _score


def _emotion_udf():
  @F.udf(
      returnType=StructType(
          [
              StructField("top_emotion", StringType()),
              StructField("emotion_scores", StringType()),
          ]
      )
  )
  def _score(text):
      result = score_emotion(text)
      return (result["top_emotion"], result["emotion_scores"])

  return _score


def enrich_with_nlp(df: DataFrame) -> DataFrame:
    sentiment = _sentiment_udf()
    emotion = _emotion_udf()

    with_sentiment = df.withColumn("sentiment", sentiment(F.col("text")))
    with_sentiment = (
        with_sentiment.withColumn("sentiment_compound", F.col("sentiment.sentiment_compound"))
        .withColumn("sentiment_label", F.col("sentiment.sentiment_label"))
        .drop("sentiment")
    )

    with_emotion = with_sentiment.withColumn("emotion", emotion(F.col("text")))
    with_emotion = (
        with_emotion.withColumn("top_emotion", F.col("emotion.top_emotion"))
        .withColumn("emotion_scores", F.col("emotion.emotion_scores"))
        .drop("emotion")
    )
    return with_emotion


def _write_csv(df: DataFrame, name: str, output_dir: Path) -> None:
    path = output_dir / name
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(path))


def run_descriptive_analytics(df: DataFrame, output_dir: Path) -> None:
    """Mirror legacy Twitter analytics with Reddit-specific fields."""
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        df.groupBy("author").count().orderBy(F.desc("count")).limit(10),
        "top_authors",
        output_dir,
    )

    if "subreddit" in df.columns:
        _write_csv(
            df.groupBy("subreddit").count().orderBy(F.desc("count")).limit(15),
            "subreddit_counts",
            output_dir,
        )

    if "score" in df.columns:
        _write_csv(
            df.groupBy("author")
            .agg(F.max("score").alias("max_score"), F.count("*").alias("post_count"))
            .orderBy(F.desc("max_score"))
            .limit(20),
            "top_scores_by_author",
            output_dir,
        )

    keyword_rows = []
    for keyword in HURRICANE_KEYWORDS:
        count = df.filter(F.lower(F.col("text")).contains(keyword.lower())).count()
        keyword_rows.append((keyword, count))

    keyword_df = df.sparkSession.createDataFrame(keyword_rows, ["keyword", "count"])
    _write_csv(keyword_df.orderBy(F.desc("count")), "hurricane_keyword_counts", output_dir)

    if "is_self" in df.columns:
        _write_csv(
            df.groupBy("is_self").count().orderBy(F.desc("count")),
            "post_type_counts",
            output_dir,
        )


def run_nlp_summaries(df: DataFrame, output_dir: Path) -> None:
    _write_csv(
        df.groupBy("sentiment_label").count().orderBy(F.desc("count")),
        "sentiment_distribution",
        output_dir,
    )
    _write_csv(
        df.groupBy("top_emotion").count().orderBy(F.desc("count")),
        "emotion_distribution",
        output_dir,
    )
    _write_csv(
        df.select("author", "subreddit", "sentiment_label", "top_emotion", "sentiment_compound")
        .orderBy(F.desc("sentiment_compound"))
        .limit(50),
        "most_positive_posts",
        output_dir,
    )
    _write_csv(
        df.select("author", "subreddit", "sentiment_label", "top_emotion", "sentiment_compound")
        .orderBy(F.asc("sentiment_compound"))
        .limit(50),
        "most_negative_posts",
        output_dir,
    )


def run_ml_pipeline(df: DataFrame, output_dir: Path) -> dict:
    labeled = add_weak_labels(df)
    model, metrics = train_classifier(df.sparkSession, labeled)
    predicted = apply_classifier(model, labeled)

    _write_csv(
        predicted.groupBy("ml_predicted_label").count().orderBy(F.desc("count")),
        "ml_classification_distribution",
        output_dir,
    )
    _write_csv(
        predicted.groupBy("weak_label", "ml_predicted_label").count().orderBy(F.desc("count")),
        "ml_confusion_summary",
        output_dir,
    )

    metrics_path = output_dir / "ml_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics


def analyze(input_path: Path, output_dir: Path | None = None) -> dict:
    output_dir = output_dir or OUTPUT_DIR
    spark = create_spark()

    try:
        df = load_posts(spark, input_path)
        enriched = enrich_with_nlp(df)
        run_descriptive_analytics(enriched, output_dir)
        run_nlp_summaries(enriched, output_dir)
        ml_metrics = run_ml_pipeline(enriched, output_dir)

        enriched.coalesce(1).write.mode("overwrite").json(str(output_dir / "enriched_posts"))
        return {
            "input": str(input_path),
            "output_dir": str(output_dir),
            "post_count": df.count(),
            "ml_metrics": ml_metrics,
        }
    finally:
        spark.stop()
