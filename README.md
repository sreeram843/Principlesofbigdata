# Principles of Big Data — Reddit Disaster Analytics

Modernized big-data pipeline for analyzing disaster-related **Reddit** posts. Replaces the legacy Twitter/Spark 1.x stack with Python, PySpark 3.5+, and NLP/ML features.

## What's included

| Feature | Implementation |
|---------|----------------|
| **Data** | Bundled samples: 38-post demo or **~7.5k–10k MASH** (Helene/Milton 2024) |
| **Sentiment analysis** | VADER (`positive` / `neutral` / `negative`) |
| **Emotion detection** | NRC Emotion Lexicon via NRCLex |
| **ML classification** | TF-IDF + Logistic Regression (Spark MLlib or scikit-learn) |
| **Analytics** | Top authors, subreddits, hurricane keyword counts |

Legacy Twitter/Scala code remains under `Source/` for reference only.

## Architecture

```text
data/sample/reddit_sample.jsonl
        │
        ▼
  src/run_pipeline.py  (Spark or pandas)
        │
        ├── Descriptive analytics → data/output/*.csv
        ├── Sentiment + emotion enrichment
        └── ML classifier training + predictions
```

## Quick start

### 1. Prerequisites

- Python 3.10+
- Java 17+ (for Spark — configured via `.tool-versions` + asdf)

```bash
cd Principlesofbigdata
java -version   # should show OpenJDK 17.x
```

### 2. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run

**Quick demo (38 posts):**

```bash
python -m src.run_pipeline
```

**MASH dataset (~10k Helene/Milton Reddit posts, 2024):**

```bash
# One-time download (no Reddit API keys; uses Hugging Face + PullPush archive)
python scripts/download_mash.py

# If interrupted, resume:
python scripts/download_mash.py --resume

# Run pipeline on MASH data
python -m src.run_pipeline --input data/sample/mash_reddit.jsonl
```

MASH provides **9,928 annotated Reddit post IDs** from [Hugging Face](https://huggingface.co/datasets/YRC10/MASH). The download script hydrates post text from the [PullPush](https://pullpush.io/) archive. Some posts may be unavailable if deleted (~7.5k–9.9k typically succeed).

```bash
# Explicit paths / engine
python -m src.run_pipeline --input data/sample/reddit_sample.jsonl
python -m src.run_pipeline --engine spark    # PySpark + Spark MLlib (default when Java is available)
python -m src.run_pipeline --engine pandas   # scikit-learn fallback (no Java)
```

## Outputs

| Output | Description |
|--------|-------------|
| `top_authors.csv` | Most active authors |
| `subreddit_counts.csv` | Posts per subreddit |
| `hurricane_keyword_counts.csv` | Helene, Milton, Harvey, etc. |
| `sentiment_distribution.csv` | Positive / neutral / negative |
| `emotion_distribution.csv` | Fear, joy, anger, … |
| `ml_classification_distribution.csv` | Predicted disaster categories |
| `ml_metrics.json` | Classifier accuracy and F1 |
| `enriched_posts.jsonl` | Posts with all NLP fields |
| `models/disaster_classifier/` | Saved Spark ML model |

## Project layout

```text
.
├── data/
│   ├── sample/
│   │   ├── reddit_sample.jsonl      # 38-post demo
│   │   └── mash_reddit.jsonl        # MASH (~7.5k–10k, after download)
│   └── output/                      # Pipeline results
├── scripts/
│   └── download_mash.py             # Fetch MASH Reddit dataset
├── src/
│   ├── nlp/                         # Sentiment, emotion, classification
│   ├── spark/analysis.py            # PySpark pipeline
│   ├── pandas_analysis.py           # Fallback without Java
│   └── run_pipeline.py              # Entry point
├── models/                          # Trained classifier
└── Source/                          # Legacy Twitter/Scala (2017)
```

## Legacy mapping

| Old (Twitter) | New |
|---------------|-----|
| `tweets.py` | Not used — bundled Reddit sample |
| `tweetanalysis.scala` | `src/spark/analysis.py` |
| `HashTweet.py` | Not needed |
| `SparkWordCount.scala` | Keyword counts in analytics |

## Course context

Original project analyzed 2017 hurricane tweets. The modernized pipeline uses Reddit-formatted disaster posts covering hurricanes (Helene, Milton, Harvey, Irma, Maria), wildfires, floods, earthquakes, and tornadoes.
