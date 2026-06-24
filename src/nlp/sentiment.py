"""Sentiment analysis using VADER (tuned for social media text)."""

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()


def score_sentiment(text: str) -> dict:
    """Return VADER polarity scores and a discrete label."""
    if not text or not str(text).strip():
        return {
            "sentiment_compound": 0.0,
            "sentiment_pos": 0.0,
            "sentiment_neu": 1.0,
            "sentiment_neg": 0.0,
            "sentiment_label": "neutral",
        }

    scores = _analyzer.polarity_scores(str(text))
    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"

    return {
        "sentiment_compound": compound,
        "sentiment_pos": scores["pos"],
        "sentiment_neu": scores["neu"],
        "sentiment_neg": scores["neg"],
        "sentiment_label": label,
    }
