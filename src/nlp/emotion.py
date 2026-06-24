"""Emotion detection using the NRC Emotion Lexicon (NRCLex)."""

import json
import re

from nrclex import NRCLex

EMOTION_LABELS = [
    "anger",
    "anticipation",
    "disgust",
    "fear",
    "joy",
    "sadness",
    "surprise",
    "trust",
    "positive",
    "negative",
]


def score_emotion(text: str) -> dict:
    """Return top emotion and full NRC emotion score map."""
    if not text or not str(text).strip():
        return {
            "top_emotion": "neutral",
            "emotion_scores": json.dumps({}),
        }

    lex = NRCLex()
    tokens = re.findall(r"[A-Za-z']+", str(text).lower())
    lex.load_token_list(tokens)
    scores = lex.raw_emotion_scores or {}
    if not scores:
        return {
            "top_emotion": "neutral",
            "emotion_scores": json.dumps({}),
        }

    top = max(scores, key=scores.get)
    return {
        "top_emotion": top,
        "emotion_scores": json.dumps(scores),
    }
