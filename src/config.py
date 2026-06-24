from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SAMPLE_PATH = DATA_DIR / "sample" / "reddit_sample.jsonl"
MASH_PATH = DATA_DIR / "sample" / "mash_reddit.jsonl"
OUTPUT_DIR = DATA_DIR / "output"
MODEL_DIR = PROJECT_ROOT / "models"

DISASTER_KEYWORDS = [
    "hurricane",
    "helene",
    "milton",
    "irma",
    "harvey",
    "maria",
    "wildfire",
    "flood",
    "earthquake",
    "tornado",
    "evacuation",
    "storm surge",
]

HURRICANE_KEYWORDS = [
    "hurricane",
    "helene",
    "milton",
    "irma",
    "harvey",
    "maria",
    "jose",
    "puerto rico",
]
