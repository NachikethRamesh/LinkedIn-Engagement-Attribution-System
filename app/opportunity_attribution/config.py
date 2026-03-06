from __future__ import annotations

from app.intent_scoring.config import EVENT_WEIGHTS

DEFAULT_WINDOW_DAYS = 30
ALLOWED_WINDOWS = {30, 60}

RECENCY_BUCKETS_30 = [
    (7, 1.0),
    (14, 0.75),
    (30, 0.5),
]

RECENCY_BUCKETS_60 = [
    (7, 1.0),
    (14, 0.8),
    (30, 0.6),
    (60, 0.35),
]

AGGREGATED_DAMPENING = 0.35
MAX_AGGREGATED_INTENSITY = 5.0

STRONG_SIGNAL_TYPES = {"post_comment", "post_repost"}

# Influence score bands.
INFLUENCE_BANDS = [
    (70.0, "strong"),
    (45.0, "medium"),
    (20.0, "weak"),
    (0.0, "none"),
]
