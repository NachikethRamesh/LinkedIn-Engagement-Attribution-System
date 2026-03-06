from __future__ import annotations

EVENT_WEIGHTS: dict[str, float] = {
    # Aggregate-only awareness metrics are intentionally excluded from
    # account-level scoring unless a future deterministic account linkage exists.
    "post_impression": 0.0,
    "post_like": 1.0,
    "post_comment": 5.0,
    "post_repost": 7.0,
    "post_link_click": 0.0,
}

WINDOW_DAYS: dict[str, int] = {
    "rolling_7d": 7,
    "rolling_30d": 30,
}

RECENCY_BUCKETS: list[tuple[int, float]] = [
    (7, 1.0),
    (14, 0.75),
    (30, 0.50),
]

AGGREGATED_DAMPENING = 0.35
MAX_AGGREGATED_INTENSITY = 5.0

STAKEHOLDER_BONUS_BY_COUNT: dict[int, float] = {
    2: 5.0,
    3: 10.0,
}

STRONG_SIGNAL_TYPES = {"post_comment", "post_repost"}

TIER_BONUS: dict[str, float] = {
    "tier 1": 4.0,
    "tier 2": 2.0,
    "tier 3": 0.0,
}

# Bonus for accounts deterministically resolved against CRM entities in identity resolution.
CRM_MATCH_BONUS = 3.0
