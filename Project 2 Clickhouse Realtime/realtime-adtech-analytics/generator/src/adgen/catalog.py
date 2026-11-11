"""Reference data for the simulation: publishers, formats, devices, geos."""

from __future__ import annotations

from .ids import NUM_PUBLISHERS

_SITE_CATEGORIES = ("news", "sports", "tech", "recipes", "travel", "finance", "gaming", "music")

# publisher_id -> site domain
PUBLISHERS: dict[int, str] = {
    pid: f"{_SITE_CATEGORIES[pid % len(_SITE_CATEGORIES)]}{pid}.example.com"
    for pid in range(1, NUM_PUBLISHERS + 1)
}

# (value, weight)
AD_FORMATS: tuple[tuple[str, float], ...] = (("banner", 0.55), ("native", 0.25), ("video", 0.20))

DEVICES: tuple[tuple[str, float], ...] = (
    ("mobile", 0.55), ("desktop", 0.30), ("tablet", 0.08), ("ctv", 0.07),
)

OS_BY_DEVICE: dict[str, tuple[tuple[str, float], ...]] = {
    "mobile": (("android", 0.6), ("ios", 0.4)),
    "tablet": (("ios", 0.6), ("android", 0.4)),
    "desktop": (("windows", 0.65), ("macos", 0.30), ("linux", 0.05)),
    "ctv": (("tizen", 0.4), ("webos", 0.3), ("roku", 0.3)),
}

COUNTRIES: tuple[tuple[str, float], ...] = (
    ("US", 0.35), ("DE", 0.15), ("GB", 0.12), ("FR", 0.10), ("BR", 0.08),
    ("IN", 0.08), ("JP", 0.07), ("NL", 0.05),
)

# CPM bid range in USD per format; prices are stored in micros per impression
CPM_RANGE_USD: dict[str, tuple[float, float]] = {
    "banner": (0.5, 4.0),
    "native": (1.0, 8.0),
    "video": (5.0, 25.0),
}
