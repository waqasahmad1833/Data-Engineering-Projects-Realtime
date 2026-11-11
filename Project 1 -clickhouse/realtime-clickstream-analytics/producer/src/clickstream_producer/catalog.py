"""Static reference data used by the simulator: products, pages, referrers, devices."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str
    price: float

    @property
    def url(self) -> str:
        return f"/products/{self.product_id}"


_CATEGORY_PRICE_RANGES = {
    "electronics": (49.0, 899.0),
    "books": (7.0, 45.0),
    "fashion": (15.0, 220.0),
    "home": (12.0, 350.0),
    "sports": (10.0, 280.0),
}


def _build_catalog(products_per_category: int = 10, seed: int = 7) -> tuple[Product, ...]:
    rng = random.Random(seed)
    products = []
    for category, (low, high) in _CATEGORY_PRICE_RANGES.items():
        for i in range(1, products_per_category + 1):
            products.append(
                Product(
                    product_id=f"{category[:3]}-{i:04d}",
                    name=f"{category.title()} item {i}",
                    category=category,
                    price=round(rng.uniform(low, high), 2),
                )
            )
    return tuple(products)


PRODUCTS: tuple[Product, ...] = _build_catalog()

CONTENT_PAGES: tuple[str, ...] = (
    "/",
    "/search?q=gift",
    "/deals",
    "/category/electronics",
    "/category/books",
    "/category/fashion",
    "/category/home",
    "/category/sports",
    "/blog/gift-guide",
    "/help/shipping",
)

# (value, weight) pairs
REFERRERS: tuple[tuple[str, float], ...] = (
    ("", 0.35),  # direct
    ("https://www.google.com/", 0.30),
    ("https://www.bing.com/", 0.05),
    ("https://www.facebook.com/", 0.10),
    ("https://www.instagram.com/", 0.10),
    ("https://newsletter.example.com/campaign?id=42", 0.10),
)

COUNTRIES: tuple[tuple[str, float], ...] = (
    ("DE", 0.30),
    ("GB", 0.20),
    ("NL", 0.15),
    ("FR", 0.12),
    ("US", 0.10),
    ("ES", 0.08),
    ("IT", 0.05),
)

USER_AGENTS: tuple[tuple[str, float], ...] = (
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/124.0 Safari/537.36", 0.30),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/17.4 Safari/605.1.15", 0.15),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/17.4 Mobile/15E148 Safari/604.1", 0.25),
    ("Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) "
     "Chrome/124.0 Mobile Safari/537.36", 0.20),
    ("Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
     "Version/17.4 Mobile/15E148 Safari/604.1", 0.07),
    ("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)", 0.03),
)
