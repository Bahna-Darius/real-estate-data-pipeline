"""
transformations.py
──────────────────
Pure-Python transformation helpers that mirror the PySpark logic in
01_Bronze_to_Silver.py.  Keeping them framework-free allows fast,
dependency-light unit testing without spinning up a SparkSession.
"""

import re


def clean_price(raw: str) -> int | None:
    """Extract a numeric EUR price from a raw scraped string.

    Args:
        raw: Raw price string, e.g. ``"125 000 €"``.

    Returns:
        Integer price, or ``None`` if the input contains no digits.

    Examples:
        >>> clean_price("125 000 €")
        125000
        >>> clean_price("N/A") is None
        True
    """
    if not raw or not isinstance(raw, str):
        return None
    digits = re.sub(r"[^0-9]", "", raw)
    if not digits:
        return None
    return int(digits)


def extract_area(raw: str) -> float | None:
    """Extract a numeric area value (m²) from a raw scraped string.

    Args:
        raw: Raw area string, e.g. ``"67 mp"``.

    Returns:
        Area as a float, or ``None`` if no number is found.

    Examples:
        >>> extract_area("67 mp")
        67.0
        >>> extract_area("") is None
        True
    """
    if not raw or not isinstance(raw, str):
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", raw)
    if not match:
        return None
    return float(match.group(1))


def compute_price_per_sqm(price: int | None, area: float | None) -> float | None:
    """Compute price per square metre, rounded to two decimal places.

    Args:
        price: Total price in EUR (integer).
        area:  Area in m² (float).

    Returns:
        EUR/m² rounded to 2 decimals, or ``None`` when either input is
        missing or ``area`` is zero.

    Examples:
        >>> compute_price_per_sqm(100_000, 50.0)
        2000.0
        >>> compute_price_per_sqm(None, 50.0) is None
        True
    """
    if price is None or area is None or area == 0:
        return None
    return round(price / area, 2)
