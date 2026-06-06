"""
test_transformations.py
───────────────────────
Unit tests for the pure-Python transformation helpers in src/transformations.py.

Each test is intentionally framework-free (no SparkSession) so the suite
runs in milliseconds and can be executed anywhere with:

    python -m pytest test/test_transformations.py -v
"""

import pandas as pd
import pytest

from src.transformations import clean_price, compute_price_per_sqm, extract_area


# ── Price cleaning ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("input_val, expected", [
    pytest.param("125 000 €", 125000, id="valid_price"),
    pytest.param("N/A",       None,   id="not_available"),
    pytest.param(None,        None,   id="none_input"),
])
def test_price_cleaning(input_val, expected):
    assert clean_price(input_val) == expected


# ── Area extraction ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("input_val, expected", [
    pytest.param("37 m²", 37.0, id="valid_area"),
    pytest.param(None,    None, id="none_input"),
])
def test_area_extraction(input_val, expected):
    assert extract_area(input_val) == expected


# ── Price per square metre ────────────────────────────────────────────────────

@pytest.mark.parametrize("price, area, expected", [
    pytest.param(100000, 50.0, 2000.0, id="valid_inputs"),
    pytest.param(None,   None, None,   id="none_inputs"),
])
def test_price_per_sqm(price, area, expected):
    assert compute_price_per_sqm(price=price, area=area) == expected


# ── Gold filter — minimum listing threshold ───────────────────────────────────

def test_gold_filter():
    df = pd.DataFrame({
        "Neighborhood": ["Floreasca", "Titan"],
        "count":        [1,           3      ],
    })
    result = df[df["count"] >= 2]

    assert len(result) == 1
    assert result["Neighborhood"].values[0] == "Titan"
