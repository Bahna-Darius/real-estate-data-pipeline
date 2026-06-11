"""
test_gold_pipeline.py
─────────────────────
Integration tests for the Silver → Gold aggregation pipeline.

A real SparkSession is created in local mode so these tests exercise the
exact same PySpark code that runs in production — not a Python copy of it.
Fixture data is minimal (7 rows) and covers all edge cases:
  - Floreasca (Sector 2)  — 3 listings, passes the >= 2 filter
  - Titan     (Sector 3)  — 1 listing,  filtered out
  - Voluntari (no sector) — 2 listings, City_Sector=None → "Ilfov / Necunoscut"
  - Dorobanti (Sector 1)  — 1 listing, rooms=None, filtered from rooms_distribution

Run with:
    python -m pytest tests/test_gold_pipeline.py -v
"""

import importlib
import sys
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# src/ on path for config.py; src/pipeline/ for the script itself
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "pipeline"))

# File starts with a digit — standard import syntax won't work
_module = importlib.import_module("02_Silver_to_Gold")
build_by_neighborhood    = _module.build_by_neighborhood
build_rooms_distribution = _module.build_rooms_distribution
build_market_summary     = _module.build_market_summary


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests in this module — started once."""
    session = (
        SparkSession.builder
        .appName("test_gold_pipeline")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def df_silver_to_gold(spark: SparkSession) -> DataFrame:
    """Minimal Silver fixture data — built once, shared across all Gold fixtures."""
    schema = StructType([
        StructField("listing_id",    StringType()),
        StructField("Price_EUR",     IntegerType()),
        StructField("Area_sqm",      FloatType()),
        StructField("Price_per_sqm", FloatType()),
        StructField("rooms",         IntegerType()),
        StructField("City_Sector",   StringType()),
        StructField("Neighborhood",  StringType()),
    ])

    rows = [
        # Floreasca Sector 2 — 3 listings, passes the >= 2 filter
        ("id1", 125000, 67.0, 1865.67, 2, "Sector 2", "Floreasca"),
        ("id2", 150000, 80.0, 1875.0,  2, "Sector 2", "Floreasca"),
        ("id3", 200000, 90.0, 2222.22, 3, "Sector 2", "Floreasca"),
        # Titan Sector 3 — 1 listing, filtered out by the >= 2 rule
        ("id4",  95000, 55.0, 1727.27, 2, "Sector 3", "Titan"),
        # Voluntari — 2 listings with no sector → coalesce to "Ilfov / Necunoscut"
        ("id5",  80000, 50.0, 1600.0,  2, None,       "Voluntari"),
        ("id6",  85000, 52.0, 1634.61, 2, None,       "Voluntari"),
        # Dorobanti Sector 1 — rooms=None, excluded from rooms_distribution
        ("id7", 110000, 60.0, 1833.33, None, "Sector 1", "Dorobanti"),
    ]

    return spark.createDataFrame(data=rows, schema=schema)


@pytest.fixture(scope="session")
def df_build_by_neighborhood(df_silver_to_gold: DataFrame) -> DataFrame:
    """Gold table: avg price per sector + neighborhood."""
    return build_by_neighborhood(df_silver_to_gold)


@pytest.fixture(scope="session")
def df_build_rooms_distribution(df_silver_to_gold: DataFrame) -> DataFrame:
    """Gold table: listing count and avg price per room type."""
    return build_rooms_distribution(df_silver_to_gold)


@pytest.fixture(scope="session")
def df_build_market_summary(df_silver_to_gold: DataFrame) -> DataFrame:
    """Gold table: single-row global market snapshot."""
    return build_market_summary(df_silver_to_gold)


# ── Tests: by_neighborhood ────────────────────────────────────────────────────

def test_filter_by_neighborhood_one_announcement(df_build_by_neighborhood):
    """Neighborhoods with < 2 listings must be absent from the output."""
    neighborhoods = [row["Neighborhood"] for row in df_build_by_neighborhood.collect()]
    assert "Titan"     not in neighborhoods
    assert "Dorobanti" not in neighborhoods

def test_filter_by_neighborhood_announcements(df_build_by_neighborhood):
    """Neighborhoods with >= 2 listings must be present in the output."""
    neighborhoods = [row["Neighborhood"] for row in df_build_by_neighborhood.collect()]
    assert "Floreasca" in neighborhoods

def test_sector_null_is_ilfov(df_build_by_neighborhood):
    """Listings with no City_Sector must be grouped under 'Ilfov / Necunoscut'."""
    row = df_build_by_neighborhood.filter(
        df_build_by_neighborhood["Neighborhood"] == "Voluntari"
    ).collect()[0]
    assert row["Sector"] == "Ilfov / Necunoscut"

def test_sort_desc_after_price_per_mp(df_build_by_neighborhood):
    """Output must be ordered by Pret_Mediu_MP_EUR descending."""
    prices = [row["Pret_Mediu_MP_EUR"] for row in df_build_by_neighborhood.collect()]
    assert prices == sorted(prices, reverse=True)


# ── Tests: market_summary ─────────────────────────────────────────────────────

def test_market_summary_agg(df_build_market_summary):
    """Global aggregation must produce exactly one row."""
    assert df_build_market_summary.count() == 1

def test_market_summary_total_announcements(df_build_market_summary):
    """Total_Anunturi must equal the number of rows in the Silver fixture (7)."""
    assert df_build_market_summary.collect()[0]["Total_Anunturi"] == 7


# ── Tests: rooms_distribution ─────────────────────────────────────────────────

def test_rooms_null_excluded(df_build_rooms_distribution):
    """Listings with null rooms must not appear in the rooms distribution."""
    rooms = [row["Numar_Camere"] for row in df_build_rooms_distribution.collect()]
    assert None not in rooms

def test_rooms_order_increase(df_build_rooms_distribution):
    """Room types must be ordered ascending (1 cam., 2 cam., 3 cam. ...)."""
    rooms = [row["Numar_Camere"] for row in df_build_rooms_distribution.collect()]
    assert rooms == sorted(rooms)
