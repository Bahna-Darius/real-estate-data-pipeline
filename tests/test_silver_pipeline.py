"""
test_silver_pipeline.py
───────────────────────
Integration tests for the Bronze → Silver transformation pipeline.

A real SparkSession is created in local mode so these tests exercise the
exact same PySpark code that runs in production — not a Python copy of it.
Fixture data is minimal (5 rows) to keep startup overhead acceptable.

Run with:
    python -m pytest tests/test_silver_pipeline.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

import sys
import importlib
from pathlib import Path

# src/ on path for config.py; src/pipeline/ for the script itself
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "pipeline"))

# File starts with a digit — standard import syntax won't work
_module = importlib.import_module("01_Bronze_to_Silver")
transform_bronze_to_silver = _module.transform_bronze_to_silver


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests in this module — started once."""
    session = (
        SparkSession.builder
        .appName("test_silver_pipeline")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def df_silver(spark):
    """Run transform_bronze_to_silver once on fixture data; reuse across tests."""
    schema = StructType([
        StructField("listing_id", StringType()),
        StructField("price",      StringType()),
        StructField("area",       StringType()),
        StructField("rooms",      StringType()),
        StructField("location",   StringType()),
        StructField("title",      StringType()),
        StructField("url",        StringType()),
    ])

    rows = [
        ("id1", "125 000 €",   "67 mp",  "2 camere", "Floreasca, Sector 2, București", "Ap 2 cam", "url1"),
        ("id2", "250 000 €",   "120 mp", "4 camere", "Dorobanți, Sector 1, București", "Ap 4 cam", "url2"),
        ("id3", "N/A",         "50 mp",  "1 camera", "Titan, Sector 3, București",     "Ap 1 cam", "url3"),
        ("id4", "95 000 €",    "N/A",    "2 camere", "Militari, Sector 6, București",  "Ap 2 cam", "url4"),
        ("id5", "180 000 €",   "90 mp",  "",         "",                               "Ap 3 cam", "url5"),
    ]

    df_raw = spark.createDataFrame(rows, schema=schema)
    return transform_bronze_to_silver(df_raw)


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_price_cast_to_int(df_silver):
    """Valid price strings are cast to INT; invalid strings become null."""
    row = df_silver.filter(df_silver.listing_id == "id1").collect()[0]
    assert row["Price_EUR"] == 125000

def test_invalid_price_becomes_null(df_silver):
    """'N/A' price must produce a null Price_EUR, not crash."""
    row = df_silver.filter(df_silver.listing_id == "id3").collect()[0]
    assert row["Price_EUR"] is None

def test_area_cast_to_double(df_silver):
    """Area string is extracted and cast to DOUBLE."""
    row = df_silver.filter(df_silver.listing_id == "id1").collect()[0]
    assert row["Area_sqm"] == 67.0

def test_price_per_sqm_computed(df_silver):
    """Price per m² is correctly computed and rounded to 2 decimals."""
    row = df_silver.filter(df_silver.listing_id == "id2").collect()[0]
    assert row["Price_per_sqm"] == round(250000 / 120, 2)

def test_missing_area_yields_null_price_per_sqm(df_silver):
    """When area is missing, Price_per_sqm must be null — not a division error."""
    row = df_silver.filter(df_silver.listing_id == "id4").collect()[0]
    assert row["Price_per_sqm"] is None

def test_city_sector_extracted(df_silver):
    """Sector is correctly extracted from the location string."""
    row = df_silver.filter(df_silver.listing_id == "id1").collect()[0]
    assert "Sector 2" in row["City_Sector"]

def test_empty_location_becomes_null(df_silver):
    """Empty location string must produce null City_Sector and Neighborhood."""
    row = df_silver.filter(df_silver.listing_id == "id5").collect()[0]
    assert row["City_Sector"] is None
    assert row["Neighborhood"] is None

def test_raw_columns_dropped(df_silver):
    """price, area and location columns must not exist in Silver."""
    assert "price"    not in df_silver.columns
    assert "area"     not in df_silver.columns
    assert "location" not in df_silver.columns
