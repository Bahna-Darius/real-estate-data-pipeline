from config import RAW_DATA_DIR, OUTPUT_DIR_SILVER
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


spark = SparkSession.builder \
    .appName("Bronze_to_Silver_Processing") \
    .getOrCreate()

# Avoid 200 default shuffle partitions — right-sized for ~3700 rows on 8 logical cores
spark.conf.set("spark.sql.shuffle.partitions", "8")

spark.sparkContext.setLogLevel("WARN")


# ── BRONZE ────────────────────────────────────────────────────────────────────

logger.info(f"[BRONZE] Reading raw data from: {RAW_DATA_DIR}")
df_raw = spark.read.json(RAW_DATA_DIR, multiLine=True)
bronze_count = df_raw.count()
logger.info(f"[BRONZE] Rows loaded: {bronze_count}")


# ── SILVER — Type casting & enrichment ───────────────────────────────────────

logger.info("[SILVER] Starting transformations...")

df_silver = df_raw.withColumn(
    "Price_EUR",
    F.expr("try_cast(regexp_replace(price, '[^0-9]', '') AS INT)")
).withColumn(
    "Area_sqm",
    F.expr("try_cast(regexp_extract(area, '([0-9.]+)', 1) AS DOUBLE)")
).withColumn(
    "Price_per_sqm",
    F.round(F.col("Price_EUR") / F.col("Area_sqm"), 2)
)

df_silver = df_silver.withColumn(
    "rooms",
    F.expr("try_cast(regexp_extract(rooms, '([0-9]+)', 1) AS INT)")
)

# Empty string → null before regex extraction
df_silver = df_silver.withColumn(
    "location",
    F.when(F.trim(F.col("location")) == "", F.lit(None)).otherwise(F.trim(F.col("location")))
)

df_silver = df_silver.withColumn(
    "City_Sector",
    F.regexp_extract(F.col("location"), r"(?i)(sector(?:ul)?\s*[1-6])", 1)
).withColumn(
    "City_Sector",
    F.when(F.col("City_Sector") == "", F.lit(None)).otherwise(F.col("City_Sector"))
)

df_silver = df_silver.withColumn(
    "Neighborhood",
    F.regexp_replace(F.col("location"), r"(?i),?\s*sector(?:ul)?\s*[1-6]", "")
).withColumn(
    "Neighborhood",
    F.trim(F.regexp_replace(F.col("Neighborhood"), r"(?i),?\s*(Bucure[sșţt]ti|Ilfov)", ""))
).withColumn(
    "Neighborhood",
    F.when(F.col("Neighborhood") == "", F.lit(None)).otherwise(F.col("Neighborhood"))
)

# Raw string columns replaced by typed versions above
df_silver = df_silver.drop("price", "location", "area")


# ── DATA QUALITY ──────────────────────────────────────────────────────────────

logger.info("[SILVER] Null value report:")
df_silver.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_silver.columns
]).show()

# Count before write — calling count() after write() would re-trigger the full pipeline
silver_count = df_silver.count()


# ── SAVE ──────────────────────────────────────────────────────────────────────

df_silver.write.format("parquet").mode("overwrite").save(OUTPUT_DIR_SILVER)

logger.info(f"[SILVER] Saved to: {OUTPUT_DIR_SILVER}")
logger.info(f"[SILVER] Done — Bronze: {bronze_count} rows → Silver: {silver_count} rows")
