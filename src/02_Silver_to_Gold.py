from pyspark.sql import SparkSession
from config import OUTPUT_DIR_GOLD, OUTPUT_DIR_SILVER
import pyspark.sql.functions as F
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


spark = SparkSession.builder \
    .appName("Silver_to_Gold_Processing") \
    .getOrCreate()
# Avoid 200 default shuffle partitions — right-sized for ~3700 rows on 8 logical cores
spark.conf.set("spark.sql.shuffle.partitions", "8")

spark.sparkContext.setLogLevel("WARN")

logger.info(f"[GOLD] Reading raw data from: {OUTPUT_DIR_SILVER}")
df_silver = spark.read.parquet(OUTPUT_DIR_SILVER)
silver_count = df_silver.count()
logger.info(f"[GOLD] Rows loaded: {silver_count}")


gold_by_neighborhood = df_silver.filter(
    F.col("Neighborhood").isNotNull()
).groupby(
    F.coalesce(F.col("City_Sector"), F.lit("Ilfov / Necunoscut")).alias("Sector"),
    F.col("Neighborhood")
).agg(
    F.count("listing_id").alias("Numar_Anunturi"),
    F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
    F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR"),
    F.round(F.min("Price_EUR"), 0).alias("Pret_Min_EUR"),
    F.round(F.max("Price_EUR"), 0).alias("Pret_Max_EUR")
).filter(
    F.col("Numar_Anunturi") >= 2
).orderBy(
    F.col("Pret_Mediu_MP_EUR").desc()
)


gold_rooms_distribution = (df_silver.filter(
    F.col("rooms").isNotNull()
).groupby(
    F.col("rooms")
).agg(
    F.count("listing_id").alias("Numar_Anunturi"),
    F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
    F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR")
).withColumnRenamed("rooms", "Numar_Camere").orderBy(
    F.col("Numar_Camere").asc()
))


gold_market_summary = df_silver.agg(
    F.count("listing_id").alias("Total_Anunturi"),
    F.round(F.avg("Price_EUR"), 0).alias("Pret_Mediu_EUR"),
    F.round(F.avg("Price_per_sqm"), 0).alias("Pret_Mediu_MP_EUR"),
    F.min("Price_EUR").alias("Pret_Min_EUR"),
    F.max("Price_EUR").alias("Pret_Max_EUR")
)


logger.info("[GOLD] Saving gold_by_neighborhood...")
gold_by_neighborhood.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/by_neighborhood")
gold_by_neighborhood.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/by_neighborhood_csv")
logger.info("[GOLD] Saved: by_neighborhood (Parquet + CSV)")

logger.info("[GOLD] Saving gold_rooms_distribution...")
gold_rooms_distribution.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/rooms_distribution")
gold_rooms_distribution.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/rooms_distribution_csv")
logger.info("[GOLD] Saved: rooms_distribution (Parquet + CSV)")

logger.info("[GOLD] Saving gold_market_summary...")
gold_market_summary.write.mode("overwrite").parquet(f"{OUTPUT_DIR_GOLD}/market_summary")
gold_market_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR_GOLD}/market_summary_csv")
logger.info("[GOLD] Saved: market_summary (Parquet + CSV)")

logger.info("[GOLD] Pipeline complete.")

