import os
import logging
import pandas as pd
from config import RAW_CSV_PATH, PROCESSED_CSV_PATH, PROCESSED_DATA_DIR


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)


def clean_real_estate_data(input_path: str, output_path: str) -> None:
    """
    Orchestrates the ETL process: Reads raw Bronze data, cleans Price and Area columns,
    performs type conversion, and persists the results as Silver-tier data.
    """

    # 1. Validation: Ensure raw data source exists
    if not os.path.exists(input_path):
        logger.error(f"Source file not found: {input_path}")
        return

    logger.info(f"Loading raw dataset from: {input_path}")
    df = pd.read_csv(input_path)

    initial_row_count = len(df)
    logger.info(f"Imported {initial_row_count} records for processing.")

    # 2. Price Normalization
    # Transformation: "114 900 €" -> "114900" -> float
    # Logic: Strip all non-numeric characters using regex
    logger.info("Cleaning 'Price' attribute...")
    df['Price_EUR'] = df['Price'].astype(str).str.replace(r'[^\d]', '', regex=True)

    # Coerce errors to NaN to maintain dataset integrity for downstream analysis
    df['Price_EUR'] = pd.to_numeric(df['Price_EUR'], errors='coerce')

    # 3. Area Normalization
    # Transformation: "56.66 m²" -> "56.66" -> float
    # Logic: Keep only digits and decimal points
    logger.info("Cleaning 'Area' attribute...")
    df['Area_sqm'] = df['Area'].astype(str).str.replace(r'[^\d.]', '', regex=True)
    df['Area_sqm'] = pd.to_numeric(df['Area_sqm'], errors='coerce')

    # 4. Schema Refinement
    # Remove raw/unprocessed columns and reorganize for logical flow
    logger.info("Reorganizing DataFrame schema...")
    df = df.drop(columns=['Price', 'Area'])

    # Standardize column order: Core identifiers followed by metrics
    cols_order = ['Title', 'Price_EUR', 'Area_sqm', 'Link']
    df = df[cols_order]


    # 5. Data Persistence (Silver Layer)
    # Ensure target directory exists before writing
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    try:
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"Process complete. Silver data saved to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to save processed data: {e}")

    logger.info("\nData Transformation Preview:\n" + df.head().to_string())


def main() -> None:
    """
    Main entry point: Defines environment paths and initiates the cleaning pipeline.
    """
    clean_real_estate_data(RAW_CSV_PATH, PROCESSED_CSV_PATH)


if __name__ == "__main__":
    main()