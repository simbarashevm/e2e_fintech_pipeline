import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ingest_raw_sources")


def get_spark_session() -> SparkSession:
    """Retrieves or creates an active PySpark session."""
    return SparkSession.builder.appName("FintechLandingIngestion").getOrCreate()


def ingest_raw_source(
    spark: SparkSession,
    source_name: str,
    source_path: str,
    base_landing_volume: str,
    execution_date: str
) -> None:
    """
    Reads incoming CSV files, adds metadata tags, and writes to a 
    date-partitioned landing path inside Unity Catalog.
    
    Idempotency: Re-running this for the same date overwrites only 
    that specific date's landing directory.
    """
    logger.info(f"Starting ingestion for '{source_name}' on date: {execution_date}")

    if not os.path.exists(source_path) and not source_path.startswith("/Volumes"):
        logger.error(f"Source path does not exist: {source_path}")
        return

    # Read incoming payload
    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )

    record_count = raw_df.count()
    logger.info(f"Read {record_count} records from {source_path}")

    # Add ingestion audit metadata
    df_with_metadata = (
        raw_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(os.path.basename(source_path)))
        .withColumn("_ingestion_date", lit(execution_date))
    )

    # Output target path inside Volume
    target_path = f"{base_landing_volume}/raw_partitioned/{source_name}/ingestion_date={execution_date}"

    # Write out data (overwrite target date directory to maintain idempotency)
    (
        df_with_metadata.write
        .mode("overwrite")
        .option("header", "true")
        .csv(target_path)
    )

    logger.info(f"Successfully wrote {record_count} records to {target_path}")


def main():
    spark = get_spark_session()
    
    # Unity Catalog Volume base path
    base_volume = "/Volumes/catalog_dev/fintech_payments/landing_volume"

    # Capture execution date (can be overridden via system argument)
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
    else:
        execution_date = datetime.now().strftime("%Y-%m-%d")

    # Mapping of raw uploaded CSV sources
    source_configs = [
        ("raw_transactions", f"{base_volume}/raw_transactions/transactions.csv"),
        ("raw_switch_log", f"{base_volume}/raw_switch_log/switch_log.csv"),
        ("raw_settlements", f"{base_volume}/raw_settlements/settlements.csv"),
        ("raw_merchants", f"{base_volume}/raw_merchants/merchants.csv"),
        ("raw_tickets", f"{base_volume}/raw_tickets/tickets.csv")
    ]

    for source_name, source_path in source_configs:
        ingest_raw_source(
            spark=spark,
            source_name=source_name,
            source_path=source_path,
            base_landing_volume=base_volume,
            execution_date=execution_date
        )

    logger.info("Landing batch ingestion job completed successfully.")


if __name__ == "__main__":
    main()