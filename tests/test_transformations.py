import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from src.transformations import clean_transactions, calculate_merchant_fees


@pytest.fixture(scope="session")
def spark():
    """Initializes a local PySpark session for running unit tests."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("FintechUnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def test_clean_transactions_filters_negative_amounts_and_nulls(spark):
    # Explicit schema matching bronze_transactions structure
    schema = StructType([
        StructField("txn_ref", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("rail", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_code", StringType(), True),
        StructField("initiated_at", StringType(), True),
        StructField("authorised_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("attempt_count", IntegerType(), True),
        StructField("final_response_code", StringType(), True),
        StructField("rail_latency_ms", IntegerType(), True),
        StructField("payer_ref", StringType(), True),
        StructField("settled_flag", StringType(), True),
    ])

    data = [
        ("TX1001", "MER-001", "ECOCASH_MM", "USD", 100.50, "SUCCESS", None, "2026-03-01 10:00:00", None, None, 1, "00", 120, "PAY1", "Y"),
        ("TX1002", "MER-001", "ZIPIT_BANK", "USD", -50.00, "FAILED", "ERR1", "2026-03-01 10:05:00", None, None, 1, "05", 300, "PAY2", "N"),   # Invalid amount (<0)
        (None, "MER-002", "VISA_CARD", "USD", 200.00, "SUCCESS", None, "2026-03-01 10:10:00", None, None, 1, "00", 150, "PAY3", "Y"),         # Null txn_ref
        ("TX1001", "MER-001", "ECOCASH_MM", "USD", 100.50, "SUCCESS", None, "2026-03-01 10:00:00", None, None, 1, "00", 120, "PAY1", "Y")    # Duplicate
    ]

    df = spark.createDataFrame(data, schema=schema)

    # Execute transformation
    cleaned_df = clean_transactions(df)
    results = cleaned_df.collect()

    # Assertions: Expecting only 1 valid row (TX1001)
    assert len(results) == 1
    assert results[0]["txn_ref"] == "TX1001"
    assert float(results[0]["amount"]) == 100.50


def test_calculate_merchant_fees_flat_percent(spark):
    # Transactions schema
    txns_schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])
    txns_data = [("MER-001", "USD", 100.00, "SUCCESS")]
    txns_df = spark.createDataFrame(txns_data, schema=txns_schema)

    # Merchants schema (SCD2 format)
    merchants_schema = StructType([
        StructField("merchant_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("fee_basis", StringType(), True),
        StructField("rate_percent", DoubleType(), True),
        StructField("flat_fee", DoubleType(), True),
        StructField("min_fee", DoubleType(), True),
        StructField("cap_fee", DoubleType(), True),
        StructField("__END_AT", StringType(), True),
    ])
    merchants_data = [("MER-001", "USD", "FLAT_PERCENT", 2.0, None, 0.50, None, None)]
    merchants_df = spark.createDataFrame(merchants_data, schema=merchants_schema)

    # Execute transformation
    result_df = calculate_merchant_fees(txns_df, merchants_df)
    row = result_df.collect()[0]

    # Assertion: 2% of $100 = $2.00
    assert float(row["calculated_fee"]) == 2.00