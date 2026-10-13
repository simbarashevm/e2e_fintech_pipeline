from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, to_date, to_timestamp,
    when, coalesce, lit, round
)


def clean_transactions(df: DataFrame) -> DataFrame:
    """
    Cleans raw transactions: casts types, creates derived date columns,
    and deduplicates by transaction reference.
    """
    return (
        df.select(
            col("txn_ref").cast("string"),
            col("merchant_id").cast("string"),
            col("rail").cast("string"),
            col("currency").cast("string"),
            col("amount").cast("decimal(18,2)"),
            col("status").cast("string"),
            col("error_code").cast("string"),
            to_timestamp(col("initiated_at")).alias("initiated_at"),
            to_timestamp(col("authorised_at")).alias("authorised_at"),
            to_timestamp(col("updated_at")).alias("updated_at"),
            col("attempt_count").cast("int"),
            col("final_response_code").cast("string"),
            col("rail_latency_ms").cast("int"),
            col("payer_ref").cast("string"),
            col("settled_flag").cast("string"),
            to_date(col("initiated_at")).alias("initiated_date")
        )
        .filter(col("txn_ref").isNotNull() & (col("amount") > 0))
        .dropDuplicates(["txn_ref"])
    )


def calculate_merchant_fees(txns_df: DataFrame, merchants_df: DataFrame) -> DataFrame:
    """
    Applies commercial fee schedule rules (FLAT_FEE, FLAT_PERCENT, PERCENT_WITH_CAP, TIERED_PERCENT)
    to calculate platform fees per successful transaction.
    """
    active_merchants = merchants_df.filter(col("__END_AT").isNull())

    return (
        txns_df.join(
            active_merchants,
            on=["merchant_id", "currency"],
            how="left"
        )
        .withColumn(
            "calculated_fee",
            when(col("status") == "SUCCESS",
                when(col("fee_basis") == "FLAT_FEE", coalesce(col("flat_fee"), lit(0)))
                .when(
                    col("fee_basis") == "FLAT_PERCENT",
                    when(col("amount") * (coalesce(col("rate_percent"), lit(0)) / 100) < coalesce(col("min_fee"), lit(0)), coalesce(col("min_fee"), lit(0)))
                    .otherwise(col("amount") * (coalesce(col("rate_percent"), lit(0)) / 100))
                )
                .when(
                    col("fee_basis") == "PERCENT_WITH_CAP",
                    when(col("amount") * (coalesce(col("rate_percent"), lit(0)) / 100) > coalesce(col("cap_fee"), lit(9999999)), coalesce(col("cap_fee"), lit(9999999)))
                    .when(col("amount") * (coalesce(col("rate_percent"), lit(0)) / 100) < coalesce(col("min_fee"), lit(0)), coalesce(col("min_fee"), lit(0)))
                    .otherwise(col("amount") * (coalesce(col("rate_percent"), lit(0)) / 100))
                )
                .when(
                    col("fee_basis") == "TIERED_PERCENT",
                    col("amount") * (coalesce(col("tier_rate_percent"), col("rate_percent"), lit(0)) / 100)
                )
                .otherwise(lit(0))
            ).otherwise(lit(0))
        )
    )