# import dlt
# from pyspark.sql.functions import current_timestamp, col

# BASE_LANDING_PATH = "/Volumes/catalog_dev/fintech_payments/landing_volume/raw_partitioned"
# CHECKPOINT_BASE = "/Volumes/catalog_dev/fintech_payments/landing_volume/_checkpoints"


# # ------------------------------------------------------------------
# # 1. BRONZE TRANSACTIONS
# # ------------------------------------------------------------------
# @dlt.table(
#     name="bronze_transactions",
#     comment="Raw streaming transactions ingested via Auto Loader."
# )
# def bronze_transactions():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option(
#             "cloudFiles.schemaLocation",
#             f"{CHECKPOINT_BASE}/schema_transactions"
#         )
#         .load(f"{BASE_LANDING_PATH}/raw_transactions/")
#         .withColumn("_dlt_ingested_at", current_timestamp())
#         .withColumn("_dlt_file_name", col("_metadata.file_path"))
#     )


# # ------------------------------------------------------------------
# # 2. BRONZE SWITCH LOG
# # ------------------------------------------------------------------
# @dlt.table(
#     name="bronze_switch_log",
#     comment="Raw streaming gateway switch logs ingested via Auto Loader."
# )
# def bronze_switch_log():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option(
#             "cloudFiles.schemaLocation",
#             f"{CHECKPOINT_BASE}/schema_switch_log"
#         )
#         .load(f"{BASE_LANDING_PATH}/raw_switch_log/")
#         .withColumn("_dlt_ingested_at", current_timestamp())
#         .withColumn("_dlt_file_name", col("_metadata.file_path"))
#     )


# # ------------------------------------------------------------------
# # 3. BRONZE SETTLEMENTS
# # ------------------------------------------------------------------
# @dlt.table(
#     name="bronze_settlements",
#     comment="Raw streaming settlement records ingested via Auto Loader."
# )
# def bronze_settlements():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option(
#             "cloudFiles.schemaLocation",
#             f"{CHECKPOINT_BASE}/schema_settlements"
#         )
#         .load(f"{BASE_LANDING_PATH}/raw_settlements/")
#         .withColumn("_dlt_ingested_at", current_timestamp())
#         .withColumn("_dlt_file_name", col("_metadata.file_path"))
#     )


# # ------------------------------------------------------------------
# # 4. BRONZE MERCHANTS
# # ------------------------------------------------------------------
# @dlt.table(
#     name="bronze_merchants",
#     comment="Raw merchant master profiles ingested via Auto Loader."
# )
# def bronze_merchants():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema_merchants")
#         .load(f"{BASE_LANDING_PATH}/raw_merchants/")
#         .withColumn("_dlt_ingested_at", current_timestamp())
#         .withColumn("_dlt_file_name", col("_metadata.file_path"))
#     )


# # ------------------------------------------------------------------
# # 5. BRONZE TICKETS
# # ------------------------------------------------------------------
# @dlt.table(
#     name="bronze_tickets",
#     comment="Raw dispute and support tickets ingested via Auto Loader."
# )
# def bronze_tickets():
#     return (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option(
#             "cloudFiles.schemaLocation",
#             f"{CHECKPOINT_BASE}/schema_tickets"
#         )
#         .load(f"{BASE_LANDING_PATH}/raw_tickets/")
#         .withColumn("_dlt_ingested_at", current_timestamp())
#         .withColumn("_dlt_file_name", col("_metadata.file_path"))
#     )




import dlt
from pyspark.sql.functions import (
    current_timestamp, col, to_date, to_timestamp, lit
)

BASE_LANDING_PATH = "/Volumes/catalog_dev/fintech_payments/landing_volume/raw_partitioned"
CHECKPOINT_BASE = "/Volumes/catalog_dev/fintech_payments/landing_volume/_checkpoints"


# ==================================================================
# PHASE 2 - DAY 4: BRONZE LAYER (STREAMING AUTO LOADER INGESTION)
# ==================================================================

# ------------------------------------------------------------------
# 1. BRONZE TRANSACTIONS
# ------------------------------------------------------------------
@dlt.table(
    name="bronze_transactions",
    comment="Raw streaming transactions ingested via Auto Loader."
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"{CHECKPOINT_BASE}/schema_transactions"
        )
        .load(f"{BASE_LANDING_PATH}/raw_transactions/")
        .withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_dlt_file_name", col("_metadata.file_path"))
    )


# ------------------------------------------------------------------
# 2. BRONZE SWITCH LOG
# ------------------------------------------------------------------
@dlt.table(
    name="bronze_switch_log",
    comment="Raw streaming gateway switch logs ingested via Auto Loader."
)
def bronze_switch_log():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"{CHECKPOINT_BASE}/schema_switch_log"
        )
        .load(f"{BASE_LANDING_PATH}/raw_switch_log/")
        .withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_dlt_file_name", col("_metadata.file_path"))
    )


# ------------------------------------------------------------------
# 3. BRONZE SETTLEMENTS
# ------------------------------------------------------------------
@dlt.table(
    name="bronze_settlements",
    comment="Raw streaming settlement records ingested via Auto Loader."
)
def bronze_settlements():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"{CHECKPOINT_BASE}/schema_settlements"
        )
        .load(f"{BASE_LANDING_PATH}/raw_settlements/")
        .withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_dlt_file_name", col("_metadata.file_path"))
    )


# ------------------------------------------------------------------
# 4. BRONZE MERCHANTS
# ------------------------------------------------------------------
@dlt.table(
    name="bronze_merchants",
    comment="Raw merchant master profiles ingested via Auto Loader."
)
def bronze_merchants():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema_merchants")
        .load(f"{BASE_LANDING_PATH}/raw_merchants/")
        .withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_dlt_file_name", col("_metadata.file_path"))
    )


# ------------------------------------------------------------------
# 5. BRONZE TICKETS
# ------------------------------------------------------------------
@dlt.table(
    name="bronze_tickets",
    comment="Raw dispute and support tickets ingested via Auto Loader."
)
def bronze_tickets():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            f"{CHECKPOINT_BASE}/schema_tickets"
        )
        .load(f"{BASE_LANDING_PATH}/raw_tickets/")
        .withColumn("_dlt_ingested_at", current_timestamp())
        .withColumn("_dlt_file_name", col("_metadata.file_path"))
    )


# ==================================================================
# PHASE 2 - DAY 5: SILVER LAYER (CLEANSING, TYPING & EXPECTATIONS)
# ==================================================================

# ------------------------------------------------------------------
# 6. SILVER TRANSACTIONS
# ------------------------------------------------------------------
@dlt.table(
    name="silver_transactions",
    comment="Cleansed transactions with enforced types and data quality expectations."
)
@dlt.expect_or_drop("valid_txn_ref", "txn_ref IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect("valid_currency", "currency IN ('USD', 'ZWG')")
def silver_transactions():
    return (
        dlt.read_stream("bronze_transactions")
        .select(
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
            to_date(col("initiated_at")).alias("initiated_date"),
            col("_dlt_ingested_at")
        )
        .dropDuplicates(["txn_ref"])
    )


# ------------------------------------------------------------------
# 7. SILVER SWITCH LOG
# ------------------------------------------------------------------
@dlt.table(
    name="silver_switch_log",
    comment="Cleansed switch logs tracking attempt latency and network responses."
)
@dlt.expect_or_drop("valid_switch_ref", "switch_ref IS NOT NULL")
@dlt.expect_or_drop("valid_txn_ref", "txn_ref IS NOT NULL")
def silver_switch_log():
    return (
        dlt.read_stream("bronze_switch_log")
        .select(
            col("switch_ref").cast("string"),
            col("txn_ref").cast("string"),
            col("rail").cast("string"),
            col("attempt_no").cast("int"),
            to_timestamp(col("submitted_at")).alias("submitted_at"),
            to_timestamp(col("responded_at")).alias("responded_at"),
            col("rail_response_code").cast("string"),
            col("rail_response_message").cast("string"),
            col("rail_reference").cast("string"),
            col("_dlt_ingested_at")
        )
        .dropDuplicates(["switch_ref"])
    )


# ------------------------------------------------------------------
# 8. SILVER SETTLEMENTS
# ------------------------------------------------------------------
@dlt.table(
    name="silver_settlements",
    comment="Cleansed bank and rail settlement lines."
)
@dlt.expect_or_drop("valid_batch_id", "settlement_batch_id IS NOT NULL")
@dlt.expect_or_drop("valid_rail_reference", "rail_reference IS NOT NULL")
def silver_settlements():
    return (
        dlt.read_stream("bronze_settlements")
        .select(
            col("settlement_batch_id").cast("string"),
            col("rail").cast("string"),
            to_date(col("value_date"), "dd/MM/yyyy").alias("value_date"),
            col("rail_reference").cast("string"),
            col("merchant_id").cast("string"),
            col("currency").cast("string"),
            col("gross_amount").cast("decimal(18,2)"),
            col("rail_charges").cast("decimal(18,2)"),
            col("net_amount").cast("decimal(18,2)"),
            col("line_type").cast("string"),
            col("_dlt_ingested_at")
        )
        .dropDuplicates(["settlement_batch_id", "rail_reference", "line_type"])
    )


# ==================================================================
# PHASE 2 - DAY 6: SCD TYPE 2 FOR MERCHANTS MASTER DATA
# ==================================================================

# ------------------------------------------------------------------
# 9. SILVER MERCHANTS (SCD TYPE 2)
# ------------------------------------------------------------------
dlt.create_streaming_table(
    name="silver_merchants_scd2",
    comment="SCD Type 2 history tracking merchant fee structures and pricing tiers."
)

dlt.apply_changes(
    target="silver_merchants_scd2",
    source="bronze_merchants",
    keys=["merchant_id", "currency"],
    sequence_by=col("effective_from"),
    stored_as_scd_type=2,
    track_history_column_list=[
        "merchant_name", "segment", "monthly_volume_band",
        "fee_basis", "rate_percent", "flat_fee", "min_fee",
        "cap_fee", "tier_threshold_count", "tier_rate_percent"
    ]
)


# ==================================================================
# PHASE 2 - DAY 7: RECONCILIATION & DEAD-LETTER QUEUE (DLQ)
# ==================================================================

# ------------------------------------------------------------------
# 10. SILVER RECONCILED TRANSACTIONS
# ------------------------------------------------------------------
@dlt.table(
    name="silver_reconciled_transactions",
    comment="Reconciled successful transactions matched against bank settlement batches."
)
def silver_reconciled_transactions():
    txns = dlt.read("silver_transactions").filter(col("status") == "SUCCESS")
    switch = dlt.read("silver_switch_log").select("txn_ref", "rail_reference").dropDuplicates(["txn_ref"])
    settlements = dlt.read("silver_settlements")

    txns_with_ref = txns.join(switch, on="txn_ref", how="inner")

    return (
        txns_with_ref.join(settlements, on=["rail_reference", "currency"], how="inner")
        .select(
            txns_with_ref["txn_ref"],
            txns_with_ref["merchant_id"],
            txns_with_ref["rail"],
            txns_with_ref["currency"],
            txns_with_ref["amount"].alias("txn_amount"),
            settlements["gross_amount"].alias("settled_gross_amount"),
            settlements["rail_charges"],
            settlements["net_amount"],
            settlements["settlement_batch_id"],
            txns_with_ref["authorised_at"],
            settlements["value_date"]
        )
    )


# ------------------------------------------------------------------
# 11. QUARANTINE UNRECONCILED SETTLEMENTS (DLQ)
# ------------------------------------------------------------------
@dlt.table(
    name="quarantine_unreconciled_settlements",
    comment="Dead-Letter Queue (DLQ) for settlement lines with no matching transaction."
)
def quarantine_unreconciled_settlements():
    txns = dlt.read("silver_transactions").filter(col("status") == "SUCCESS")
    switch = dlt.read("silver_switch_log").select("txn_ref", "rail_reference").dropDuplicates(["txn_ref"])
    settlements = dlt.read("silver_settlements")

    matched_refs = txns.join(switch, on="txn_ref", how="inner").select("rail_reference")

    return (
        settlements.join(matched_refs, on="rail_reference", how="left_anti")
        .withColumn("quarantine_reason", lit("UNMATCHED_SETTLEMENT_RAIL_REF"))
        .withColumn("quarantined_at", current_timestamp())
    )



# ==================================================================
# PHASE 3 - DAY 8: GOLD LAYER - DATA MART 1 (EXECUTIVE FINANCIALS)
# ==================================================================

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, sum, count, round, when, coalesce, lit
)

# ------------------------------------------------------------------
# 12. GOLD DAILY EXECUTIVE TPV & REVENUE SUMMARY
# ------------------------------------------------------------------
@dlt.materialized_view(
    name="gold_daily_executive_tpv_revenue",
    comment="Daily executive financial performance, TPV, fees, rail costs, net margin, and conversion rates by currency and merchant segment."
)
def gold_daily_executive_tpv_revenue():
    txns = dlt.read("silver_transactions")
    merchants = dlt.read("silver_merchants_scd2")
    settlements = dlt.read("silver_settlements")

    # 1. Filter current active merchant fee structures
    merchants_current = merchants.filter(col("__END_AT").isNull())

    # 2. Join transactions with merchant fee rules and compute fee per transaction
    txns_m = (
        txns.join(
            merchants_current,
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

    # 3. Aggregate daily transaction metrics
    daily_txns_kpi = (
        txns_m.groupBy("initiated_date", "currency", "segment")
        .agg(
            sum(when(col("status") == "SUCCESS", col("amount")).otherwise(0)).alias("gross_tpv"),
            count(when(col("status") == "SUCCESS", 1)).alias("successful_txns"),
            count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
            count(when(col("status") == "ABANDONED", 1)).alias("abandoned_txns"),
            count("*").alias("total_initiated_txns"),
            sum("calculated_fee").alias("gross_merchant_fees")
        )
    )

    # 4. Aggregate daily rail settlement charges
    daily_rail_charges = (
        settlements.groupBy(col("value_date").alias("initiated_date"), "currency")
        .agg(sum("rail_charges").alias("total_rail_charges"))
    )

    # 5. Combine KPIs and derived metrics
    return (
        daily_txns_kpi.join(
            daily_rail_charges,
            on=["initiated_date", "currency"],
            how="left"
        )
        .select(
            col("initiated_date"),
            col("currency"),
            coalesce(col("segment"), lit("UNKNOWN")).alias("merchant_segment"),
            round(col("gross_tpv"), 2).alias("gross_tpv"),
            col("successful_txns"),
            col("failed_txns"),
            col("abandoned_txns"),
            col("total_initiated_txns"),
            round(
                when(col("total_initiated_txns") > 0, (col("successful_txns") / col("total_initiated_txns")) * 100)
                .otherwise(0), 2
            ).alias("conversion_rate_pct"),
            round(col("gross_merchant_fees"), 2).alias("gross_merchant_fees"),
            round(coalesce(col("total_rail_charges"), lit(0)), 2).alias("rail_charges"),
            round(
                col("gross_merchant_fees") - coalesce(col("total_rail_charges"), lit(0)), 2
            ).alias("net_platform_margin"),
            current_timestamp().alias("_mart_updated_at")
        )
    )


# ==================================================================
# PHASE 3 - DAY 9: GOLD LAYER - DATA MART 2 (PAYMENT RAIL SLA)
# ==================================================================

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, sum, count, round, when, coalesce, lit, avg, expr
)

# ------------------------------------------------------------------
# 13. GOLD PAYMENT RAIL HEALTH & TECHNICAL SLA MONITOR
# ------------------------------------------------------------------
@dlt.materialized_view(
    name="gold_payment_rail_health_sla",
    comment="Daily payment rail health, authorization SLAs, average/p95 latency, and network decline reason breakdown."
)
def gold_payment_rail_health_sla():
    txns = dlt.read("silver_transactions")
    switch = dlt.read("silver_switch_log")

    # 1. Aggregate transaction counts and latencies by date, rail, and currency
    txn_sla = (
        txns.groupBy("initiated_date", "rail", "currency")
        .agg(
            count("*").alias("total_initiated_txns"),
            count(when(col("status") == "SUCCESS", 1)).alias("successful_txns"),
            count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
            count(when(col("status") == "ABANDONED", 1)).alias("abandoned_txns"),
            round(
                when(count("*") > 0, (count(when(col("status") == "SUCCESS", 1)) / count("*")) * 100).otherwise(0),
                2
            ).alias("authorization_success_rate_pct"),
            round(avg("rail_latency_ms"), 0).alias("avg_latency_ms"),
            round(expr("percentile_approx(rail_latency_ms, 0.95)"), 0).alias("p95_latency_ms")
        )
    )

    # 2. Join switch logs with transaction dates to capture error details
    switch_with_date = (
        switch.join(
            txns.select("txn_ref", "initiated_date", "currency"),
            on="txn_ref",
            how="inner"
        )
    )

    # 3. Aggregate network decline reasons from switch response messages
    switch_errors = (
        switch_with_date.filter(col("rail_response_message") != "APPROVED")
        .groupBy("initiated_date", "rail", "currency")
        .agg(
            count(when(col("rail_response_message") == "INSUFFICIENT_FUNDS", 1)).alias("insufficient_funds_count"),
            count(when(col("rail_response_message") == "ISSUER_UNAVAILABLE", 1)).alias("issuer_unavailable_count"),
            count(when(col("rail_response_message") == "DO_NOT_HONOUR", 1)).alias("do_not_honour_count"),
            count(when(col("rail_response_message") == "SYSTEM_MALFUNCTION", 1)).alias("system_malfunction_count"),
            count(when(col("rail_response_message") == "TRANSACTION_NOT_PERMITTED", 1)).alias("txn_not_permitted_count"),
            count(when(col("rail_response_message") == "INVALID_ACCOUNT", 1)).alias("invalid_account_count"),
            count(when(col("rail_response_message") == "EXPIRED_INSTRUMENT", 1)).alias("expired_instrument_count"),
            count("*").alias("total_switch_declines")
        )
    )

    # 4. Join SLA metrics with error breakdowns
    return (
        txn_sla.join(
            switch_errors,
            on=["initiated_date", "rail", "currency"],
            how="left"
        )
        .select(
            txn_sla["initiated_date"],
            txn_sla["rail"].alias("payment_rail"),
            txn_sla["currency"],
            txn_sla["total_initiated_txns"],
            txn_sla["successful_txns"],
            txn_sla["failed_txns"],
            txn_sla["abandoned_txns"],
            txn_sla["authorization_success_rate_pct"],
            txn_sla["avg_latency_ms"],
            txn_sla["p95_latency_ms"],
            coalesce(switch_errors["insufficient_funds_count"], lit(0)).alias("decline_insufficient_funds"),
            coalesce(switch_errors["issuer_unavailable_count"], lit(0)).alias("decline_issuer_unavailable"),
            coalesce(switch_errors["do_not_honour_count"], lit(0)).alias("decline_do_not_honour"),
            coalesce(switch_errors["system_malfunction_count"], lit(0)).alias("decline_system_malfunction"),
            coalesce(switch_errors["txn_not_permitted_count"], lit(0)).alias("decline_txn_not_permitted"),
            coalesce(switch_errors["invalid_account_count"], lit(0)).alias("decline_invalid_account"),
            coalesce(switch_errors["expired_instrument_count"], lit(0)).alias("decline_expired_instrument"),
            coalesce(switch_errors["total_switch_declines"], lit(0)).alias("total_switch_declines"),
            current_timestamp().alias("_mart_updated_at")
        )
    )