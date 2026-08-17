# ============================================================
# US101 - SALES BRONZE INGESTION
# ============================================================
#
# Purpose:
# Ingest POS sales transaction files into the Retail360
# Bronze layer and maintain ingestion audit information.
#
# Latest Updates:
# - Added unique RUN_ID for execution tracking
# - Added source system and ingestion metadata
# - Added file-level idempotency check
# - Added data quality validation and quarantine handling
# - Added Bronze append-only ingestion
# - Added audit logging with record counts and timestamps
#
# Processing Flow:
# Source File
#      ↓
# Bronze Delta Table
#      ↓
# Data Quality Validation
#      ↓
# Quarantine Invalid Records
#      ↓
# Audit Load
#
# ============================================================
"""from pyspark.sql.functions import *
from pyspark.sql.types import *


# Start Bronze ingestion process
print("Starting US101 Sales Bronze Ingestion")

# Source file configuration
source_file = "sales_transactions_20260808.csv"

file_path = (
    "/Volumes/retail360_dev/raw/retail360_raw/"
    f"{source_file}"
)

print(f"Reading source file: {source_file}")


# Define source schema
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])


# Read source CSV file
df_sales = (
    spark.read
    .option("header", True)
    .schema(sales_schema)
    .csv(file_path)
)


# Add ingestion metadata
df_bronze = (
    df_sales
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file_name", lit(source_file))
    .withColumn("load_date", current_date())
)


# Identify records that fail basic data quality checks
reject_df = (
    df_bronze
    .filter(
        col("transaction_id").isNull()
        | col("store_id").isNull()
        | (col("quantity") <= 0)
        | (col("sale_amount") <= 0)
    )
)


# Filter valid records
valid_df = (
    df_bronze
    .filter(
        col("transaction_id").isNotNull()
        & col("store_id").isNotNull()
        & (col("quantity") > 0)
        & (col("sale_amount") > 0)
    )
)


# Create required schemas
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS retail360_dev.bronze
""")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS retail360_dev.audit
""")


# Write valid records to Bronze table
(
    valid_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "retail360_dev.bronze.sales_transactions"
    )
)


# Write rejected records to Bronze rejects table
(
    reject_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "retail360_dev.bronze.sales_transactions_rejects"
    )
)


# Generate record counts for audit
source_count = df_sales.count()
valid_count = valid_df.count()
reject_count = reject_df.count()


# Create audit record
audit_df = spark.createDataFrame(
    [
        (
            "US101_SALES_BRONZE_LOAD",
            source_file,
            source_count,
            valid_count,
            reject_count,
            "SUCCESS"
        )
    ],
    [
        "job_name",
        "source_file",
        "source_count",
        "target_count",
        "reject_count",
        "status"
    ]
)


# Write audit record
(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "retail360_dev.audit.sales_load_audit"
    )
)


# Load summary
print("US101 Sales Bronze Load Completed Successfully")
print(f"Source Records : {source_count}")
print(f"Valid Records  : {valid_count}")
print(f"Rejected       : {reject_count}")
"""


import uuid

from pyspark.sql.functions import *
from pyspark.sql.types import *


# ============================================================
# 1. JOB CONFIGURATION
# ============================================================

JOB_NAME = "US101_SALES_BRONZE_LOAD"
SOURCE_SYSTEM = "POS"
SOURCE_FILE = "sales_transactions_20260809.csv"

SOURCE_PATH = (
    "/Volumes/retail360_dev/raw/retail360_raw/"
    f"{SOURCE_FILE}"
)

BRONZE_TABLE = (
    "retail360_dev.bronze.sales_transactions"
)

QUARANTINE_TABLE = (
    "retail360_dev.bronze.sales_transactions_quarantine"
)

AUDIT_TABLE = (
    "retail360_dev.audit.sales_load_audit"
)

# Generate a unique identifier for this execution
RUN_ID = str(uuid.uuid4())


print("=" * 70)
print("Starting Bronze Ingestion")
print("=" * 70)
print(f"Job        : {JOB_NAME}")
print(f"Run ID     : {RUN_ID}")
print(f"Source     : {SOURCE_FILE}")
print(f"Source Sys : {SOURCE_SYSTEM}")


# ============================================================
# 2. CREATE REQUIRED SCHEMAS
# ============================================================

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS retail360_dev.bronze
""")

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS retail360_dev.audit
""")


# ============================================================
# 3. CREATE AUDIT TABLE
#
# The audit table is created before the idempotency check
# so that the pipeline can safely query it on the first run.
# ============================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail360_dev.audit.sales_load_audit (
        run_id STRING,
        job_name STRING,
        source_system STRING,
        source_file STRING,
        source_count INT,
        bronze_count INT,
        quarantine_count INT,
        status STRING,
        start_timestamp TIMESTAMP,
        end_timestamp TIMESTAMP
    )
    USING DELTA
""")


# ============================================================
# 4. DEFINE SOURCE SCHEMA
# ============================================================

sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])


# ============================================================
# 5. CHECK WHETHER FILE WAS ALREADY PROCESSED
#
# Prevents the same source file from being loaded twice.
# ============================================================

file_already_processed = (
    spark.table(AUDIT_TABLE)
    .filter(
        (col("source_file") == SOURCE_FILE)
        & (col("status") == "SUCCESS")
    )
    .limit(1)
    .count()
    > 0
)


if file_already_processed:

    print(
        f"File '{SOURCE_FILE}' has already been successfully processed."
    )

    print("Skipping ingestion to maintain idempotency.")


else:

    # ========================================================
    # 6. RECORD JOB START TIME
    # ========================================================

    job_start_time = current_timestamp()


    # ========================================================
    # 7. READ SOURCE FILE
    # ========================================================

    print(f"Reading source file: {SOURCE_PATH}")

    df_source = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(sales_schema)
        .csv(SOURCE_PATH)
    )


    # ========================================================
    # 8. ADD INGESTION METADATA
    # ========================================================

    df_bronze = (
        df_source
        .withColumn("run_id", lit(RUN_ID))
        .withColumn("source_system", lit(SOURCE_SYSTEM))
        .withColumn("source_file_name", lit(SOURCE_FILE))
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "load_date",
            current_date()
        )
    )


    # ========================================================
    # 9. DATA QUALITY RULES
    #
    # Bronze keeps all successfully parsed source records.
    # Records failing these checks are also written to the
    # quarantine table for investigation.
    # ========================================================

    reject_condition = (
        col("transaction_id").isNull()
        | col("store_id").isNull()
        | col("product_id").isNull()
        | col("quantity").isNull()
        | (col("quantity") <= 0)
        | col("sale_amount").isNull()
        | (col("sale_amount") < 0)
        | col("transaction_date").isNull()
    )


    # ========================================================
    # 10. CREATE QUARANTINE DATA
    # ========================================================

    quarantine_df = (
        df_bronze
        .filter(reject_condition)
        .withColumn(
            "reject_reason",
            when(
                col("transaction_id").isNull(),
                lit("MISSING_TRANSACTION_ID")
            )
            .when(
                col("store_id").isNull(),
                lit("MISSING_STORE_ID")
            )
            .when(
                col("product_id").isNull(),
                lit("MISSING_PRODUCT_ID")
            )
            .when(
                col("quantity").isNull(),
                lit("MISSING_QUANTITY")
            )
            .when(
                col("quantity") <= 0,
                lit("INVALID_QUANTITY")
            )
            .when(
                col("sale_amount").isNull(),
                lit("MISSING_SALE_AMOUNT")
            )
            .when(
                col("sale_amount") < 0,
                lit("INVALID_SALE_AMOUNT")
            )
            .when(
                col("transaction_date").isNull(),
                lit("MISSING_TRANSACTION_DATE")
            )
            .otherwise(
                lit("UNKNOWN_VALIDATION_ERROR")
            )
        )
        .withColumn(
            "rejected_timestamp",
            current_timestamp()
        )
    )


    # ========================================================
    # 11. RECORD COUNTS
    # ========================================================

    source_count = df_source.count()
    bronze_count = df_bronze.count()
    quarantine_count = quarantine_df.count()


    # ========================================================
    # 12. WRITE BRONZE TABLE
    #
    # Bronze is append-only so previous ingestion history
    # is not overwritten.
    # ========================================================

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )


    # ========================================================
    # 13. WRITE QUARANTINE TABLE
    # ========================================================

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )


    # ========================================================
    # 14. CREATE AUDIT RECORD
    # ========================================================

    audit_df = spark.createDataFrame(
        [
            (
                RUN_ID,
                JOB_NAME,
                SOURCE_SYSTEM,
                SOURCE_FILE,
                source_count,
                bronze_count,
                quarantine_count,
                "SUCCESS"
            )
        ],
        [
            "run_id",
            "job_name",
            "source_system",
            "source_file",
            "source_count",
            "bronze_count",
            "quarantine_count",
            "status"
        ]
    ).withColumn(
        "start_timestamp",
        job_start_time
    ).withColumn(
        "end_timestamp",
        current_timestamp()
    )


    # ========================================================
    # 15. WRITE AUDIT RECORD
    # ========================================================

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


    # ========================================================
    # 16. LOAD SUMMARY
    # ========================================================

    print("=" * 70)
    print("BRONZE INGESTION COMPLETED")
    print("=" * 70)

    print(f"Run ID           : {RUN_ID}")
    print(f"Source File      : {SOURCE_FILE}")
    print(f"Source Records   : {source_count}")
    print(f"Bronze Records   : {bronze_count}")
    print(f"Quarantine       : {quarantine_count}")
    print("Status           : SUCCESS")
    print("=" * 70)