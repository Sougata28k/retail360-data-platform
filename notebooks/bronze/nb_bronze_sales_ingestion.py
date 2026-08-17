# ============================================================
# US101 - SALES BRONZE INGESTION
# ============================================================
#
# Purpose:
# Ingest POS sales transaction files into the Bronze layer.
#
# Key capabilities:
# - Explicit source schema
# - Bronze ingestion metadata
# - NULL / empty / whitespace / literal "NULL" handling
# - Data quality validation
# - Quarantine of rejected records
# - Audit logging
# - File-level idempotency
# - Append-only Bronze storage
#
# ============================================================


# ============================================================
# 1. JOB CONFIGURATION
# ============================================================

import uuid

from pyspark.sql.functions import *
from pyspark.sql.types import *


JOB_NAME = "US101_SALES_BRONZE_LOAD"
SOURCE_SYSTEM = "POS"

SOURCE_FILE = "sales_missing_txns_test2.csv"

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


# Generate unique identifier for every execution
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
# File-level idempotency:
# If the same source file already has a SUCCESS audit record,
# the ingestion will be skipped.
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


# ============================================================
# 6. IDEMPOTENCY CONTROL
# ============================================================

if file_already_processed:

    print("=" * 70)
    print("INGESTION SKIPPED")
    print("=" * 70)

    print(
        f"File '{SOURCE_FILE}' has already been successfully processed."
    )

    print(
        "Skipping ingestion to maintain idempotency."
    )

    print("=" * 70)


else:

    # ========================================================
    # 7. RECORD JOB START TIME
    # ========================================================

    job_start_time = current_timestamp()


    # ========================================================
    # 8. READ SOURCE FILE
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
    # 9. ADD INGESTION METADATA
    # ========================================================

    df_bronze = (
        df_source
        .withColumn(
            "run_id",
            lit(RUN_ID)
        )
        .withColumn(
            "source_system",
            lit(SOURCE_SYSTEM)
        )
        .withColumn(
            "source_file_name",
            lit(SOURCE_FILE)
        )
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
    # 10. NORMALIZE STRING FIELDS FOR VALIDATION
    #
    # The following values are considered missing:
    #
    # 1. Actual NULL
    # 2. Empty string
    # 3. Whitespace-only string
    # 4. Literal "NULL"
    #
    # Examples:
    # "NULL"
    # "null"
    # " Null "
    # ""
    # "   "
    #
    # Bronze retains the original source values.
    # Normalized columns are used only for validation.
    # ========================================================

    def normalized_string(column_name):

        cleaned_value = trim(
            col(column_name)
        )

        return (
            when(
                cleaned_value.isNull()
                | (length(cleaned_value) == 0)
                | (lower(cleaned_value) == "null"),
                lit(None)
            )
            .otherwise(cleaned_value)
        )


    # ========================================================
    # 11. CREATE VALIDATION DATAFRAME
    # ========================================================

    df_validation = (
        df_bronze

        .withColumn(
            "_transaction_id",
            normalized_string("transaction_id")
        )

        .withColumn(
            "_store_id",
            normalized_string("store_id")
        )

        .withColumn(
            "_product_id",
            normalized_string("product_id")
        )

        .withColumn(
            "_customer_id",
            normalized_string("customer_id")
        )

        .withColumn(
            "_transaction_date",
            normalized_string("transaction_date")
        )
    )


    # ========================================================
    # 12. DATA QUALITY REJECTION RULES
    #
    # Records failing one or more mandatory validation rules
    # are sent to the quarantine table.
    #
    # Bronze still retains the original source record.
    # ========================================================

    reject_condition = (

        # Transaction ID validation
        col("_transaction_id").isNull()

        |

        # Store validation
        col("_store_id").isNull()

        |

        # Product validation
        col("_product_id").isNull()

        |

        # Quantity validation
        col("quantity").isNull()

        |

        (col("quantity") <= 0)

        |

        # Sales amount validation
        col("sale_amount").isNull()

        |

        (col("sale_amount") < 0)

        |

        # Transaction date validation
        col("_transaction_date").isNull()
    )


    # ========================================================
    # 13. CREATE QUARANTINE DATA
    #
    # Multiple validation failures are captured for the same
    # record instead of stopping at the first failure.
    #
    # Example:
    #
    # transaction_id = NULL
    # store_id       = NULL
    # quantity       = -1
    #
    # reject_reason:
    #
    # MISSING_TRANSACTION_ID;
    # MISSING_STORE_ID;
    # INVALID_QUANTITY
    # ========================================================

    quarantine_df = (
        df_validation

        .filter(
            reject_condition
        )

        .withColumn(
            "reject_reason",

            concat_ws(
                "; ",

                when(
                    col("_transaction_id").isNull(),
                    lit("MISSING_TRANSACTION_ID")
                ),

                when(
                    col("_store_id").isNull(),
                    lit("MISSING_STORE_ID")
                ),

                when(
                    col("_product_id").isNull(),
                    lit("MISSING_PRODUCT_ID")
                ),

                when(
                    col("quantity").isNull(),
                    lit("MISSING_QUANTITY")
                ),

                when(
                    col("quantity") <= 0,
                    lit("INVALID_QUANTITY")
                ),

                when(
                    col("sale_amount").isNull(),
                    lit("MISSING_SALE_AMOUNT")
                ),

                when(
                    col("sale_amount") < 0,
                    lit("INVALID_SALE_AMOUNT")
                ),

                when(
                    col("_transaction_date").isNull(),
                    lit("MISSING_TRANSACTION_DATE")
                )
            )
        )

        .withColumn(
            "rejected_timestamp",
            current_timestamp()
        )

        # Remove validation-only columns.
        # Original source values remain unchanged.
        .drop(
            "_transaction_id",
            "_store_id",
            "_product_id",
            "_customer_id",
            "_transaction_date"
        )
    )


    # ========================================================
    # 14. RECORD COUNTS
    # ========================================================

    source_count = df_source.count()

    bronze_count = df_bronze.count()

    quarantine_count = quarantine_df.count()


    # ========================================================
    # 15. WRITE BRONZE TABLE
    #
    # Bronze is append-only.
    #
    # All successfully parsed source records are retained in
    # Bronze, including records that fail data-quality rules.
    #
    # Rejected records are additionally copied to quarantine.
    # ========================================================

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE)
    )


    # ========================================================
    # 16. WRITE QUARANTINE TABLE
    #
    # Only records failing validation are written here.
    # ========================================================

    (
        quarantine_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )


    # ========================================================
    # 17. CREATE AUDIT RECORD
    # ========================================================

    audit_df = (
        spark.createDataFrame(
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
        )

        .withColumn(
            "start_timestamp",
            job_start_time
        )

        .withColumn(
            "end_timestamp",
            current_timestamp()
        )
    )


    # ========================================================
    # 18. WRITE AUDIT RECORD
    # ========================================================

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


    # ========================================================
    # 19. LOAD SUMMARY
    #
    # IMPORTANT:
    # This section is INSIDE the ELSE block.
    #
    # Therefore it will execute only when a new file is
    # actually processed.
    #
    # It will NOT execute when idempotency skips the file.
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