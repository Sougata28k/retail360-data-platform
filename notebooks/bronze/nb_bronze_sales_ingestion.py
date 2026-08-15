from pyspark.sql.functions import *
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