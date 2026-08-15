from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start the Bronze ingestion process
print("Starting US101 Sales Bronze Ingestion")

# Define the source CSV file
source_file = "sales_transactions_20260808.csv"

# Print the source file being read
print(f"Reading {source_file}")

# Define the schema for the sales transaction data
sales_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

# Confirm that the schema has been created successfully
print("Schema Created Successfully")

# Read the CSV file using the defined schema
df_sales = (
    spark.read.option("header", True)
    .schema(sales_schema)
    .csv(file_path)
)

# Display the raw sales data
display(df_sales)

# Add ingestion-related metadata columns to the Bronze data
df_bronze = (
    df_sales
    # Capture the timestamp when the data is ingested
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    # Store the name of the source file
    .withColumn(
        "source_file_name",
        lit("sales_transactions_20260808.csv")
    )
    # Store the date on which the data is loaded
    .withColumn(
        "load_date",
        current_date()
    )
)

# Display the Bronze data with ingestion metadata
display(df_bronze)

# Count the total number of records received from the source
source_count = df_sales.count()
print(source_count)

# Identify records that fail the basic data quality checks
# A record is rejected if:
# - transaction_id is NULL
# - store_id is NULL
# - quantity is less than or equal to 0
# - sale_amount is less than or equal to 0
reject_df = (
    df_bronze
    .filter(
        col("transaction_id").isNull()
        |
        col("store_id").isNull()
        |
        (col("quantity") <= 0)
        |
        (col("sale_amount") <= 0)
    )
)

# Display the rejected records
display(reject_df)

# Filter the records that pass the data quality checks
valid_df = (
    df_bronze
    .filter(
        col("transaction_id").isNotNull()
        |
        col("store_id").isNotNull()
        |
        (col("quantity") > 0)
        |
        (col("sale_amount") > 0)
    )
)

# Display the valid records
display(valid_df)