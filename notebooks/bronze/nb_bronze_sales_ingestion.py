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