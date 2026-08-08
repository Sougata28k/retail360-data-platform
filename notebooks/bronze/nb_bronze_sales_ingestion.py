from pyspark.sql.functions import *

print("Starting US101 Sales Bronze Ingestion")

source_file = "sales_transactions_20260808.csv"

print(f"Reading {source_file}")