# US101 – Sales Bronze Ingestion Test Cases

---


## TC001 – Valid Sales File

**Input File**
sales_valid.csv

**Scenario:**  
Validate ingestion of a sales file containing valid records.

**Expected Result:**  
The sales file should be successfully ingested into the Bronze table.

**Actual Result:**  
Sales file was successfully ingested into the Bronze table.

**Result:**  
Pass

-----------------------------------------------

## TC002 – Missing Transaction ID

**Input File**
sales_missing_txns.csv

**Scenario:**  
Validate handling of records where the Transaction ID is missing.

**Expected Result:**  
Records with a missing Transaction ID should be rejected and written to the quarantine table with the appropriate rejection reason.

**Actual Result:**  
Records with a missing Transaction ID were successfully written to the quarantine table.

**Result:**  
Pass

---

## TC003 – Audit Table Validation

**Input File**
sales_missing_txns.csv
sales_valid_txns.csv
etc

**Scenario:**  
Validate whether the ingestion execution details are captured in the Audit table.

**Expected Result:**  
A successful audit record should be created containing the run ID, source file, source record count, Bronze record count, quarantine count, status, and timestamps.

**Actual Result:**  
The ingestion execution details were successfully recorded in the Audit table.

**Result:**  
Pass

---

## TC004 – Negative Quantity

**Input File**
sales_invalid_quantity.csv

**Scenario:**  
Validate handling of records containing a negative quantity.

**Expected Result:**  
Records with a negative quantity should be rejected and written to the quarantine table with the rejection reason `INVALID_QUANTITY`.

**Actual Result:**  
Records with a negative quantity were successfully written to the quarantine table.

**Result:**  
Pass

---

## TC005 – Duplicate File / Idempotency

**Input File**
sales_valid.csv

**Scenario:**  
Validate ingestion behavior when the same source file is processed more than once.

**Expected Result:**  
If the source file has already been successfully processed, the subsequent ingestion should be skipped to prevent duplicate records.

**Actual Result:**  
The previously processed file was detected and ingestion was skipped.

**Result:**  
Pass

---

## TC006 – Multiple Data Quality Errors

**Input File**
sales_mul_err.csv

**Scenario:**  
Validate handling of a record containing multiple data quality errors, such as a missing Store ID and negative Sale Amount.

**Expected Result:**  
The record should be written to the quarantine table, and all applicable validation errors should be captured in the rejection reason.

**Actual Result:**  
The record was successfully quarantined and all applicable validation errors were captured.

**Result:**  
Pass

---

## TC007 – Literal NULL Transaction ID

**Input File**
sales_missing_txns_test2.csv

**Scenario:**  
Validate handling of records where the Transaction ID contains the literal string `NULL`.

**Expected Result:**  
Records containing a literal `NULL` Transaction ID should be rejected and written to the quarantine table with the rejection reason `MISSING_TRANSACTION_ID`.

**Actual Result:**  
Records containing a literal `NULL` Transaction ID were successfully quarantined.

**Result:**  
Pass

## TC008 – Missing Store ID

**Input File**
sales_missing_str.csv

**Scenario:**  
Validate handling of records where the Store ID is missing.

**Expected Result:**  
Records with a missing Store ID should be rejected and written to the quarantine table with the appropriate rejection reason.

**Actual Result:**  
Records with a missing Store ID were successfully written to the quarantine table.

**Result:**  
Pass

## TC009 – Missing Store ID

**Input File**
sales_missing_prd.csv

**Scenario:**  
Validate handling of records where the Product ID is missing.

**Expected Result:**  
Records with a missing Product ID should be rejected and written to the quarantine table with the appropriate rejection reason.

**Actual Result:**  
Records with a missing Product ID were successfully written to the quarantine table.

**Result:**  
Pass
