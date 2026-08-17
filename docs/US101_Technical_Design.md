# US101 – Sales Bronze Ingestion
## Technical Design

**Objective:**  
Ingest POS sales transaction files into the Bronze layer while maintaining data
quality, auditability and idempotency.

**Source:**  
POS sales transaction CSV file  
`sales_transactions_20260808.csv`

**Targets:**  
- Bronze: `retail360_dev.bronze.sales_transactions`
- Quarantine: `retail360_dev.bronze.sales_transactions_quarantine`
- Audit: `retail360_dev.audit.sales_load_audit`

**Processing Flow:**  

Source CSV → Schema Validation → Metadata Enrichment → Data Quality Validation
→ Bronze Load → Quarantine Invalid Records → Audit Logging

The source CSV is read using an explicit schema to ensure the expected data
types are applied. Ingestion metadata such as Run ID, source system, source
file name, ingestion timestamp and load date are added to the Bronze records.

Data quality validations are applied to mandatory fields such as Transaction ID,
Store ID, Product ID, Quantity, Sale Amount and Transaction Date. Actual NULL,
empty, whitespace and literal `"NULL"` values are treated as missing values.
Quantity must be greater than zero and Sale Amount must not be negative.

All successfully parsed source records are retained in the Bronze table for
traceability. Records that fail the defined validation rules are additionally
written to the Quarantine table along with the applicable rejection reasons.
Multiple validation errors for the same record are captured.

The Audit table records the Run ID, source file, source count, Bronze count,
quarantine count, processing status and execution timestamps.

File-level idempotency is implemented using the Audit table. If the same source
file has already been successfully processed, the ingestion is skipped to
prevent duplicate loading.

**Implementation Completed:**

- Explicit schema validation
- Metadata enrichment
- Data quality validation
- Quarantine/reject record framework
- Bronze Delta table
- Quarantine table
- Audit table
- File-level idempotency
- Multiple validation error handling