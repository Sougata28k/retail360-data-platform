## Data Quality Rules

| Rule ID | Data Quality Rule | Action |
|---|---|---|
| DQ101 | `transaction_id` cannot be NULL, empty, whitespace, or literal `"NULL"` | Quarantine |
| DQ102 | `store_id` cannot be NULL, empty, whitespace, or literal `"NULL"` | Quarantine |
| DQ103 | `product_id` cannot be NULL, empty, whitespace, or literal `"NULL"` | Quarantine |
| DQ104 | `quantity` must be present and greater than 0 | Quarantine |
| DQ105 | `sale_amount` must be present and greater than or equal to 0 | Quarantine |
| DQ106 | `transaction_date` cannot be NULL, empty, whitespace, or literal `"NULL"` | Quarantine |