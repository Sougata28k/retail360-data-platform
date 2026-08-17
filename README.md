# Retail360 Data Platform

Retail360 is an enterprise-style retail data engineering project
designed to ingest, validate, transform and serve retail data
using a modern cloud data platform architecture.

## Technology Stack

- Databricks
- PySpark
- Delta Lake
- ADLS Gen2
- GitHub
- Jira

## Architecture

Source Systems
    ↓
ADLS / Raw
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Analytics

## Current Implementation

### US101 - Sales Bronze Ingestion

- Explicit schema validation
- Data quality validation
- Quarantine handling
- Audit framework
- Idempotent ingestion
- Metadata enrichment
- Test cases
- Technical documentation

## Repository Structure

...

## Development Workflow

Feature Branch
→ Pull Request
→ Code Review
→ develop
→ QA
→ Production

## Roadmap

- Customer Bronze ingestion
- Product Bronze ingestion
- Silver transformations
- Incremental processing
- ADF orchestration
- CI/CD
- Monitoring
