# Mal Senior Data Engineer Assessment - Unified Payment Data Pipeline

A compact but production-minded Python pipeline that unifies **Cards**, **Transfers**, and **Bill Payments** into one canonical payment model.

This submission is designed to show:
- reusable platform thinking across product squads
- pragmatic data contract management
- clean validation and error handling
- downstream analytics readiness

## 1) What this project does

The pipeline:
1. reads 3 squad-owned CSV files with different schemas
2. maps them into one canonical payment event schema
3. validates mandatory fields and business rules
4. demonstrates a **v1 -> v2 data contract migration**
5. writes unified outputs to **Parquet** and **JSON**
6. stores invalid rows separately with clear error reasons

## 2) Canonical payment schema

Main fields in the unified model:
- `event_id`
- `contract_version`
- `payment_type`
- `customer_id`
- `amount`
- `currency`
- `event_timestamp`
- `status`
- `payment_method`
- `counterparty_reference`
- `merchant_name`
- `biller_name`
- `source_account_id`
- `destination_account_id`
- `card_last4`
- `channel`
- `country_code`
- `is_international`
- `raw_source`
- `ingestion_ts`

### Why this shape?
This keeps the model simple for downstream teams while still covering the three payment rails.
- shared columns are standardized once
- type-specific columns stay nullable instead of forcing separate downstream models
- audit fields (`raw_source`, `contract_version`, `ingestion_ts`) support lineage and migration safely

## 3) Contract versioning example

This repo includes a simple migration example:
- **v1**: older transfer records do not include `country_code`, `is_international`, or `channel`
- **v2**: adds those fields to support broader monitoring and analytics

The pipeline migrates transfer data from **v1 to v2** during ingestion.

This is realistic because product squads often move at different speeds. A shared platform should absorb that safely without breaking downstream users.

## 4) Project structure

```text
mal_payment_platform/
├── README.md
├── requirements.txt
├── data/
│   └── input/
│       ├── cards.csv
│       ├── transfers.csv
│       └── bill_payments.csv
├── output/
│   ├── unified_payments.parquet
│   ├── unified_payments.json
│   ├── run_summary.json
│   └── validation_errors.jsonl
└── src/
    ├── contracts.py
    ├── pipeline.py
    └── sql_queries.sql
```

## 5) Setup

### Local run
```bash
python -m venv .venv
source .venv/bin/activate   # Mac/Linux
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
python src/pipeline.py
```

## 6) Expected output

After running the pipeline, you should see:
- `output/unified_payments.parquet`
- `output/unified_payments.json`
- `output/run_summary.json`
- `output/validation_errors.jsonl`

Example run summary from the sample data:

```json
{
  "valid_record_count": 8,
  "error_count": 2,
  "by_payment_type": {
    "TRANSFER": 3,
    "CARD": 3,
    "BILL_PAYMENT": 2
  },
  "by_status": {
    "SUCCESS": 5,
    "FAILED": 2,
    "PENDING": 1
  }
}
```

## 7) Validation rules included

The pipeline rejects rows when:
- mandatory fields are missing
- `amount <= 0`
- currency is not in 3-letter ISO style, like `AED`
- `event_timestamp` is not valid ISO-8601
- `payment_type` is invalid
- `status` is outside the shared status set

Invalid records are not lost. They are written to `validation_errors.jsonl` with the exact reason.

## 8) Downstream SQL examples

SQL samples are included in:
- `src/sql_queries.sql`

These cover:
- daily payment volume by product
- failed payment follow-up
- international card spend monitoring
- biller collections view
- transfer rail analytics

## 9) Assumptions made

- Amount is stored in transaction currency for simplicity.
- A shared status model is used: `PENDING`, `SUCCESS`, `FAILED`, `REVERSED`.
- Nullability is allowed for rail-specific fields like `merchant_name`, `biller_name`, and `card_last4`.
- This submission focuses on batch ingestion for clarity, but the same mapping/validation logic can be reused in orchestrated jobs.

## 10) What I would add in production

For a real Mal rollout I would add:
- schema registry / contract approval workflow
- orchestration with Airflow or Dagster
- unit tests and CI checks
- warehouse load step (DuckDB / Postgres / Snowflake / BigQuery)
- observability (freshness, volume drift, failure alerts)
- PII masking rules and role-based access controls

## 11) Why this fits Mal

Mal is building an AI-native Islamic digital finance platform in the UAE, and a shared payments foundation matters because cards, transfers, and bill payments should look consistent to operations, analytics, risk, and customer-facing product teams. Mal also positions itself around ethical, transparent, and intelligent financial experiences, so contract clarity, lineage, and validation are important design choices here.
