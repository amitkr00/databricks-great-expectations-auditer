# Databricks + Great Expectations: Duplicates & 24‑Hour Freshness

This project shows how I use Great Expectations (GX) with Databricks to validate two common data quality claims:

- “We catch every duplicate.”
- “No record older than 24 hours reaches the dashboard.”


---

## Architecture

**Platform**

- Databricks Free Edition (serverless, Unity Catalog).
- DBFS root is disabled, so data lives in Unity Catalog tables and metadata can live in a UC Volume if needed.


**Data Flow**

- **Bronze** – `events_raw`  
  Raw events with intentional issues:
  - duplicates
  - very old events
  - borderline 24‑hour events
  - nulls
  - a future timestamp

- **Silver** – `events_clean`  
  Deduplicated by `event_id`, keeping the latest `ingest_timestamp`.

- **Gold** – `events_dashboard`  
  Only events from the last 24 hours by `event_timestamp`. This is the “dashboard” table.

**Schema (all tables)**

- `event_id` (string)
- `user_id` (string, nullable)
- `event_timestamp` (timestamp, business/event time)
- `ingest_timestamp` (timestamp, ingest time)

---

#Inside one notebook, it:

1. **Creates sample event data**  
   - Raw table `events_raw` with:
     - duplicates  
     - very old events  
     - borderline 24‑hour events  
     - null `user_id`  
     - a future `event_timestamp`

2. **Builds a simple lakehouse pipeline**  
   - **Silver** `events_clean`: deduplicated by `event_id` using window + `row_number`, keeping the latest `ingest_timestamp`.  
   - **Gold** `events_dashboard`: only events from the last 24 hours based on `event_timestamp`.

3. **Runs Great Expectations checks (GX 1.17, via Pandas)**  
   - **Duplicates suite** on `events_clean`:
     - `expect_column_values_to_not_be_null("event_id")`
     - `expect_column_values_to_be_unique("event_id")`
   - **Freshness suite** on `events_dashboard`:
     - `expect_column_values_to_not_be_null("event_timestamp")`
     - `expect_column_min_to_be_between("event_timestamp", min_value=now-24h)`

All GX validations are run through `PandasExecutionEngine + Validator` on Pandas DataFrames so they work cleanly on Databricks serverless.

---
![](![path](![path](path)))

## Databricks Setup

Example SQL setup for catalog/schema (run once):

```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.demo;

USE CATALOG main;
USE demo;
```

Raw table creation (Bronze) is in `01_ingest_bronze.py` as a simple `saveAsTable("events_raw")`.  
Silver and Gold tables are written similarly to `events_clean` and `events_dashboard`.

---

## Great Expectations Checks

Databricks Free Edition serverless has restrictions around Spark‑side GX integration, so validations are done via **PandasExecutionEngine + Validator** on Pandas DataFrames.

### 1. Duplicates on `events_clean`

Goal: prove “we catch every duplicate” on the Silver table.

```python
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator

df_clean_pd = spark.table("events_clean").toPandas()

engine = PandasExecutionEngine()
batch = Batch(data=df_clean_pd)
validator = Validator(execution_engine=engine, batches=[batch])

validator.expect_column_values_to_not_be_null("event_id")
validator.expect_column_values_to_be_unique("event_id")

results = validator.validate()
assert results.success
```

### 2. 24‑Hour Freshness on `events_dashboard`

Goal: prove “no record older than 24 hours reaches the dashboard” by asserting that the **minimum** `event_timestamp` is within the last 24 hours.

```python
from datetime import datetime, timedelta
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator

df_dash_pd = spark.table("events_dashboard").toPandas()

engine = PandasExecutionEngine()
batch = Batch(data=df_dash_pd)
validator = Validator(execution_engine=engine, batches=[batch])

validator.expect_column_values_to_not_be_null("event_timestamp")

now_ts = datetime.utcnow()
threshold_ts = now_ts - timedelta(hours=24)

validator.expect_column_min_to_be_between(
    column="event_timestamp",
    min_value=threshold_ts,
    max_value=None,
)

results = validator.validate()
assert results.success
```

---

## How this maps to real‑world claims

- **“We catch every duplicate”**  
  - The Silver‑layer dedup logic is explicit (window + `row_number`).
  - GX asserts non‑null and uniqueness on `event_id` in `events_clean`.

- **“No record older than 24 hours reaches the dashboard”**  
  - The Gold transform applies a 24‑hour filter on `event_timestamp`.
  - GX separately checks that the **minimum** `event_timestamp` in `events_dashboard` is `>= now − 24 hours`.

Together, the pipeline logic + GX expectations provide an auditable config to compare directly against those claims.