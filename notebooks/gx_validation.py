# Databricks notebook source
# MAGIC %pip install great-expectations

# COMMAND ----------

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------



# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("USE demo")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

data = [
    ("e1", "u1", "2026-05-31T10:00:00", "2026-05-31T10:05:00"),
    ("e1", "u1", "2026-05-31T10:00:00", "2026-05-31T10:06:00"),  # duplicate
    ("e2", "u2", "2026-05-30T08:00:00", "2026-05-31T09:00:00"),  # >24h old by event_timestamp
]

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("ingest_timestamp", StringType(), False),
])

df_raw = (
    spark.createDataFrame(data, schema)
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("ingest_timestamp", F.to_timestamp("ingest_timestamp"))
)

df_raw.write.mode("overwrite").format("delta").saveAsTable("events_raw")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

df_raw = spark.table("demo.events_raw")

w = Window.partitionBy("event_id").orderBy(F.desc("ingest_timestamp"))

df_clean = (
    df_raw
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

df_clean.write.mode("overwrite").format("delta").saveAsTable("events_clean")

# COMMAND ----------

from pyspark.sql import functions as F

df_clean = spark.table("demo.events_clean")

df_gold = df_clean.filter(
    F.col("event_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
)

df_gold.write.mode("overwrite").format("delta").saveAsTable("events_dashboard")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Make sure we're in the right catalog & schema
spark.sql("USE CATALOG main")
spark.sql("USE demo")

data = [
    # Recent, clean events (should always pass)
    ("e1", "u1", "2026-05-31T10:00:00", "2026-05-31T10:05:00"),
    ("e2", "u2", "2026-05-31T10:10:00", "2026-05-31T10:11:00"),

    # Duplicate event_ids (same event_id, different ingest_timestamp)
    ("e3", "u3", "2026-05-31T09:30:00", "2026-05-31T09:35:00"),
    ("e3", "u3", "2026-05-31T09:30:00", "2026-05-31T09:36:00"),  # dup

    # Very old event by event_timestamp (> 24h)
    ("e4", "u4", "2026-05-29T08:00:00", "2026-05-31T09:00:00"),

    # Borderline 24h old – we can use this later to test exact boundary
    ("e5", "u5", "2026-05-30T10:59:59", "2026-05-31T11:00:00"),

    # Nulls in non-key column (user_id) – useful for other expectations later
    ("e6", None, "2026-05-31T07:00:00", "2026-05-31T07:05:00"),

    # Weird case: event in the future (clock skew, bad data)
    ("e7", "u7", "2026-06-02T12:00:00", "2026-05-31T10:00:00"),
]

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", StringType(), False),
    StructField("ingest_timestamp", StringType(), False),
])

df_raw = (
    spark.createDataFrame(data, schema)
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("ingest_timestamp", F.to_timestamp("ingest_timestamp"))
)

# Overwrite a managed Delta table in main.demo
df_raw.write.mode("overwrite").format("delta").saveAsTable("events_raw")

display(spark.table("events_raw"))
print("Row count:", spark.table("events_raw").count())

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.sql("USE CATALOG main")
spark.sql("USE demo")

df_raw = spark.table("events_raw")

w = Window.partitionBy("event_id").orderBy(F.desc("ingest_timestamp"))

df_clean = (
    df_raw
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")           # keep latest per event_id
    .drop("rn")
)

df_clean.write.mode("overwrite").format("delta").saveAsTable("events_clean")

display(spark.table("events_clean"))
print("Row count (clean):", spark.table("events_clean").count())
print("Distinct event_id:", spark.table("events_clean").select("event_id").distinct().count())

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE CATALOG main")
spark.sql("USE demo")

df_clean = spark.table("events_clean")

df_gold = df_clean.filter(
    F.col("event_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
)

df_gold.write.mode("overwrite").format("delta").saveAsTable("events_dashboard")

display(spark.table("events_dashboard"))
print("Row count (dashboard):", spark.table("events_dashboard").count())

# COMMAND ----------

# MAGIC %pip install great-expectations

# COMMAND ----------

import great_expectations as gx

# Use the UC volume instead of DBFS
context_root_dir = "/Volumes/main/demo/gx_meta/great_expectations"

context = gx.get_context(context_root_dir=context_root_dir)

print("Context root dir:", context_root_dir)
print("Data Context loaded OK")

# COMMAND ----------

import great_expectations as gx

spark.sql("USE CATALOG main")
spark.sql("USE demo")

context_root_dir = "/Volumes/main/demo/gx_meta/great_expectations"
context = gx.get_context(context_root_dir=context_root_dir)

# NOTE: using data_sources instead of sources
data_source = context.data_sources.add_spark(name="events_delta")

print("Created data source:", data_source.name)

# COMMAND ----------

import great_expectations as gx

print("GX version:", gx.__version__)
print("Has 'sources':", hasattr(gx.get_context(), "sources"))
print("Has 'data_sources':", hasattr(gx.get_context(), "data_sources"))
print("Has 'add_or_update_expectation_suite':", hasattr(gx.get_context(), "add_or_update_expectation_suite"))
print("Has 'create_expectation_suite':", hasattr(gx.get_context(), "create_expectation_suite"))

# COMMAND ----------

import great_expectations as gx
import inspect

spark.sql("USE CATALOG main")
spark.sql("USE demo")

context_root_dir = "/Volumes/main/demo/gx_meta/great_expectations"
context = gx.get_context(context_root_dir=context_root_dir)

print("get_validator signature:")
print(inspect.signature(context.get_validator))

help(context.get_validator)

# COMMAND ----------

print("Validation success:", results.success)
print("Evaluated expectations:", results.statistics["evaluated_expectations"])
print("Successful expectations:", results.statistics["successful_expectations"])

for r in results.results:
    # Just print the whole config; it's still clear enough for your writeup
    print("Config:", r.expectation_config)
    print("  success:", r.success)

# COMMAND ----------

for r in results.results:
    etype = getattr(r.expectation_config, "expectation_type", None)
    print("Expectation:", etype, "->", r.success)
    

# COMMAND ----------

import great_expectations as gx
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator
from datetime import datetime, timedelta

spark.sql("USE CATALOG main")
spark.sql("USE demo")

# 1) Get dashboard table into Pandas
df_dash_pd = spark.table("events_dashboard").toPandas()
print("Dashboard shape:", df_dash_pd.shape)
print(df_dash_pd)

# 2) Build Pandas execution engine and batch
execution_engine_dash = PandasExecutionEngine()
batch_dash = Batch(data=df_dash_pd)

validator_dash = Validator(
    execution_engine=execution_engine_dash,
    batches=[batch_dash],
)

# 3) Basic sanity: timestamp not null
validator_dash.expect_column_values_to_not_be_null("event_timestamp")

# 4) Freshness: no record older than 24 hours (by event_timestamp)
now_ts = datetime.utcnow()
threshold_ts = now_ts - timedelta(hours=24)
print("Now (UTC):", now_ts)
print("Threshold (UTC):", threshold_ts)

validator_dash.expect_column_min_to_be_between(
    column="event_timestamp",
    min_value=threshold_ts,
    max_value=None,  # we don't cap the future yet
)

# 5) Validate
results_dash = validator_dash.validate()

print("Dashboard validation success:", results_dash.success)
print("Evaluated expectations:", results_dash.statistics["evaluated_expectations"])
print("Successful expectations:", results_dash.statistics["successful_expectations"])

for r in results_dash.results:
    print("Config:", r.expectation_config)
    print("  success:", r.success)

# COMMAND ----------

import great_expectations as gx
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.core.batch import Batch
from great_expectations.validator.validator import Validator
from datetime import datetime, timedelta

spark.sql("USE CATALOG main")
spark.sql("USE demo")

# ---------- events_clean: duplicates check ----------

df_clean_pd = spark.table("events_clean").toPandas()

engine_clean = PandasExecutionEngine()
batch_clean = Batch(data=df_clean_pd)
validator_clean = Validator(execution_engine=engine_clean, batches=[batch_clean])

validator_clean.expect_column_values_to_not_be_null("event_id")
validator_clean.expect_column_values_to_be_unique("event_id")

results_clean = validator_clean.validate()

print("=== events_clean – duplicates suite ===")
print("Validation success:", results_clean.success)
for r in results_clean.results:
    print("  ", r.expectation_config, "->", r.success)


# ---------- events_dashboard: 24-hour freshness check ----------

df_dash_pd = spark.table("events_dashboard").toPandas()

engine_dash = PandasExecutionEngine()
batch_dash = Batch(data=df_dash_pd)
validator_dash = Validator(execution_engine=engine_dash, batches=[batch_dash])

validator_dash.expect_column_values_to_not_be_null("event_timestamp")

now_ts = datetime.utcnow()
threshold_ts = now_ts - timedelta(hours=24)

validator_dash.expect_column_min_to_be_between(
    column="event_timestamp",
    min_value=threshold_ts,
    max_value=None,
)

results_dash = validator_dash.validate()

print("=== events_dashboard – freshness suite ===")
print("Now (UTC):", now_ts)
print("Threshold (UTC):", threshold_ts)
print("Validation success:", results_dash.success)
for r in results_dash.results:
    print("  ", r.expectation_config, "->", r.success)

# COMMAND ----------


