# Databricks Deployment Guide - Clickstream Streaming Pipeline

Since you want to implement this on **Databricks** using **PySpark**, this guide explains how to adapt the local Kafka-Spark pipeline for a cloud-based Databricks environment.

---

## 1. Connecting Databricks to Kafka

In a local setup, we use `localhost:9092`. In Databricks (which runs in the cloud), Kafka must be accessible over the internet (e.g., Confluent Cloud, AWS MSK, Azure Event Hubs).

Here is the production PySpark streaming code to read from a secured Kafka cluster (e.g., using SASL/PLAIN authentication):

```python
# Read streaming data from secured Cloud Kafka (e.g. Confluent Cloud)
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "YOUR_BOOTSTRAP_SERVER_URL:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<API_SECRET>';") \
    .option("subscribe", "clickstream-raw") \
    .option("startingOffsets", "latest") \
    .load()
```

> [!TIP]
> **Security Best Practice**: Never hardcode credentials (`API_KEY`, `API_SECRET`) in your Databricks Notebook. Use **Databricks Secrets Utility**:
> `password = dbutils.secrets.get(scope = "my-scope", key = "kafka-secret")`

---

## 2. Target Locations: DBFS vs. Unity Catalog

Instead of writing to local paths, you will write data to **Unity Catalog** tables or **DBFS** (Databricks File System) / Cloud Object Storage (S3 / Azure ADLS Gen2).

### Option A: Standard DBFS Paths (Delta format)
```python
CLEAN_TABLE_PATH = "dbfs:/mnt/clickstream/processed/clean_events"
DLQ_TABLE_PATH = "dbfs:/mnt/clickstream/processed/quarantine_dlq"
CHECKPOINT_PATH = "dbfs:/mnt/clickstream/checkpoints/clickstream_job"

# Write in foreachBatch (similar to local pipeline.py)
clean_df.write \
    .format("delta") \
    .mode("append") \
    .save(CLEAN_TABLE_PATH)
```

### Option B: Unity Catalog Tables (Recommended)
Unity Catalog allows you to write tables directly under a catalog and schema governance layer:
```python
CHECKPOINT_PATH = "dbfs:/mnt/clickstream/checkpoints/clickstream_job"

# Within foreachBatch function:
clean_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("main.clickstream.clean_events")

quarantine_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("main.clickstream.quarantine_dlq")
```

---

## 3. Alternative Ingestion: Databricks Auto Loader (`cloudFiles`)

If your clickstream data is dumped as JSON files into an S3 bucket or ADLS folder (instead of streaming through Kafka), Databricks provides a proprietary tool called **Auto Loader**. It is highly optimized for file streaming.

Instead of `spark.readStream.format("json")`, use:

```python
# Auto Loader with schema inference and schema evolution support
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/clickstream/schemas/raw_events") \
    .load("dbfs:/mnt/clickstream/landing_zone/raw_events/")
```

---

## 4. Modernizing with Delta Live Tables (DLT)

Databricks has a declarative pipeline tool called **Delta Live Tables (DLT)** which natively manages orchestration, checkpoints, and data quality (called *Expectations*).

If you build a DLT pipeline, the custom `foreachBatch` routing can be rewritten declaratively. DLT automatically manages the quarantine tables and rules check:

```python
import dlt
from pyspark.sql.functions import col, to_timestamp

# 1. Read Raw Streaming from Kafka/Landing Zone
@dlt.view
def raw_clickstream():
    return spark.readStream.format("kafka").option(...).load()

# 2. Bronze Table (All Raw Data ingested)
@dlt.table(
    name="bronze_clickstream",
    comment="Raw ingested clickstream events"
)
def bronze_clickstream():
    return raw_clickstream().selectExpr("CAST(value AS STRING) as raw_payload")

# 3. Silver Table: Clean Events (Valid Records only)
# DLT will check data quality and automatically drop invalid rows
@dlt.table(
    name="clean_events_silver",
    comment="Parsed and cleaned clickstream events"
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def clean_events_silver():
    # Parse and extract clean records
    ...
```

---

## 5. Visualizing on Databricks

Databricks has built-in visualization tools that replace the local Streamlit dashboard:

1. **Databricks SQL Dashboards**: 
   You can query your Delta tables (`main.clickstream.clean_events`) using SQL and create real-time charts (Bar, Line, Funnel) directly inside Databricks SQL interface.
2. **Lakeview Dashboards**:
   The newest WYSIWYG dashboard capability in Databricks. You can connect it directly to your Delta tables and build fast dashboards without maintaining any backend.
