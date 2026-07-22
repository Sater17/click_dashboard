import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when, to_timestamp, expr, year
from pyspark.sql.types import StructType, StructField, StringType
import pyspark

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'clickstream-raw'

# Resolve absolute paths in workspace
WORKSPACE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_STREAM_DIR = os.path.join(WORKSPACE_DIR, "data", "raw_stream")
CLEAN_TABLE_PATH = os.path.join(WORKSPACE_DIR, "data", "processed_delta", "clean_events")
DLQ_TABLE_PATH = os.path.join(WORKSPACE_DIR, "data", "processed_delta", "quarantine_dlq")
CHECKPOINT_PATH = os.path.join(WORKSPACE_DIR, "data", "checkpoints", "clickstream")

def create_spark_session(mode):
    """Create Spark session. Only loads Kafka connector jar if running in Kafka mode."""
    # Configure HADOOP_HOME to workspace and prepend workspace bin to PATH to load the compatible local hadoop.dll
    if sys.platform.startswith("win"):
        os.environ["HADOOP_HOME"] = WORKSPACE_DIR
        path_dirs = os.environ.get("PATH", "").split(os.path.pathsep)
        clean_path_dirs = [d for d in path_dirs if "hadoop" not in d.lower()]
        local_bin = os.path.join(WORKSPACE_DIR, "bin")
        os.environ["PATH"] = os.path.pathsep.join([local_bin] + clean_path_dirs)

    spark_version = pyspark.__version__
    print(f"--- Detected local PySpark Version: {spark_version} ---")
    
    packages = []
    
    # Auto-detecting packages based on major.minor version of PySpark
    if spark_version.startswith("4."):
        delta_package = "io.delta:delta-spark_2.13:4.0.0"
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    elif spark_version.startswith("3.5"):
        delta_package = "io.delta:delta-spark_2.12:3.0.0"
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    elif spark_version.startswith("3.4"):
        delta_package = "io.delta:delta-core_2.12:2.4.0"
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    elif spark_version.startswith("3.3"):
        delta_package = "io.delta:delta-core_2.12:2.2.0"
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    else:
        delta_package = "io.delta:delta-spark_2.13:4.0.0"
        kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
        
    packages.append(delta_package)
    if mode == "kafka":
        packages.append(kafka_package)
        
    print(f"Loading Packages: {', '.join(packages)}")
    
    builder = SparkSession.builder \
        .appName(f"ClickstreamResilientPipeline-{mode.upper()}") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "2")
    
    if sys.platform.startswith("win"):
        os.environ["hadoop.home.dir"] = "C:\\winutils" if os.path.exists("C:\\winutils") else WORKSPACE_DIR
        
    return builder.getOrCreate()

def main():
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Clickstream Pipeline")
    parser.add_argument("--mode", choices=["kafka", "file"], default="file", 
                        help="Streaming source: 'kafka' uses Apache Kafka topics, 'file' uses a local directory folder.")
    args = parser.parse_args()
    
    spark = create_spark_session(args.mode)
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"\nStarting PySpark Structured Streaming Pipeline in [{args.mode.upper()}] mode...")
    print(f"Clean Target path: {CLEAN_TABLE_PATH}")
    print(f"DLQ Target path:   {DLQ_TABLE_PATH}")
    print(f"Checkpoint path:   {CHECKPOINT_PATH}\n")
    
    # Define Clickstream Schema (with columnNameOfCorruptRecord)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])
    
    # Read stream depending on mode
    if args.mode == "file":
        # Create directory first if it doesn't exist
        os.makedirs(RAW_STREAM_DIR, exist_ok=True)
        # Read text lines from raw_stream folder to simulate Kafka string records
        raw_stream = spark.readStream \
            .format("text") \
            .load(RAW_STREAM_DIR)
        raw_payload_df = raw_stream.select(col("value").alias("raw_payload"))
    else:
        # Read stream from Kafka
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        raw_payload_df = kafka_stream.selectExpr("CAST(value AS STRING) as raw_payload")
        
    # Parse JSON payload
    parsed_df = raw_payload_df.withColumn(
        "parsed_event", 
        from_json(col("raw_payload"), schema, {"columnNameOfCorruptRecord": "_corrupt_record"})
    )
    
    # Apply Data Quality Rules & Flag Reject Reason
    analyzed_df = parsed_df.withColumn(
        "rejection_reason",
        when(
            col("parsed_event._corrupt_record").isNotNull(), 
            "Malformed JSON payload (parse error)"
        ).when(
            col("parsed_event.event_id").isNull() | (col("parsed_event.event_id") == ""),
            "Missing critical field: event_id"
        ).when(
            col("parsed_event.user_id").isNull() | (col("parsed_event.user_id") == ""),
            "Missing critical field: user_id"
        ).when(
            to_timestamp(col("parsed_event.timestamp")).isNull(),
            "Invalid timestamp format"
        ).when(
            year(to_timestamp(col("parsed_event.timestamp"))) > 2030,
            "Future/Unrealistic timestamp (Year > 2030)"
        ).otherwise(None)
    )
    
    # Define ForeachBatch processing function
    def write_to_sinks(batch_df, batch_id):
        # Cache batch dataframe to optimize dual-write performance
        batch_df.persist()
        
        try:
            # 1. Clean Data Sink
            clean_df = batch_df.filter(col("rejection_reason").isNull()) \
                .select(
                    col("parsed_event.event_id").alias("event_id"),
                    col("parsed_event.user_id").alias("user_id"),
                    col("parsed_event.session_id").alias("session_id"),
                    to_timestamp(col("parsed_event.timestamp")).alias("timestamp"),
                    col("parsed_event.event_type").alias("event_type"),
                    col("parsed_event.page_url").alias("page_url"),
                    col("parsed_event.referrer").alias("referrer"),
                    col("parsed_event.ip_address").alias("ip_address"),
                    col("parsed_event.device_type").alias("device_type"),
                    col("parsed_event.os").alias("os"),
                    col("parsed_event.browser").alias("browser"),
                    current_timestamp().alias("processed_at")
                )
            
            clean_count = clean_df.count()
            
            # 2. Dead Letter Queue (DLQ / Quarantine) Sink
            quarantine_df = batch_df.filter(col("rejection_reason").isNotNull()) \
                .select(
                    col("raw_payload").alias("raw_payload"),
                    col("rejection_reason").alias("rejection_reason"),
                    current_timestamp().alias("quarantined_at")
                )
            
            quarantine_count = quarantine_df.count()
            
            print(f"[Batch {batch_id}] Stream stats: {clean_count} clean, {quarantine_count} quarantined")
            
            # Save clean events as Delta table
            if clean_count > 0:
                clean_df.write \
                    .format("delta") \
                    .mode("append") \
                    .save(CLEAN_TABLE_PATH)
                    
            # Save quarantine records as Delta table
            if quarantine_count > 0:
                quarantine_df.write \
                    .format("delta") \
                    .mode("append") \
                    .save(DLQ_TABLE_PATH)
                    
        except Exception as e:
            print(f"Error executing batch {batch_id}: {e}")
            raise e
        finally:
            batch_df.unpersist()
            
    # Write streaming DataFrame to sinks using foreachBatch
    query = analyzed_df.writeStream \
        .foreachBatch(write_to_sinks) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()
        
    # Wait for the stream to terminate
    query.awaitTermination()

if __name__ == '__main__':
    main()
