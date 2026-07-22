from pipeline import create_spark_session

spark = create_spark_session("file")

df = spark.read.format("delta").load(
    r"C:/Users/SATER/Projects/streaming_click/data/processed_delta/clean_events"
)

print(df.select("user_id").distinct().count())