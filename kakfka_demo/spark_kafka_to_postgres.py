from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg, count,
    to_json, struct, current_timestamp,
    to_timestamp                       # 🔁 CHANGED
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark setup (UNCHANGED)
spark = (
    SparkSession.builder
    .appName("KafkaBatchProcessor")
    .config("spark.driver.extraClassPath", "file:///C:/D_DRIVE/python_wrk_spc/python_practice/lib/postgresql-42.7.8.jar")
    .config("spark.executor.extraClassPath", "file:///C:/D_DRIVE/python_wrk_spc/python_practice/lib/postgresql-42.7.8.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("busline", StringType()),
    StructField("key", StringType()),
    StructField("timestamp", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("status", StringType())
])

# -------------------------------------------------------------------
# 🔁 CHANGED: Batch → Streaming
# -------------------------------------------------------------------
df = (
    spark.readStream                       # 🔁 CHANGED
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "t2_test")
    .option("startingOffsets", "latest")   # 🔁 CHANGED
    .load()
)

# -------------------------------------------------------------------
# 🔁 CHANGED: add event_time for watermark
# -------------------------------------------------------------------
parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
      .withColumn("event_time", to_timestamp("timestamp"))  # 🔁 CHANGED
)

# -------------------------------------------------------------------
# 🔁 CHANGED: aggregation with watermark
# -------------------------------------------------------------------
agg_df = (
    parsed
    .withWatermark("event_time", "5 minutes")               # 🔁 CHANGED
    .groupBy("status")
    .agg(
        count("*").alias("count"),
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude")
    )
    .withColumn("batch_time", current_timestamp())
)

# -------------------------------------------------------------------
# 🔁 CHANGED: JDBC write using foreachBatch
# -------------------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .mode("append") \
        .jdbc(
            "jdbc:postgresql://localhost:5432/busdb",
            "warehouse_bus_summary",
            properties={
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }
        )

pg_query = (
    agg_df.writeStream                     # 🔁 CHANGED
    .foreachBatch(write_to_postgres)       # 🔁 CHANGED
    .outputMode("update")                  # 🔁 CHANGED
    .option("checkpointLocation", "C:/tmp/spark_checkpoint/pg")
    .start()
)

# -------------------------------------------------------------------
# 🔁 CHANGED: Kafka sink as streaming write
# -------------------------------------------------------------------
agg_kafka_df = agg_df.select(to_json(struct("*")).alias("value"))

kafka_query = (
    agg_kafka_df.writeStream               # 🔁 CHANGED
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "t3_test")
    .option("checkpointLocation", "C:/tmp/spark_checkpoint/kafka")
    .outputMode("update")
    .start()
)

# -------------------------------------------------------------------
# 🔁 CHANGED: streaming jobs never call spark.stop()
# -------------------------------------------------------------------
spark.streams.awaitAnyTermination()

