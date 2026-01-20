import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, current_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ------------------------------------------------
# Environment setup for Windows
# ------------------------------------------------
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["SPARK_LOCAL_DIRS"] = "C:\\Temp"

# ------------------------------------------------
# Create a Spark Session C:\APACHE_SPARK\spark-3.5.7-bin-hadoop3\jars
# ------------------------------------------------
def create_spark_session():
    spark = SparkSession.builder \
        .appName("KafkaToPostgresScheduler") \
        .config("spark.jars", "C:\\APACHE_SPARK\\spark-3.5.7-bin-hadoop3\\jars\\postgresql-42.7.8.jar") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ------------------------------------------------
# Define JSON schema for Kafka messages
# ------------------------------------------------
schema = StructType([
    StructField("busline", StringType(), True),
    StructField("key", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("status", StringType(), True)
])

# ------------------------------------------------
# Function: Process Kafka data and write to PostgreSQL
# ------------------------------------------------
def process_kafka_batch(spark, batch_id):
    print(f"\nStarting batch job #{batch_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Read messages from Kafka
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "t2_test") \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING)")
    parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Aggregate data
    agg_df = parsed_df.groupBy("status").agg(
        count("*").alias("count"),
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude")
    )

    # Add batch metadata
    agg_df = agg_df \
        .withColumn("batch_id", monotonically_increasing_id() + batch_id * 1000) \
        .withColumn("batch_time", current_timestamp())

    # Show the output
    print("Aggregated Data:")
    agg_df.show(truncate=False)

    # Write to PostgreSQL
    agg_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/busdb") \
        .option("dbtable", "warehouse_bus_summary_batch") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"Batch #{batch_id} written to PostgreSQL at {datetime.now().strftime('%H:%M:%S')}")

# ------------------------------------------------
# Main Loop: Run job periodically
# ------------------------------------------------
if __name__ == "__main__":
    spark = create_spark_session()

    # Number of minutes between each batch
    INTERVAL_MINUTES = 2
    batch_counter = 1

    print(f"Starting Kafka PostgreSQL Scheduler (interval = {INTERVAL_MINUTES} min)\n")

    while True:
        process_kafka_batch(spark, batch_counter)
        batch_counter += 1
        print(f"Sleeping for {INTERVAL_MINUTES} minutes...\n")
        time.sleep(INTERVAL_MINUTES * 60)