"""
Bronze layer: TripUpdates Kafka topic -> Delta Lake.

STREAMING job: always running, micro-batch processing from kafka topic
in every 15 seconds after that it writes the raw data to the bronze layer in Delta file format 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# --- 1. Creating SparkSession  ---
spark = (
    SparkSession.builder
    .appName("BronzeTripUpdates")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --- 2. Scheme definition ---
trip_updates_schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("feed_timestamp", LongType(), True),
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("schedule_relationship", StringType(), True),
    StructField("stop_sequence", IntegerType(), True),
    StructField("stop_id", StringType(), True),
    StructField("arrival_time", LongType(), True),
    StructField("arrival_uncertainty", IntegerType(), True),
    StructField("departure_time", LongType(), True)
])

# --- 3. Reading from Kafka ---
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bkk-trip-updates")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# --- 4. The Kafka messages processing ---
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), trip_updates_schema).alias("data"))
    .select("data.*")
)

# --- 5. Add a metadata column for auditing purposes ---
enriched_df = parsed_df.withColumn("ingested_at", current_timestamp())

# --- 6. Write to Delta format (streaming sink) ---
query = (
    enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/data-lake/checkpoints/bronze_trip_updates")
    .trigger(processingTime="2 minutes")
    .start("/opt/data-lake/bronze/trip_updates")
)

# --- 7. Always running this job ---
query.awaitTermination()