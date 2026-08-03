"""
Bronze layer: VehiclePositions Kafka topic -> Delta Lake.

STREAMING job: always running, mikro-batch procession from kafka topic
in every 2 minutes after that it writes the raw data to the bronze layer in Delta file format 

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, FloatType, IntegerType
)

# --- 1. Creating SparkSession  ---
# A "spark://spark-master:7077" the Spark container address 

spark = (
    SparkSession.builder
    .appName("BronzeVehiclePositions")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

# With the "WARN" setting It will only shows us the important INFOS 

spark.sparkContext.setLogLevel("WARN")

# --- 2. Scheme definition ---
# The kafka store the data values in bites. The Spark producer convert it to json format 
# and we need to create the scheme how we would like to store the datas in the delta lake
vehicle_schema = StructType([
    StructField("entity_id", StringType()),
    StructField("feed_timestamp", LongType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("start_date", StringType()),
    StructField("schedule_relationship", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("bearing", FloatType()),
    StructField("speed", FloatType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("current_status", StringType()),
    StructField("stop_id", StringType()),
    StructField("vehicle_timestamp", LongType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_label", StringType()),
    StructField("license_plate", StringType()),
])

# --- 3. Reading from Kafka ---
# kafka.bootstrap.servers: This job will run from the spark container
# thats why we need to define  the port
#
# startingOffsets: The point where the job needs to start processing
# It can start from the begining or from the checkpoint
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "bkk-vehicle-positions")
    .option("startingOffsets", "earliest")
    .load()
)

# --- 4. The Kafka messages processing ---
# Kafka row always like this scheme :
# key, value, topic, partition, offset, timestamp, timestampType
# We will only use the value. This is where the raw datas stored 
parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), vehicle_schema).alias("data"))
    .select("data.*")  #  "data" struct => different column
)

# --- 5. Write to Delta format (streaming sink) ---
#
# outputMode("append"): This is why it will only write out the new rows
#
# option("path", ...): Where to write in the Delta Lake directory
#
# option("checkpointLocation", ...): checkpoint 
#
# trigger(processingTime="2 minutes"): Data processing in every 2 minutes
query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", "/opt/data-lake/bronze/vehicle_positions")
    .option("checkpointLocation", "/opt/data-lake/checkpoints/bronze_vehicle_positions")
    .trigger(processingTime="2 minutes")
    .start()
)

# --- 6. Always running this job ---

query.awaitTermination()