from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = (
        SparkSession.builder.appName("SilverFactStreamingPipeline")
        .master("spark://localhost:7077")
        .config("spark.jars.ivy", "/home/azureuser/.ivy2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Bronze delta tables as Streams
    vhc_positions_df = (
        spark.readStream.format("delta")
        .load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/vehicle_positions")
    )
    
    trip_updates_df = (
        spark.readStream.format("delta")
        .load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/trip_updates")
    )

    # 2. Clean Vehicle Positions & cast timestamps to proper TimestampType if needed
    clean_vhc_positions_df = vhc_positions_df.filter(
        (F.col("route_id").isNotNull()) & (F.trim(F.col("route_id")) != "") &
        (F.col("trip_id").isNotNull()) & (F.trim(F.col("trip_id")) != "") &
        (F.col("stop_id").isNotNull()) & (F.trim(F.col("stop_id")) != "")
    ).drop("vehicle_label", "license_plate")

    # 3. Clean Trip Updates
    clean_trip_updates_df = trip_updates_df.filter(
        (F.col("route_id").isNotNull()) & (F.trim(F.col("route_id")) != "") &
        (F.col("trip_id").isNotNull()) & (F.trim(F.col("trip_id")) != "") &
        (F.col("stop_id").isNotNull()) & (F.trim(F.col("stop_id")) != "")
    )

    # 4. STREAM-STREAM JOIN CONFIGURATION (Watermarking + Time Window)
    # Spark requires a watermark for stream joins in order to clean up old data from memory.
    # We assume there is a 'vehicle_timestamp' and an 'ingested_at' column, or another event-time based field.

    # Add a watermark (e.g., 30 minutes tolerance for late-arriving data)
    vp_watermarked = clean_vhc_positions_df.withWatermark("vehicle_timestamp", "5 minutes")
    tu_watermarked = clean_trip_updates_df.withWatermark("ingested_at", "5 minutes")

    # 5. Fact table creation via Stream-Stream Join with time constraints
    fact_df = vp_watermarked.join(
        tu_watermarked,
        (vp_watermarked["trip_id"] == tu_watermarked["trip_id"])
        & (vp_watermarked["route_id"] == tu_watermarked["route_id"])
        & (vp_watermarked["stop_id"] == tu_watermarked["stop_id"])
        #Time-based constraint (time window), which is mandatory for stream-stream joins in Spark:
        #That is, the trip update can be at most 10 minutes earlier or later than the vehicle position.
        & (tu_watermarked["ingested_at"].between(
            vp_watermarked["vehicle_timestamp"] - F.expr("INTERVAL 10 minutes"),
            vp_watermarked["vehicle_timestamp"] + F.expr("INTERVAL 10 minutes")
        )),
        "left_outer", 
    ).select(
        vp_watermarked["trip_id"],
        vp_watermarked["route_id"],
        vp_watermarked["stop_id"],
        vp_watermarked["vehicle_id"],
        vp_watermarked["latitude"],
        vp_watermarked["longitude"],
        vp_watermarked["speed"],
        vp_watermarked["bearing"],
        vp_watermarked["current_status"],
        vp_watermarked["vehicle_timestamp"],
        tu_watermarked["arrival_time"],
        tu_watermarked["departure_time"],
        tu_watermarked["arrival_uncertainty"],
        tu_watermarked["ingested_at"],
    )

    # 6. Write Stream to Delta Fact Table
    query = (
        fact_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/_checkpoints/fact")
        .load("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/fact") 
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    main()