from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = (
        SparkSession.builder.appName("SilverFactPipeline")
        .master("spark://localhost:7077")  # <--- Itt a Master port!
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

    # Read Bronze delta tables
    vhc_positions_df = spark.readStream.format("delta").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/vehicle_positions")
    trip_updates_df = spark.readStream.format("delta").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/trip_updates")

    # Clean Vehicle Positions and drop attributes moved to dimension
    clean_vhc_positions_df = vhc_positions_df.filter(
        (F.col("route_id").isNotNull()) & (F.trim(F.col("route_id")) != "") &
        (F.col("trip_id").isNotNull()) & (F.trim(F.col("trip_id")) != "") &
        (F.col("stop_id").isNotNull()) & (F.trim(F.col("stop_id")) != "")
    ).drop("vehicle_label", "license_plate")

    # Clean Trip Updates
    clean_trip_updates_df = trip_updates_df.filter(
        (F.col("route_id").isNotNull()) & (F.trim(F.col("route_id")) != "") &
        (F.col("trip_id").isNotNull()) & (F.trim(F.col("trip_id")) != "") &
        (F.col("stop_id").isNotNull()) & (F.trim(F.col("stop_id")) != "")
    )

    # Create the fact table using the triple join condition
    fact_df = clean_vhc_positions_df.join(
        clean_trip_updates_df,
        (clean_vhc_positions_df["trip_id"] == clean_trip_updates_df["trip_id"])
        & (clean_vhc_positions_df["route_id"] == clean_trip_updates_df["route_id"])
        & (clean_vhc_positions_df["stop_id"] == clean_trip_updates_df["stop_id"]),
        "left_outer",
    ).select(
        clean_vhc_positions_df["trip_id"],
        clean_vhc_positions_df["route_id"],
        clean_vhc_positions_df["stop_id"],
        clean_vhc_positions_df["vehicle_id"],
        clean_vhc_positions_df["latitude"],
        clean_vhc_positions_df["longitude"],
        clean_vhc_positions_df["speed"],
        clean_vhc_positions_df["bearing"],
        clean_vhc_positions_df["current_status"],
        clean_vhc_positions_df["vehicle_timestamp"],
        clean_trip_updates_df["arrival_time"],
        clean_trip_updates_df["departure_time"],
        clean_trip_updates_df["arrival_uncertainty"],
        clean_trip_updates_df["ingested_at"],
    )

    # Append data to the Silver Fact Delta table
    fact_df.writeStream.format("delta").mode("append").save("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/fact")
    
    print("Silver fact table successfully processed and appended.")
    spark.stop()

if __name__ == "__main__":
    main()