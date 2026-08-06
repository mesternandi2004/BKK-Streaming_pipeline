from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = (
        SparkSession.builder.appName("SilverDimensionsPipeline")
        .master("local[*]")
        .config("spark.jars.ivy", "/home/azureuser/.ivy2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read Bronze static data
    stop_df = spark.read.format("parquet").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/dim_stop.parquet")
    route_df = spark.read.format("parquet").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/dim_route.parquet")
    trip_df = spark.read.format("parquet").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/dim_trip.parquet")
    
    # Read vehicle positions to extract vehicle dimension
    vhc_positions_df = spark.read.format("delta").load("/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze/vehicle_positions")
    
    clean_vhc_positions_df = vhc_positions_df.filter(
        (F.col("route_id").isNotNull()) & (F.trim(F.col("route_id")) != "") &
        (F.col("trip_id").isNotNull()) & (F.trim(F.col("trip_id")) != "") &
        (F.col("stop_id").isNotNull()) & (F.trim(F.col("stop_id")) != "")
    )
    
    # Create the new vehicle dimension
    vehicle_df = clean_vhc_positions_df.select("vehicle_id", "vehicle_label", "license_plate").dropDuplicates()

    # Write dimensions to Silver layer (overwrite mode is fine for periodic updates)
    route_df.write.mode("overwrite").parquet("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/route")
    stop_df.write.mode("overwrite").parquet("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/stop")
    trip_df.write.mode("overwrite").parquet("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/trip")
    vehicle_df.write.mode("overwrite").parquet("/home/azureuser/BKK-Streaming_pipeline/data-lake/silver/vehicle")
    
    print("Silver dimensions successfully processed and written.")
    spark.stop()

if __name__ == "__main__":
    main()