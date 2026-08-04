from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("SilverEnrichment")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


VP_df = spark.readStream \
    .format("delta") \
    .load("/opt/data-lake/bronze/vehicle_positions")

TU_df = spark.readStream \
    .format("delta") \
    .load("/opt/data-lake/bronze/trip_updates")

route_df = spark.read \
    .format("parquet") \
    .load("/opt/data-lake/bronze/dim_route.parquet")

trip_df = spark.read \
    .format("parquet") \
    .load("/opt/data-lake/bronze/dim_trip.parquet" )

stop_df = spark.read \
    .format("parquet") \
    .load("/opt/data-lake/bronze/dim_stop.parquet")


stop_df.show()