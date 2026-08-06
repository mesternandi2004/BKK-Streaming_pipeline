"""
Static GTFS loader script for the Bronze Layer.
Reads the raw budapest_gtfs.zip CSV files and saves them directly
to the Delta Lake directory as Parquet files.

This can be scheduled via cron to run every 3 days.
"""

import os
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
# The location where you unzipped the BKK static GTFS files
GTFS_DIR = os.path.expanduser("~/gtfs_static")

# The location of our Data Lake Bronze layer
BRONZE_LAKE_DIR = "/home/azureuser/BKK-Streaming_pipeline/data-lake/bronze"
LOAD_NUMBER = datetime.now().strftime("%Y%m%d_%H%M%S")

def load_routes():
    df = pd.read_csv(f"{GTFS_DIR}/routes.txt")
    cols = ["route_id", "route_short_name", "route_long_name", "route_type", "route_desc", "route_color"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    # Write directly to the Lakehouse (Bronze) as Parquet

    output_path = f"{BRONZE_LAKE_DIR}/dim_route.parquet"
    df.to_parquet(output_path, index=False)
    
    print(f"dim_route: {len(df)} rows saved to {output_path}")

def load_stops():
    df = pd.read_csv(f"{GTFS_DIR}/stops.txt")
    cols = ["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_code", "wheelchair_boarding"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    output_path = f"{BRONZE_LAKE_DIR}/dim_stop.parquet"
    df.to_parquet(output_path, index=False)
    
    print(f"dim_stop: {len(df)} rows saved to {output_path}")

def load_trips():
    df = pd.read_csv(f"{GTFS_DIR}/trips.txt")
    cols = ["route_id", "trip_id", "service_id", "trip_headsign", "direction_id",
            "shape_id", "wheelchair_accessible", "bikes_allowed"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    output_path = f"{BRONZE_LAKE_DIR}/dim_trip.parquet"
    df.to_parquet(output_path, index=False)
    
    print(f"dim_trip: {len(df)} rows saved to {output_path}")

def main():
    print(f"Starting Static GTFS Bronze Loader. Load number: {LOAD_NUMBER}\n")
    
    # Create the bronze directory if it doesn't exist
    os.makedirs(BRONZE_LAKE_DIR, exist_ok=True)

    try:
        load_routes()
        load_stops()
        load_trips()
        print("\nAll dimension tables successfully saved to the Bronze Lakehouse!")
    except Exception as e:
        print(f"\nERROR during data loading: {e}")

if __name__ == "__main__":
    main()