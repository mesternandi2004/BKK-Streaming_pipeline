"""
Static GTFS betöltő script.
Feltölti a dim_route, dim_stop, dim_trip, dim_date táblákat
a letöltött budapest_gtfs.zip CSV fájljaiból.

Futtatás: python load_static_gtfs.py
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

DB_USER = os.environ.get("POSTGRES_USER", "nandi")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "changeme")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB", "bkk_transit")
GTFS_DIR = os.path.expanduser("~/gtfs_static")

LOAD_NUMBER = datetime.now().strftime("%Y%m%d_%H%M%S")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")


def load_routes():
    df = pd.read_csv(f"{GTFS_DIR}/routes.txt")
    cols = ["route_id", "route_short_name", "route_long_name", "route_type", "route_desc", "route_color"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_route"))
        df.to_sql("dim_route", conn, if_exists="append", index=False)

    print(f"dim_route: {len(df)} sor betöltve")
    return len(df)


def load_stops():
    df = pd.read_csv(f"{GTFS_DIR}/stops.txt")
    cols = ["stop_id", "stop_name", "stop_lat", "stop_lon", "stop_code", "wheelchair_boarding"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_stop_delay"))
        conn.execute(text("DELETE FROM dim_stop"))
        df.to_sql("dim_stop", conn, if_exists="append", index=False)

    print(f"dim_stop: {len(df)} sor betöltve")
    return len(df)


def load_trips():
    df = pd.read_csv(f"{GTFS_DIR}/trips.txt")
    cols = ["route_id", "trip_id", "service_id", "trip_headsign", "direction_id",
            "shape_id", "wheelchair_accessible", "bikes_allowed"]
    df = df[cols].copy()
    df["load_number"] = LOAD_NUMBER

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_trip"))
        df.to_sql("dim_trip", conn, if_exists="append", index=False)

    print(f"dim_trip: {len(df)} sor betöltve")
    return len(df)


def load_dim_date(days_back=30, days_forward=60):
    today = datetime.now().date()
    start = today - timedelta(days=days_back)
    end = today + timedelta(days=days_forward)

    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"date_id": dates.date})
    df["day_of_week"] = dates.dayofweek
    df["is_weekend"] = dates.dayofweek >= 5
    df["is_holiday"] = False

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_stop_delay"))
        conn.execute(text("DELETE FROM dim_date"))
        df.to_sql("dim_date", conn, if_exists="append", index=False)

    print(f"dim_date: {len(df)} sor betöltve")
    return len(df)


def log_audit(layer, status, row_count, start_time, end_time):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit_pipeline_runs (load_number, layer, status, row_count, start_time, end_time)
            VALUES (:load_number, :layer, :status, :row_count, :start_time, :end_time)
        """), {
            "load_number": LOAD_NUMBER, "layer": layer, "status": status,
            "row_count": row_count, "start_time": start_time, "end_time": end_time,
        })


def main():
    print(f"Static GTFS betöltés indul. Load number: {LOAD_NUMBER}\n")

    steps = [
        ("dim_date", load_dim_date),
        ("dim_route", load_routes),
        ("dim_stop", load_stops),
        ("dim_trip", load_trips),
    ]

    for name, func in steps:
        start = datetime.now()
        try:
            count = func()
            log_audit(name, "SUCCESS", count, start, datetime.now())
        except Exception as e:
            log_audit(name, "FAILED", 0, start, datetime.now())
            print(f"HIBA a(z) {name} betöltésekor: {e}")
            raise

    print("\nMinden dimenzió tábla sikeresen betöltve!")


if __name__ == "__main__":
    main()