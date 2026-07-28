-- Dimenzió táblák

CREATE TABLE dim_route (
    route_id            TEXT PRIMARY KEY,
    route_short_name    TEXT,
    route_long_name     TEXT,
    route_type          INTEGER,
    route_desc          TEXT,
    route_color         TEXT,
    created_at          TIMESTAMP DEFAULT now(),
    load_number         TEXT
);

CREATE TABLE dim_stop (
    stop_id             TEXT PRIMARY KEY,
    stop_name           TEXT,
    stop_lat            DOUBLE PRECISION,
    stop_lon            DOUBLE PRECISION,
    stop_code           TEXT,
    wheelchair_boarding INTEGER,
    created_at          TIMESTAMP DEFAULT now(),
    load_number         TEXT
);

CREATE TABLE dim_trip (
    trip_id             TEXT PRIMARY KEY,
    route_id            TEXT REFERENCES dim_route(route_id),
    service_id          TEXT,
    trip_headsign       TEXT,
    direction_id        TEXT,
    shape_id            TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed       INTEGER,
    created_at          TIMESTAMP DEFAULT now(),
    load_number         TEXT
);

CREATE TABLE dim_vehicle (
    vehicle_id          TEXT PRIMARY KEY,
    license_plate       TEXT,
    vehicle_model       TEXT,
    wheelchair_accessible TEXT,
    created_at          TIMESTAMP DEFAULT now(),
    updated_at          TIMESTAMP DEFAULT now(),
    load_number         TEXT
);

CREATE TABLE dim_date (
    date_id             DATE PRIMARY KEY,
    day_of_week         INTEGER,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN
);

-- Fact tábla

CREATE TABLE fact_stop_delay (
    fact_id             BIGSERIAL PRIMARY KEY,
    vehicle_id          TEXT REFERENCES dim_vehicle(vehicle_id),
    route_id            TEXT REFERENCES dim_route(route_id),
    trip_id             TEXT REFERENCES dim_trip(trip_id),
    stop_id             TEXT REFERENCES dim_stop(stop_id),
    date_id             DATE REFERENCES dim_date(date_id),
    scheduled_arrival   TIMESTAMP,
    estimated_arrival   TIMESTAMP,
    actual_arrival      TIMESTAMP,
    delay_seconds       INTEGER,
    avg_speed_segment   DOUBLE PRECISION,
    weather_condition   TEXT,
    ingested_at         TIMESTAMP DEFAULT now(),
    load_number         TEXT
);

CREATE INDEX idx_fact_route ON fact_stop_delay(route_id);
CREATE INDEX idx_fact_date ON fact_stop_delay(date_id);

-- Audit tábla

CREATE TABLE audit_pipeline_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    load_number         TEXT,
    layer               TEXT,
    status              TEXT,
    row_count           INTEGER,
    start_time          TIMESTAMP,
    end_time            TIMESTAMP
);