"""
BKK FUTAR GTFS-realtime producer.

Lekérdezi a VehiclePositions és TripUpdates végpontokat, dekódolja a
protobuf választ, és JSON formában beküldi a megfelelő Kafka topicokba.

Adatok forrása: BKK Zrt., CC BY 4.0

Környezeti változók (a docker/.env-ből vagy exportálva):
    BKK_API_KEY              - kötelező, a saját BKK API kulcsod
    KAFKA_BOOTSTRAP_SERVERS  - alapértelmezett: localhost:9092
    POLL_INTERVAL_SECONDS    - alapértelmezett: 15
"""

import json
import os
import time

import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

# --- Config ---
BKK_API_KEY = os.environ.get("BKK_API_KEY")
if not BKK_API_KEY:
    raise RuntimeError("A BKK_API_KEY error with the env var")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS=localhost:29092")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))

VEHICLE_POSITIONS_URL = (
    "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb"
    f"?key={BKK_API_KEY}"
)
TRIP_UPDATES_URL = (
    "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb"
    f"?key={BKK_API_KEY}"
)

VEHICLE_POSITIONS_TOPIC = "bkk-vehicle-positions"
TRIP_UPDATES_TOPIC = "bkk-trip-updates"


def fetch_feed(url):
    """Ask and decodethe GTFS-realtime feed protobuf's answer."""
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def vehicle_entity_to_dict(entity, feed_timestamp):
    """Egy VehiclePositions FeedEntity-t converting to send to kafka"""
    vehicle = entity.vehicle

    record = {
        "entity_id": entity.id,
        "feed_timestamp": feed_timestamp,
        "trip_id": vehicle.trip.trip_id if vehicle.HasField("trip") else None,
        "route_id": vehicle.trip.route_id if vehicle.HasField("trip") else None,
        "start_date": vehicle.trip.start_date if vehicle.HasField("trip") else None,
        "schedule_relationship": (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                vehicle.trip.schedule_relationship
            )
            if vehicle.HasField("trip")
            else None
        ),
        "latitude": vehicle.position.latitude if vehicle.HasField("position") else None,
        "longitude": vehicle.position.longitude if vehicle.HasField("position") else None,
        "bearing": vehicle.position.bearing if vehicle.HasField("position") else None,
        "speed": vehicle.position.speed if vehicle.HasField("position") else None,
        "current_stop_sequence": vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None,
        "current_status": (
            gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
            if vehicle.HasField("current_status")
            else None
        ),
        "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else None,
        "vehicle_timestamp": vehicle.timestamp if vehicle.HasField("timestamp") else None,
        "vehicle_id": vehicle.vehicle.id if vehicle.HasField("vehicle") else None,
        "vehicle_label": vehicle.vehicle.label if vehicle.HasField("vehicle") else None,
        "license_plate": vehicle.vehicle.license_plate if vehicle.HasField("vehicle") else None,
    }
    return record


def trip_update_to_records(entity, feed_timestamp):
    """Egy TripUpdate FeedEntity minden stop_time_update-jét külön rekorddá alakítja."""
    trip_update = entity.trip_update
    records = []

    for stu in trip_update.stop_time_update:
        record = {
            "entity_id": entity.id,
            "feed_timestamp": feed_timestamp,
            "trip_id": trip_update.trip.trip_id if trip_update.HasField("trip") else None,
            "route_id": trip_update.trip.route_id if trip_update.HasField("trip") else None,
            "start_date": trip_update.trip.start_date if trip_update.HasField("trip") else None,
            "schedule_relationship": (
                gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                    trip_update.trip.schedule_relationship
                )
                if trip_update.HasField("trip")
                else None
            ),
            "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
            "stop_id": stu.stop_id if stu.HasField("stop_id") else None,
            "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
            "arrival_uncertainty": stu.arrival.uncertainty if stu.HasField("arrival") else None,
            "departure_time": stu.departure.time if stu.HasField("departure") else None,
        }
        records.append(record)

    return records


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producer elindult. Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Lekérdezési gyakoriság: {POLL_INTERVAL_SECONDS} másodperc\n")

    while True:
        cycle_start = time.time()

        # --- VehiclePositions ---
        try:
            vp_feed = fetch_feed(VEHICLE_POSITIONS_URL)
            vp_timestamp = vp_feed.header.timestamp
            vp_count = 0

            for entity in vp_feed.entity:
                if entity.HasField("vehicle"):
                    record = vehicle_entity_to_dict(entity, vp_timestamp)
                    producer.send(VEHICLE_POSITIONS_TOPIC, value=record)
                    vp_count += 1

            print(f"[{time.strftime('%H:%M:%S')}] VehiclePositions: {vp_count} jármű elküldve.")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Hiba VehiclePositions lekérésekor: {e}")

        # --- TripUpdates ---
        try:
            tu_feed = fetch_feed(TRIP_UPDATES_URL)
            tu_timestamp = tu_feed.header.timestamp
            tu_count = 0

            for entity in tu_feed.entity:
                if entity.HasField("trip_update"):
                    for record in trip_update_to_records(entity, tu_timestamp):
                        producer.send(TRIP_UPDATES_TOPIC, value=record)
                        tu_count += 1

            print(f"[{time.strftime('%H:%M:%S')}] TripUpdates: {tu_count} megálló-frissítés elküldve.")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Hiba TripUpdates lekérésekor: {e}")

        producer.flush()

        elapsed = time.time() - cycle_start
        sleep_time = max(0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()