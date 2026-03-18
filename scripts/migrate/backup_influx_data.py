import csv
import gzip
import io
import os
from datetime import UTC, datetime, timedelta
from time import perf_counter

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

from scripts.migrate.influx_client import get_influxdb_client_from_env

INFLUXDB_ORG = "robinklaassen"
INFLUXDB_BUCKET = "live-api-collector"
INFLUXDB_MEASUREMENT = "train_locations"

FIRST_DATE = datetime(2022, 6, 7, tzinfo=UTC)
# FIRST_DATE = datetime(2025, 12, 18, tzinfo=UTC)
LAST_DATE = datetime.now(UTC)
JUST_FIRST_DATE = False

OUTPUT_DIR = "D:\\Data\\influx_train_locations"
MAX_RETRIES = 5


def construct_flux_query(start: datetime, end: datetime) -> str:
    # |> truncateTimeColumn(unit: 10s)
    # first day takes 20 seconds without time truncation and 216 seconds with (3.5 minutes).
    # much better to do the time truncation when loading the data (if ever) using polars
    return f"""
        from(bucket: "{INFLUXDB_BUCKET}")
        |> range(start: {start.isoformat()}, stop: {end.isoformat()})
        |> filter(fn: (r) => r["_measurement"] == "{INFLUXDB_MEASUREMENT}")
        |> pivot(columnKey: ["_field"], rowKey: ["train_id", "_time"], valueColumn: "_value")
        |> keep(columns: ["train_id", "train_type", "source", "_time", "lat", "lng", "speed", "direction", "accuracy"])
        |> filter(fn: (r) =>
            exists r.lat and exists r.lng
        )
        |> sort(columns: ["_time"])
        """


def backup_single_day(client: InfluxDBClient, start: datetime):
    """
    Backs up InfluxDB data as CSV for a single day, compresses it, and writes to disk.
    """
    begin_time = perf_counter()
    stop = start + timedelta(days=1)

    # Query data for the current day
    query = construct_flux_query(start, stop)
    query_api = client.query_api()
    result = query_api.query(query, params={"start": start.isoformat(), "stop": stop.isoformat()})

    # Convert the result to CSV format in memory
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Write header
    header = ["time", "source", "train_id", "train_type", "accuracy", "speed", "lat", "lng", "direction"]
    csv_writer.writerow(header)

    # Write data rows
    for table in result:
        for record in table.records:
            row = [
                record.get_time(),
                record.values.get("source", ""),
                record.values.get("train_id", ""),
                record.values.get("train_type", ""),
                record.values.get("accuracy", ""),
                record.values.get("speed", ""),
                record.values.get("lat", ""),
                record.values.get("lng", ""),
                record.values.get("direction", ""),
            ]
            csv_writer.writerow(row)

    # Compress the CSV data in memory
    csv_data = csv_buffer.getvalue().encode("utf-8")
    compressed_data = gzip.compress(csv_data)

    # Write the compressed data to disk
    filename = f"{INFLUXDB_MEASUREMENT}_{start.date()}.csv.gz"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "wb") as f:
        f.write(compressed_data)

    print(f"Backed up data for {start.date()} to {filepath} in {round(perf_counter() - begin_time, 2)} seconds")


def backup_influx_data_csv(client: InfluxDBClient):
    """
    Back up complete InfluxDB measurement from configured start and end date (see constants).
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    current_day = FIRST_DATE
    while current_day <= LAST_DATE:
        for attempt in range(MAX_RETRIES):
            try:
                backup_single_day(client, current_day)
                break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise  # Re-raise if all retries exhausted

        if JUST_FIRST_DATE:
            break

        current_day += timedelta(days=1)


if __name__ == "__main__":
    load_dotenv()
    client = get_influxdb_client_from_env()
    backup_influx_data_csv(client)
