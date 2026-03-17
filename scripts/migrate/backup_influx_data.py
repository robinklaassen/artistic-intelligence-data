import csv
import gzip
import io
import os
from datetime import timedelta, datetime

from influxdb_client import InfluxDBClient

from scripts.migrate.influx_client import get_influxdb_client_from_env

INFLUXDB_ORG = "robinklaassen"

FIRST_DATE = datetime(2021, 12, 18)
LAST_DATE = datetime.today()

def backup_influx_data_csv(client: InfluxDBClient, bucket: str, measurement: str, output_dir: str):
    """
    Backs up InfluxDB data as CSV by day, compresses it, and writes to disk.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Define the query to fetch data in daily chunks and output as CSV
    query = f'''
        from(bucket: "{bucket}")
          |> range(start: {{start}}, stop: {{stop}})
          |> filter(fn: (r) => r._measurement == "{measurement}")
    '''

    # Get the earliest and latest timestamps for the measurement
    # query_min_max = f'''
    #     from(bucket: "{bucket}")
    #       |> range(start: -30y)
    #       |> filter(fn: (r) => r._measurement == "{measurement}")
    #       |> group()
    #       |> first()
    #       |> keep(columns: ["_time"])
    #       |> yield(name: "first")
    #     from(bucket: "{bucket}")
    #       |> range(start: -30y)
    #       |> filter(fn: (r) => r._measurement == "{measurement}")
    #       |> group()
    #       |> last()
    #       |> keep(columns: ["_time"])
    #       |> yield(name: "last")
    # '''
    # tables = client.query_api().query(query_min_max, org=INFLUXDB_ORG)
    # first_time = tables["first"][0].values["_time"]
    # last_time = tables["last"][0].values["_time"]

    # Iterate over each day in the range
    # current_day = first_time.to_datetime().replace(hour=0, minute=0, second=0, microsecond=0)
    # end_day = last_time.to_datetime().replace(hour=0, minute=0, second=0, microsecond=0)

    current_day = FIRST_DATE
    while current_day <= LAST_DATE:
        start = current_day
        stop = start + timedelta(days=1)

        # Query data for the current day and output as CSV
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
        csv_data = csv_buffer.getvalue().encode('utf-8')
        compressed_data = gzip.compress(csv_data)

        # Write the compressed data to disk
        filename = f"{measurement}_{start.date()}.csv.gz"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "wb") as f:
            f.write(compressed_data)

        print(f"Backed up data for {start.date()} to {filepath}")

        current_day += timedelta(days=1)


if __name__ == "__main__":
    client = get_influxdb_client_from_env()
    backup_influx_data_csv(client, "live-api-collector", "train_locations", "D:\\Data\\influx_train_locations")
