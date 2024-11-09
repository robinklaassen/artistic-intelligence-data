import os

from influxdb_client import InfluxDBClient

from aid.logger import logger


def get_influxdb_client_from_env() -> InfluxDBClient:
    """Open and return a new connection to InfluxDB."""

    client = InfluxDBClient(
        url=os.getenv("INFLUXDB_URL", ""),
        token=os.getenv("INFLUXDB_TOKEN", ""),
        org=os.getenv("INFLUXDB_ORG", ""),
        timeout=(
            os.getenv("INFLUXDB_CONNECT_TIMEOUT", 5_000),
            os.getenv("INFLUXDB_READ_TIMEOUT", 300_000),
        ),  # unit: ms
        verify_ssl=True,
        enable_gzip=True,
    )

    if not client.ping():
        raise ConnectionRefusedError("Could not connect to InfluxDB, ping failed.")

    bucket_name = os.getenv("INFLUXDB_BUCKET", "")
    buckets_api = client.buckets_api()

    if buckets_api.find_bucket_by_name(bucket_name) is None:
        raise LookupError(f"Could not find InfluxDB bucket `{bucket_name}`.")

    logger.debug("InfluxDB client created successfully")

    return client
