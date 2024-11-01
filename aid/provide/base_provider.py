import os
from abc import ABC

import psycopg
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from psycopg import Connection

from aid.logger import logger


class BaseProvider(ABC):
    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual providers locally for testing

    @property
    def _pg_conn(self) -> Connection:
        """Open and return a new connection to the Postgres database."""
        # TODO handle connection failure gracefully
        return psycopg.connect(
            dbname=os.getenv("PG_DBNAME", "postgres"),
            user=os.getenv("PG_READ_USER", "postgres"),
            password=os.getenv("PG_READ_PASS", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
        )

    @property
    def _influx_client(self) -> InfluxDBClient:
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
