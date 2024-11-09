import os
from abc import ABC

import psycopg
from dotenv import load_dotenv
from psycopg import Connection

from aid.influxdb_client import get_influxdb_client_from_env


class BaseProvider(ABC):
    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual providers locally for testing
        self._influx_client = get_influxdb_client_from_env()
        self._influx_bucket = os.getenv("INFLUXDB_BUCKET", "")

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
