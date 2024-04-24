import os
from abc import ABC

import psycopg
from dotenv import load_dotenv
from psycopg import Connection


class BaseProvider(ABC):
    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual providers locally for testing

    @property
    def _pg_conn(self) -> Connection:
        """Open and return a new connection to the Postgres database."""
        return psycopg.connect(
            dbname=os.getenv("PG_DBNAME", "postgres"),
            user=os.getenv("PG_READ_USER", "postgres"),
            password=os.getenv("PG_READ_PASS", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
        )
