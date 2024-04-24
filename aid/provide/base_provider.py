import os
from abc import ABC

import psycopg
from dotenv import load_dotenv


class BaseProvider(ABC):
    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual providers locally for testing

        self._pg_conn = psycopg.connect(
            dbname=os.getenv("PG_DBNAME", "postgres"),
            user=os.getenv("PG_READ_USER", "postgres"),
            password=os.getenv("PG_READ_PASS", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
        )
