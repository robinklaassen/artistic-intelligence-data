import os
from abc import ABC, abstractmethod

import psycopg2
from dotenv import load_dotenv


class BaseCollector(ABC):

    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual collectors locally for testing

        self._pg_conn = psycopg2.connect(
            dbname=os.getenv("PG_DBNAME"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASS"),
        )

    @property
    @abstractmethod
    def interval_seconds(self) -> int:
        """The interval that this collector should run on, in seconds."""
        raise NotImplementedError

    @abstractmethod
    def run(self):
        """Run this collector once."""
        raise NotImplementedError
