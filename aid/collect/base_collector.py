import os
from abc import ABC, abstractmethod
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg import Connection

from aid.logger import logger


class BaseCollector(ABC):
    def __init__(self):
        load_dotenv(verbose=True)  # allows running individual collectors locally for testing

    @property
    def _pg_conn(self) -> Connection:
        """Open and return a new connection to the Postgres database."""
        return psycopg.connect(
            dbname=os.getenv("PG_DBNAME", "postgres"),
            user=os.getenv("PG_WRITE_USER", "postgres"),
            password=os.getenv("PG_WRITE_PASS", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", "5432"),
        )

    @property
    @abstractmethod
    def interval_seconds(self) -> int:
        """The interval that this collector should run on, in seconds."""
        raise NotImplementedError

    def run(self):
        """Run this collector once."""
        start_time = datetime.now()
        try:
            record_count = self._execute(start_time)
        except Exception as exc:  # anything can happen
            logger.error(self.__class__.__name__, err_type=exc.__class__.__name__, msg=str(exc))
            return
        duration_seconds = (datetime.now() - start_time).total_seconds()
        logger.info(
            self.__class__.__name__,
            record_count=record_count,
            duration_seconds=duration_seconds,
        )

    @abstractmethod
    def _execute(self, timestamp: datetime) -> int:
        """Execute the work specific to this collector. Return the amount of records processed."""
        raise NotImplementedError
