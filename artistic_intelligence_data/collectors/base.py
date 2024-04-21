import os
from abc import ABC, abstractmethod
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

from artistic_intelligence_data.utils.logger import logger


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

    def run(self):
        """Run this collector once."""
        start_time = datetime.now()
        try:
            record_count = self._execute(start_time)
        except Exception as exc:  # anything can happen
            logger.error(self.__class__.__name__, exception=exc)
            return
        duration_seconds = (datetime.now() - start_time).total_seconds()
        logger.info(self.__class__.__name__, record_count=record_count, duration_seconds=duration_seconds)

    @abstractmethod
    def _execute(self, timestamp: datetime) -> int:
        """Execute the work specific to this collector. Return the amount of records processed."""
        raise NotImplementedError
