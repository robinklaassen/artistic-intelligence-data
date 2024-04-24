from dataclasses import dataclass
from datetime import datetime, timedelta

from psycopg.rows import class_row

from aid.logger import logger
from aid.provide.base_provider import BaseProvider


@dataclass
class TrainRecord:
    timestamp: datetime
    id: int
    x: float
    y: float


class NSTrainProvider(BaseProvider):
    def get_trains(self, start: datetime, end: datetime) -> list[TrainRecord]:
        query = """
        select timestamp, rit_id as id, st_x(st_transform(location, 28992)) as x, st_y(st_transform(location, 28992)) as y
        from raw.ns_trains
        where timestamp between %s and %s
        """
        with self._pg_conn as conn:
            with conn.cursor(row_factory=class_row(TrainRecord)) as cur:
                cur.execute(query, (start, end))
                results = cur.fetchall()

        return results


if __name__ == "__main__":
    now = datetime.now()
    prov = NSTrainProvider()
    trains = prov.get_trains(now - timedelta(days=2), now)
    logger.info("Get trains", count=len(trains))
