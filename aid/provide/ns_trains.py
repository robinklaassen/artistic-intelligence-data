from datetime import datetime, timedelta

from psycopg.rows import class_row

from aid.logger import logger
from aid.provide.base_provider import BaseProvider
from aid.provide.models import TrainRecord, TrainType


class NSTrainProvider(BaseProvider):
    def get_trains(self, start: datetime, end: datetime) -> list[TrainRecord]:
        query = """
        select timestamp, id, round(x) as x, round(y) as y, speed, direction, accuracy, type
        from std.trains
        where timestamp between %s and %s
        and x > 0 and y > 0
        order by timestamp asc, id asc
        """
        with self._pg_conn as conn:
            with conn.cursor(row_factory=class_row(TrainRecord)) as cur:
                results = cur.execute(query, (start, end)).fetchall()

        return results

    def get_train_types(self, start: datetime, end: datetime) -> list[TrainType]:
        query = """
        select distinct id, type
        from std.trains
        where timestamp between %s and %s
        order by id asc
        """
        with self._pg_conn as conn:
            with conn.cursor(row_factory=class_row(TrainType)) as cur:
                results = cur.execute(query, (start, end)).fetchall()

        return results

    def get_current_count(self) -> int:
        query = """
        select count(*) as cnt
        from raw.ns_trains
        where timestamp between (now() - '10 seconds'::interval) and now()
        """
        with self._pg_conn as conn:
            with conn.cursor() as cur:
                result: int = cur.execute(query).fetchone()[0]  # type: ignore

        return result


if __name__ == "__main__":
    now = datetime.now()
    prov = NSTrainProvider()
    trains = prov.get_trains(now - timedelta(days=2), now)
    logger.info("Get trains", count=len(trains))
