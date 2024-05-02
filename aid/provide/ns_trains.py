from datetime import datetime, timedelta

from psycopg.rows import class_row
from pydantic import BaseModel

from aid.logger import logger
from aid.provide.base_provider import BaseProvider


class TrainRecord(BaseModel):
    timestamp: datetime
    id: int
    x: float
    y: float
    speed: float
    direction: float
    accuracy: float


class NSTrainProvider(BaseProvider):
    def get_trains(self, start: datetime, end: datetime) -> list[TrainRecord]:
        # TODO this could be view in postgres
        # TODO optional gap filling
        query = """
        select time_bucket('10 seconds', timestamp, '-5 seconds'::INTERVAL) + '5 seconds' as timestamp, rit_id as id, 
            avg(st_x(st_transform(location, 28992))) as x, avg(st_y(st_transform(location, 28992))) as y,
            avg(snelheid) as speed, avg(richting) as direction, avg(horizontale_nauwkeurigheid) as accuracy
        from raw.ns_trains
        where timestamp between %s and %s
        group by timestamp, rit_id
        order by timestamp asc, rit_id asc
        """
        with self._pg_conn as conn:
            with conn.cursor(row_factory=class_row(TrainRecord)) as cur:
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
