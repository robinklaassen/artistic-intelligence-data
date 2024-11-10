import os
from datetime import datetime

import requests
from influxdb_client import Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pydantic import BaseModel

from aid.collect.base_collector import BaseCollector
from aid.constants import WGS84_TO_RDNEW
from aid.logger import logger

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"
REQUEST_TIMEOUT: int = 4


class TrainModel(BaseModel):
    treinNummer: int
    ritId: str
    lat: float
    lng: float
    snelheid: float
    richting: float
    horizontaleNauwkeurigheid: float
    type: str
    bron: str


class TrainPayload(BaseModel):
    treinen: list[TrainModel]


class TrainResponse(BaseModel):
    payload: TrainPayload


class NSTrainCollector(BaseCollector):
    """
    Collect live train locations from NS API.
    """

    interval_seconds = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._api_key = os.getenv("NS_API_KEY")

    def _execute(self, timestamp: datetime) -> int:
        trains = self._get_trains()
        if trains is None:
            return 0
        self._store_trains(trains, timestamp)
        return len(trains.payload.treinen)

    def _get_trains(self) -> TrainResponse | None:
        response = requests.get(
            NS_VIRTUAL_TRAIN_URL,
            headers={"Ocp-Apim-Subscription-Key": self._api_key},
            timeout=REQUEST_TIMEOUT,
        )

        if not response.ok:
            logger.error("Train response error", code=response.status_code, text=response.text)
            return None

        return TrainResponse.model_validate_json(response.text, strict=True)

    def _store_trains(self, trains: TrainResponse, timestamp: datetime = datetime.now()):
        """Store train response into InfluxDB"""

        if self._influx_client is None:
            logger.warning("No InfluxDB client available, skip storing trains")
            return

        points = []
        for t in trains.payload.treinen:
            x, y = WGS84_TO_RDNEW.transform(t.lng, t.lat)
            points.append(
                Point("train_locations")
                .time(self._round_timestamp(timestamp, freq="10s"))
                .tag("train_id", t.ritId)
                .tag("train_type", t.type)
                .tag("source", t.bron)
                .field("lat", t.lat)
                .field("lng", t.lng)
                .field("x", x)
                .field("y", y)
                .field("speed", t.snelheid)
                .field("direction", t.richting)
                .field("accuracy", t.horizontaleNauwkeurigheid)
            )

        with self._influx_client.write_api(write_options=SYNCHRONOUS) as write_api:
            write_api.write(bucket=self._influx_bucket, record=points)

        logger.debug("Wrote train records to InfluxDB.", count=len(points))

    def _store_trains_postgres(self, trains: TrainResponse, timestamp: datetime = datetime.now()):
        """Currently replaced by storing into InfluxDB, but needs further performance analysis."""
        query = """
        insert into raw.ns_trains
        (timestamp, rit_id, snelheid, richting, horizontale_nauwkeurigheid, type, bron, location)
        values (%s, %s, %s, %s, %s, %s, %s, ST_MakePoint(%s, %s))
        """

        params = [
            (
                timestamp,
                int(t.ritId),
                t.snelheid,
                t.richting,
                t.horizontaleNauwkeurigheid,
                t.type,
                t.bron,
                t.lng,
                t.lat,
            )
            for t in trains.payload.treinen
        ]

        with self._pg_conn as conn:
            with conn.cursor() as cur:
                cur.executemany(query, params)


if __name__ == "__main__":
    trains = NSTrainCollector(with_influx=False)._get_trains()
    if trains is not None:
        train0 = [t for t in trains.payload.treinen if t.treinNummer == 8746]
        print(train0[0])
        # print(trains.payload.treinen)
