import os
from datetime import datetime

import requests
from pydantic import BaseModel

from artistic_intelligence_data.collectors.base import BaseCollector
from artistic_intelligence_data.utils.logger import logger

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"
TARGET_SCHEMA = "raw"
TARGET_TABLE = "ns_trains"


# TODO list:
#  - query API
#  - package setup
#  - CI/CD


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


class TrainCollector(BaseCollector):
    """
    Collect live train locations from NS API.
    """

    interval_seconds = 10

    def __init__(self):
        super().__init__()
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
            timeout=10,
        )

        if not response.ok:
            logger.error(
                "Train response error", code=response.status_code, text=response.text
            )
            return None

        return TrainResponse.model_validate_json(response.text, strict=True)

    def _store_trains(
        self, trains: TrainResponse, timestamp: datetime = datetime.now()
    ):
        with self._pg_conn as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    f"insert into {TARGET_SCHEMA}.{TARGET_TABLE} "
                    "(timestamp, rit_id, snelheid, richting, horizontale_nauwkeurigheid, type, bron, location) "
                    "values (%s, %s, %s, %s, %s, %s, %s, ST_MakePoint(%s, %s))",
                    [
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
                    ],
                )

        conn.close()


if __name__ == "__main__":
    trains = TrainCollector()._get_trains()
    if trains is not None:
        print(trains.payload.treinen[0])

    TrainCollector().run()