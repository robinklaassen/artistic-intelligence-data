import os
from datetime import datetime

import requests
from pydantic import BaseModel

from aid.collect.base_collector import BaseCollector
from aid.logger import logger

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"
REQUEST_TIMEOUT: int = 5


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
            timeout=REQUEST_TIMEOUT,
        )

        if not response.ok:
            logger.error("Train response error", code=response.status_code, text=response.text)
            return None

        return TrainResponse.model_validate_json(response.text, strict=True)

    def _store_trains(self, trains: TrainResponse, timestamp: datetime = datetime.now()):
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
    trains = NSTrainCollector()._get_trains()
    if trains is not None:
        print(trains.payload.treinen[0])

    NSTrainCollector().run()
