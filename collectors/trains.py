import os
from datetime import datetime

import requests
from pydantic import BaseModel

from collectors.base import BaseCollector
from utils.logger import logger

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"


# TODO list:
#  - save to postgresql
#  - query API
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

    def run(self):
        start_time = datetime.now()
        trains = self._get_trains()
        self._store_trains(trains)
        logger.info("Collected train records", count=len(trains.payload.treinen),
                    total_seconds=(datetime.now() - start_time).total_seconds())

    def _get_trains(self) -> TrainResponse | None:
        response = requests.get(NS_VIRTUAL_TRAIN_URL,
                                headers={'Ocp-Apim-Subscription-Key': self._api_key},
                                timeout=10)

        if not response.ok:
            logger.error("Train response error", code=response.status_code, text=response.text)
            return None

        return TrainResponse.model_validate_json(response.text, strict=True)

    def _store_trains(self, trains: TrainResponse):
        pass


if __name__ == "__main__":
    TrainCollector().run()
