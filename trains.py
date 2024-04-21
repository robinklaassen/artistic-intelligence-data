import os
from pprint import pp

import requests
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"
NS_API_KEY = os.getenv("NS_API_KEY")


# TODO list:
#  - save to postgresql
#  - schedule
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


def get_trains() -> TrainResponse | None:
    response = requests.get(NS_VIRTUAL_TRAIN_URL,
                            headers={'Ocp-Apim-Subscription-Key': NS_API_KEY},
                            timeout=10)

    if not response.ok:
        return None

    return TrainResponse.model_validate_json(response.text, strict=True)


if __name__ == "__main__":
    trains = get_trains()
    if trains is not None:
        pp(trains.model_dump())
