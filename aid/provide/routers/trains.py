from collections import defaultdict
from datetime import datetime, timedelta
from typing import TypeAlias

import pandas as pd
from fastapi import APIRouter, Security, status
from pydantic import BaseModel

from aid.provide.dependencies import get_api_key
from aid.provide.ns_trains import TrainRecord, NSTrainProvider
from aid.provide.response import CSVResponse

router = APIRouter(prefix="/trains", tags=["trains"], dependencies=[Security(get_api_key)])


class TrainLocation(BaseModel):
    id: int
    x: int
    y: int
    speed: float
    direction: float
    accuracy: float


KeyedTrainLocations: TypeAlias = dict[datetime, list[TrainLocation]]


def _records(start: datetime | None = None, end: datetime | None = None) -> list[TrainRecord]:
    end = end or datetime.now()
    start = start or end - timedelta(seconds=10)
    return NSTrainProvider().get_trains(start, end)


@router.get("/records")
def get_records(start: datetime | None = None, end: datetime | None = None) -> list[TrainRecord]:
    """
    Get train locations for the requested period as list of records.

    - **start**: start of the requested period (timestamp, defaults to 10 seconds ago)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    return _records(start, end)


@router.get("/keyed")
def get_records_keyed_by_timestamp(start: datetime | None = None, end: datetime | None = None) -> KeyedTrainLocations:
    """
    Get train locations for the requested period as record sets keyed by timestamp.

    - **start**: start of the requested period (timestamp, defaults to 10 seconds ago)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    records = _records(start, end)
    output: KeyedTrainLocations = defaultdict(list)
    for record in records:
        output[record.timestamp].append(
            TrainLocation(
                id=record.id,
                x=record.x,
                y=record.y,
                speed=record.speed,
                direction=record.direction,
                accuracy=record.accuracy,
            )
        )
    return output


@router.get("/pivoted", response_class=CSVResponse)
def get_pivoted_data(start: datetime | None = None, end: datetime | None = None):
    """
    Get train locations pivoted for use in touch designer.
    Start and end parameters work the same as in `/records`.
    """
    records = _records(start, end)
    if not records:
        print("No records")
        return status.HTTP_204_NO_CONTENT

    df = pd.DataFrame.from_records([rec.model_dump() for rec in records])
    # df["speed"] = df["speed"].astype(int)
    df = df.melt(id_vars=["timestamp", "id"], value_vars=["x", "y", "speed"], var_name="var")
    df = df.pivot(columns="id", index=["timestamp", "var"], values="value")
    df = df.reset_index()
    df["timestamp"] = df["timestamp"].dt.strftime("%H:%M:%S")

    # TODO cast floats to ints
    # id_cols = [col for col in df.columns if col not in {"timestamp", "var"}]
    # df[id_cols] = df[id_cols].astype("Int64")
    # print(df.dtypes)

    return df.to_csv(index=False)


if __name__ == "__main__":
    start = datetime.now() - timedelta(minutes=1)
    get_pivoted_data(start=start)
