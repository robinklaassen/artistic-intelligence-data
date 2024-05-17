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
    Get train locations pivoted for use in TouchDesigner.
    Start and end parameters work the same as in `/records`.
    """
    records = _records(start, end)
    if not records:
        print("No records")
        return status.HTTP_204_NO_CONTENT

    df = pd.DataFrame.from_records([rec.model_dump() for rec in records])

    # Scale x and y to a [-1,1] square area
    # RDS range is 0 < x 280 and 300 < y < 625 (km)
    # Center Amersfoort (155, 463) to (0, 0)
    df["x"] = (df["x"] - 155_000) / (325_000 / 2)
    df["y"] = (df["y"] - 463_000) / (325_000 / 2)

    df = df.round(5)

    # Pivot to requested format
    df = df.melt(id_vars=["timestamp", "id"], value_vars=["x", "y", "speed", "type"], var_name="var")
    df = df.pivot(columns="id", index=["timestamp", "var"], values="value")
    df = df.reset_index()
    df["timestamp"] = df["timestamp"].dt.strftime("%H:%M:%S")

    return df.to_csv(index=False)


if __name__ == "__main__":
    start = datetime.now() - timedelta(minutes=1)
    print(get_pivoted_data(start=start))
