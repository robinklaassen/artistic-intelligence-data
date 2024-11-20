from collections import defaultdict
from datetime import datetime, timedelta
from time import perf_counter
from typing import TypeAlias

import pandas as pd
from fastapi import APIRouter, Security, HTTPException
from pydantic import BaseModel

from aid.constants import DEFAULT_TIMEZONE
from aid.logger import logger
from aid.provide.dependencies import get_api_key
from aid.provide.influx_trains import InfluxTrainProvider
from aid.provide.models import TrainRecord
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
    end = end or datetime.now(DEFAULT_TIMEZONE)
    start = start or end - timedelta(seconds=10)
    return InfluxTrainProvider().get_trains_as_records(start, end)


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
def get_pivoted_data(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train locations pivoted for use in TouchDesigner.
    Start and end parameters work the same as in `/records`.
    """
    end = end or datetime.now(DEFAULT_TIMEZONE)
    start = start or end - timedelta(seconds=10)
    df = InfluxTrainProvider().get_trains(start, end)
    if df is None:
        raise HTTPException(status_code=204)  # no content

    df = df.rename(
        columns={
            "_time": "timestamp",
            "train_id": "id",
            "train_type": "type",
        }
    )

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


@router.get("/types", response_class=CSVResponse)
def get_train_types(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train types for the requested period.
    """
    end = end or datetime.now(DEFAULT_TIMEZONE)
    start = start or end - timedelta(seconds=10)
    records = InfluxTrainProvider().get_train_types(start, end)

    df = pd.DataFrame.from_records([rec.model_dump() for rec in records])
    return df.to_csv(index=False)


if __name__ == "__main__":
    time_start = perf_counter()
    start = datetime.now(DEFAULT_TIMEZONE) - timedelta(hours=3)
    data = get_pivoted_data(start=start)
    # data = get_train_types(start=start)
    print(data)
    logger.info("Local run done", duration=perf_counter() - time_start)
