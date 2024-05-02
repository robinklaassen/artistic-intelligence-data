from collections import defaultdict
from datetime import datetime, timedelta
from typing import TypeAlias

from fastapi import APIRouter, Security
from pydantic import BaseModel

from aid.provide.dependencies import get_api_key
from aid.provide.ns_trains import TrainRecord, NSTrainProvider

router = APIRouter(prefix="/trains", tags=["trains"], dependencies=[Security(get_api_key)])


class TrainLocation(BaseModel):
    id: int
    x: float
    y: float
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
