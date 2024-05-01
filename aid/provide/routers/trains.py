from datetime import datetime, timedelta

from fastapi import APIRouter, Security

from aid.provide.dependencies import get_api_key
from aid.provide.ns_trains import TrainRecord, NSTrainProvider

router = APIRouter(prefix="/trains", tags=["trains"], dependencies=[Security(get_api_key)])


@router.get("/current")
def get_current_train_locations() -> list[TrainRecord]:
    """
    Get current train locations (last 10 seconds).
    """
    end = datetime.now()
    start = end - timedelta(seconds=10)
    return NSTrainProvider().get_trains(start, end)


@router.get("/period")
def get_train_locations_in_period(start: datetime, end: datetime | None = None) -> list[TrainRecord]:
    """
    Get the train locations for a specified period.

    - **start**: start of the requested period (timestamp, required)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    end = end or datetime.now()
    return NSTrainProvider().get_trains(start, end)
