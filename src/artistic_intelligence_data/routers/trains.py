from datetime import datetime, timedelta
from time import perf_counter

from fastapi import APIRouter, Security

from artistic_intelligence_data.dependencies import get_api_key
from artistic_intelligence_data.models import TrainRecord
from artistic_intelligence_data.response import CSVResponse
from artistic_intelligence_data.trains.questdb_train_provider import QuestDBTrainProvider
from src.artistic_intelligence_data.constants import DEFAULT_TIMEZONE
from src.artistic_intelligence_data.logger import logger

# TODO can I inject an instance of QuestDBTrainProvider to all routes?
router = APIRouter(prefix="/trains", tags=["trains"], dependencies=[Security(get_api_key)])


# class TrainLocation(BaseModel):
#     id: int
#     x: int
#     y: int
#     speed: float
#     direction: float
#     accuracy: float
#
#
# type KeyedTrainLocations = dict[datetime, list[TrainLocation]]


@router.get("/locations")
def get_locations(start: datetime | None = None, end: datetime | None = None) -> list[TrainRecord]:
    """
    Get train locations for the requested period as list of records.

    - **start**: start of the requested period (timestamp, defaults to 10 seconds ago)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    provider = QuestDBTrainProvider()
    locations = provider.get_train_locations(start, end)
    return [
        TrainRecord(
            timestamp=rec["timestamp"],
            id=rec["train_id"],
            x=rec["x"],
            y=rec["y"],
            speed=rec["speed"],
            direction=rec["direction"],
            accuracy=rec["accuracy"],
            type=rec["train_type"],
        )
        for rec in locations.to_dicts()
    ]


# @router.get("/keyed")
# def get_records_keyed_by_timestamp(start: datetime | None = None, end: datetime | None = None) -> KeyedTrainLocations:
#     """
#     Get train locations for the requested period as record sets keyed by timestamp.
#
#     - **start**: start of the requested period (timestamp, defaults to 10 seconds ago)
#     - **end**: end of the requested period (timestamp, defaults to current time)
#     """
#     records = _records(start, end)
#     output: KeyedTrainLocations = defaultdict(list)
#     for record in records:
#         output[record.timestamp].append(
#             TrainLocation(
#                 id=record.id,
#                 x=record.x,
#                 y=record.y,
#                 speed=record.speed,
#                 direction=record.direction,
#                 accuracy=record.accuracy,
#             )
#         )
#     return output


@router.get("/locations-pivoted", response_class=CSVResponse)
def get_locations_pivoted(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train locations pivoted for use in TouchDesigner.
    Start and end parameters work the same as in `/records`.
    """
    provider = QuestDBTrainProvider()
    pivoted_locations = provider.get_locations_torbenized(start, end)
    # TODO convert timestamp to time in local tz here
    return pivoted_locations.write_csv()


@router.get("/types", response_class=CSVResponse)
def get_train_types(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train types for the requested period.
    """
    provider = QuestDBTrainProvider()
    train_types = provider.get_train_types(start, end)
    return train_types.write_csv()


if __name__ == "__main__":
    time_start = perf_counter()
    start = datetime.now(DEFAULT_TIMEZONE) - timedelta(hours=3)
    data = get_locations_pivoted(start=start)
    # data = get_train_types(start=start)
    print(data)
    logger.info("Local run done", duration=perf_counter() - time_start)
