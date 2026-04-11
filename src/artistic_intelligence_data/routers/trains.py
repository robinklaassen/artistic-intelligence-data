from datetime import datetime, timedelta
from time import perf_counter

import polars as pl
from fastapi import APIRouter, Security

from artistic_intelligence_data.dependencies import verify_api_key
from artistic_intelligence_data.models import TrainPosition, TrainRecord
from artistic_intelligence_data.response import CSVResponse
from artistic_intelligence_data.trains.questdb_train_provider import QuestDBTrainProvider
from src.artistic_intelligence_data.constants import DEFAULT_TIMEZONE
from src.artistic_intelligence_data.logger import logger

# TODO can I inject an instance of QuestDBTrainProvider to all routes?
router = APIRouter(prefix="/trains", tags=["trains"], dependencies=[Security(verify_api_key)])


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


@router.get("/locations-keyed")
def get_locations_keyed_by_timestamp(
    start: datetime | None = None, end: datetime | None = None
) -> dict[str, list[TrainPosition]]:
    """
    Get train positions for the requested period as lists keyed by timestamp.
    Contains a subset of location attributes to minimize transport size.

    - **start**: start of the requested period (timestamp, defaults to 10 seconds ago)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """

    provider = QuestDBTrainProvider()
    locations = provider.get_train_locations(start, end)

    keyed_positions = (
        locations.with_columns(
            pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S"),  # removes timezone info
        )
        .select(["timestamp", "train_id", "x", "y"])
        .rows_by_key(key="timestamp", named=True)
    )

    # TODO check if this cast can be more efficient inside the polars conversion
    return {k: [TrainPosition(id=r["train_id"], x=r["x"], y=r["y"]) for r in v] for k, v in keyed_positions.items()}


@router.get("/locations-pivoted", response_class=CSVResponse)
def get_locations_pivoted(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train locations pivoted for use in TouchDesigner.
    Start and end parameters work the same as in `/records`.
    """
    provider = QuestDBTrainProvider()
    pivoted_locations = provider.get_locations_torbenized(start, end)
    return pivoted_locations.with_columns(
        pl.col("timestamp").dt.strftime("%H:%M:%S"),
    ).write_csv()


@router.get("/types/csv", response_class=CSVResponse)
def get_train_types_csv(start: datetime | None = None, end: datetime | None = None) -> str:
    """
    Get train types for the requested period as a CSV string.
    """
    provider = QuestDBTrainProvider()
    train_types = provider.get_train_types(start, end)
    return train_types.write_csv()


@router.get("/types/json")
def get_train_types_json(start: datetime | None = None, end: datetime | None = None) -> dict[int, str]:
    """
    Get train types for the requested period as a JSON dictionary.
    """
    provider = QuestDBTrainProvider()
    train_types = provider.get_train_types(start, end)
    return dict(train_types.select("train_id", "train_type").iter_rows())


@router.get("/materials/main")
def get_train_main_materials(start: datetime | None = None, end: datetime | None = None) -> dict[int, str]:
    """
    Get train materials for the requested period as a JSON dictionary.
    These are the main type of 'materieel', not the subtype (e.g. 'VIRM', not 'VIRM IV').

    - **start**: start of the requested period (timestamp, defaults to 1 hour ago)
    - **end**: end of the requested period (timestamp, defaults to current time)
    """
    provider = QuestDBTrainProvider()
    train_material = provider.get_train_material(start, end)
    return dict(train_material.select("train_id", "material").iter_rows())


if __name__ == "__main__":
    time_start = perf_counter()
    start = datetime.now(DEFAULT_TIMEZONE) - timedelta(hours=3)
    data = get_locations_pivoted(start=start)
    # data = get_train_types(start=start)
    print(data)
    logger.info("Local run done", duration=perf_counter() - time_start)
