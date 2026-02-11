import os
from datetime import datetime, timedelta
from time import perf_counter

import polars as pl
from dotenv import load_dotenv

from artistic_intelligence_data.constants import DEFAULT_TIMEZONE
from artistic_intelligence_data.logger import get_logger
from artistic_intelligence_data.utils import validate_start_end

_logger = get_logger(__name__)


# TODO add an interface so we can swap implementations easier
class QuestDBTrainProvider:
    """
    Get train data from the QuestDB time series database.

    Note that any timestamps returned from the database are in UTC.
    """

    def __init__(self):
        load_dotenv()

        qdb_host = os.getenv("QDB_HOST", "localhost")
        qdb_port = os.getenv("QDB_PORT", "8812")
        qdb_user = os.getenv("QDB_USER", "admin")
        qdb_pass = os.getenv("QDB_PASS", "quest")

        # use redshift protocol to disable features not supported by QuestDB
        self._qdb_uri = f"redshift://{qdb_user}:{qdb_pass}@{qdb_host}:{qdb_port}/qdb"

    def _get_fields_from_db(
        self, start: datetime | None = None, end: datetime | None = None, select: str = "*", order_by: str = "timestamp"
    ) -> pl.DataFrame:
        start, end = validate_start_end(start, end)

        # QuestDB docs recommend using read_database_uri, but note this is not safe against SQL injection!
        # however in our case the API will prevent passing anything that is not a datetime
        query = f"""
            select {select}
            from train_locations
            where timestamp >= '{start}'
            and timestamp <= '{end}'
            and train_id != ''
            and x is not null
            and y is not null
            and x > 0
            and y > 0
            order by {order_by}
            ;
        """  # nosec

        result = pl.read_database_uri(query=query, uri=self._qdb_uri)
        if select != "*" and "timestamp" not in select:
            return result

        return result.with_columns(
            timestamp=pl.col("timestamp").dt.replace_time_zone("UTC").dt.convert_time_zone("Europe/Amsterdam")
        ).filter(
            pl.col("timestamp") >= start,  # filter for corrupted data in questdb that is somehow at epoch
        )

    def get_train_locations(self, start: datetime | None = None, end: datetime | None = None) -> pl.DataFrame:
        """
        Get train locations in table format straight from the database.
        """

        return self._get_fields_from_db(start, end, order_by="timestamp, train_id")

    def get_locations_torbenized(
        self, start: datetime | None = None, end: datetime | None = None, scale: bool = True
    ) -> pl.DataFrame:
        """
        Get train locations pivoted to wide format where every train_id has its own column.

        The 'scale' parameter scales the RDNew x/y coordinates to a (-1,1) square grid for use in TouchDesigner.
        """
        time_start = perf_counter()

        locations = self._get_fields_from_db(
            start, end, select="train_id, train_type, x, y, speed, timestamp", order_by="timestamp, train_id"
        )

        time_retrieved = perf_counter()
        _logger.debug(
            "Retrieved data from QuestDB", record_count=locations.height, duration_seconds=time_retrieved - time_start
        )

        train_ids = locations.select("train_id").unique().to_series().to_list()

        if scale:
            locations = locations.with_columns(
                ((pl.col("x") - 155_000) / (325_000 / 2)).round(5),
                ((pl.col("y") - 463_000) / (325_000 / 2)).round(5),
            )

        locations = locations.lazy()
        locations = locations.unpivot(
            on=["x", "y", "speed", "train_type"], index=["timestamp", "train_id"], variable_name="var"
        )
        locations = locations.pivot(
            on="train_id", on_columns=train_ids, index=["timestamp", "var"], values="value", aggregate_function="first"
        )
        locations = locations.sort(by=["timestamp", "var"])
        locations = locations.with_columns(
            pl.col("timestamp").dt.replace_time_zone("UTC").dt.convert_time_zone("Europe/Amsterdam")
        )
        locations = locations.collect()
        _logger.debug(
            "Pivoted data to format", record_count=locations.height, duration_seconds=perf_counter() - time_retrieved
        )
        return locations

    def get_train_types(self, start: datetime | None = None, end: datetime | None = None) -> pl.DataFrame:
        """
        Get type (IC, SPR, ...) for each train_id in the given period, straight from the database.
        """

        return self._get_fields_from_db(start, end, select="distinct train_id, train_type", order_by="train_id").filter(
            pl.col("train_id") != "",
        )

    def get_current_count(self) -> int:
        """
        Get train records for the last default period.
        """
        start, end = validate_start_end(start=None, end=None)  # use defaults
        return self.get_train_locations(start=start, end=end).height  # not optimized, loads all data first


if __name__ == "__main__":
    provider = QuestDBTrainProvider()
    start = datetime.now(tz=DEFAULT_TIMEZONE) - timedelta(days=3)

    print("--records--")
    train_locations = provider.get_train_locations(start=start)
    print(train_locations.head())

    print("--types--")
    train_types = provider.get_train_types(start=start)
    print(train_types.head())

    print("--torbenized--")
    tl_torbenized = provider.get_locations_torbenized(start=start)
    print(tl_torbenized.head())
