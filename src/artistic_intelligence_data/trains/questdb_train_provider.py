import os
from datetime import datetime, timedelta

import polars as pl
from dotenv import load_dotenv

from artistic_intelligence_data.constants import DEFAULT_TIMEZONE
from artistic_intelligence_data.utils import _validate_start_end


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

    def get_train_locations(self, start: datetime | None = None, end: datetime | None = None) -> pl.DataFrame:
        """
        Get train locations in table format straight from the database.
        """
        start, end = _validate_start_end(start, end)

        # QuestDB docs recommend using read_database_uri, but note this is not safe against SQL injection!
        # however in our case the API will prevent passing anything that is not a datetime
        query = f"""
            select *
            from train_locations
            where timestamp >= '{start}'
            and timestamp <= '{end}'
            and x is not null
            and y is not null
            order by timestamp, train_id
            ;
        """  # nosec

        return pl.read_database_uri(query=query, uri=self._qdb_uri)

    def get_locations_torbenized(
        self, start: datetime | None = None, end: datetime | None = None, scale: bool = True
    ) -> pl.DataFrame:
        """
        Get train locations pivoted to wide format where every train_id has its own column.

        The 'scale' parameter scales the RDNew x/y coordinates to a (-1,1) square grid for use in TouchDesigner.
        """
        locations = self.get_train_locations(start=start, end=end)
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
        locations = locations.pivot(on="train_id", on_columns=train_ids, index=["timestamp", "var"], values="value")
        locations = locations.sort(by=["timestamp", "var"])
        return locations.collect()

    def get_train_types(self, start: datetime | None = None, end: datetime | None = None) -> pl.DataFrame:
        """
        Get type (IC, SPR, ...) for each train_id in the given period, straight from the database.
        """
        start, end = _validate_start_end(start, end)

        query = f"""
            select distinct train_id, train_type
            from train_locations
            where timestamp >= '{start}'
            and timestamp <= '{end}'
            and x is not null
            and y is not null
            order by train_id
            ;
        """  # nosec

        return pl.read_database_uri(query=query, uri=self._qdb_uri)

    def get_current_count(self) -> int:
        """
        Get train records for the last default period.
        """
        start, end = _validate_start_end(start=None, end=None)  # use defaults
        return self.get_train_locations(start=start, end=end).height  # not optimized, loads all data first


if __name__ == "__main__":
    provider = QuestDBTrainProvider()
    start = datetime.now(tz=DEFAULT_TIMEZONE) - timedelta(days=1)

    print("--records--")
    train_locations = provider.get_train_locations(start=start)
    print(train_locations.head())

    print("--types--")
    train_types = provider.get_train_types(start=start)
    print(train_types.head())

    print("--torbenized--")
    tl_torbenized = provider.get_locations_torbenized(start=start)
    print(tl_torbenized.head())
