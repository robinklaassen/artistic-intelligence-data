import os
from datetime import datetime, timedelta
from time import perf_counter

import geopandas as gpd
import pandas as pd
from geopandas import points_from_xy

from aid.constants import DEFAULT_TIMEZONE
from aid.logger import logger
from aid.provide.base_provider import BaseProvider
from aid.provide.models import TrainRecord, TrainType

pd.options.display.max_columns = None


class InfluxTrainProvider(BaseProvider):
    # TODO better name, changing from postgres to influx kind of on the fly now

    def get_trains(self, start: datetime, end: datetime) -> pd.DataFrame:
        """
        Get train data from InfluxDB as a data frame.

        Learnings:
        - truncateTimeColumn sounds nice but is ridiculously slow, insert rounded or use pandas
        """

        start, end = self._validate_datetimes(start, end)

        query = f"""
        from(bucket: "{os.getenv("INFLUXDB_BUCKET")}")
        |> range(start: {start.isoformat(timespec="seconds")}, stop: {end.isoformat(timespec="seconds")})
        |> filter(fn: (r) => r["_measurement"] == "train_locations")
        //|> truncateTimeColumn(unit: 10s)
        |> pivot(columnKey: ["_field"], rowKey: ["train_id", "_time"], valueColumn: "_value")
        |> keep(columns: ["train_id", "train_type", "_time", "lat", "lng", "speed", "direction"])
        |> filter(fn: (r) =>
            exists r.lat and exists r.lng
        )
        |> sort(columns: ["_time"])
        """

        time_start = perf_counter()

        query_api = self._influx_client.query_api()
        df = query_api.query_data_frame(query=query, use_extension_dtypes=True)

        time_query_done = perf_counter()
        logger.debug("Influx query time", duration=time_query_done - time_start)

        # round timestamps to nearest 10 seconds
        df._time = df._time.dt.round("10s")
        time_rounding_done = perf_counter()
        logger.debug("Time rounding done", duration=time_rounding_done - time_query_done)

        # transform coordinates to RDS
        gdf = gpd.GeoDataFrame(data=df, geometry=points_from_xy(df.lng, df.lat), crs="EPSG:4326").to_crs(epsg=28992)
        gdf["x"] = round(gdf.geometry.x)
        gdf["y"] = round(gdf.geometry.y)
        time_geo_done = perf_counter()
        logger.debug("Time geo conversion done", duration=time_geo_done - time_rounding_done)

        return gdf

    def get_trains_as_records(self, start: datetime, end: datetime) -> list[TrainRecord]:
        """
        Get train data from InfluxDB as a list of pydantic records.
        """
        gdf = self.get_trains(start, end)

        output = [
            TrainRecord(
                timestamp=r["_time"],
                id=r["train_id"],
                x=r["x"],
                y=r["y"],
                speed=r["speed"],
                direction=r["direction"],
                accuracy=0.0,
                type=r["train_type"],
            )
            for r in gdf.to_dict("records")  # TODO this could be quicker
        ]

        return output

    def get_train_types(self, start: datetime, end: datetime) -> list[TrainType]:
        start, end = self._validate_datetimes(start, end)

        query = f"""
        from(bucket: "{os.getenv("INFLUXDB_BUCKET")}")
        |> range(start: {start.isoformat(timespec="seconds")}, stop: {end.isoformat(timespec="seconds")})
        |> filter(fn: (r) => r["_measurement"] == "train_locations")
        |> group(columns: ["train_id", "train_type"])
        |> distinct(column: "train_type")
        |> keep(columns: ["train_id", "_value"])
        """

        query_api = self._influx_client.query_api()
        tables = query_api.query(query=query)  # query_data_frame warns about missing pivot, but we don't need it

        return [
            TrainType(id=row.values["train_id"], type=row.values["_value"]) for table in tables for row in table.records
        ]

    def get_current_count(self) -> int:
        # TODO
        return 0

    def _validate_datetimes(self, start: datetime, end: datetime) -> tuple[datetime, datetime]:
        if start.tzinfo is None:
            logger.warning("Using default timezone for trains start time")
            start = start.replace(tzinfo=DEFAULT_TIMEZONE)

        if end.tzinfo is None:
            logger.warning("Using default timezone for trains end time")
            end = end.replace(tzinfo=DEFAULT_TIMEZONE)

        return start, end


if __name__ == "__main__":
    now = datetime.now(tz=DEFAULT_TIMEZONE)
    prov = InfluxTrainProvider()
    time_start = perf_counter()
    # records = prov.get_trains(now - timedelta(hours=1), now)
    records = prov.get_train_types(now - timedelta(hours=1), now)

    logger.info("Local run done", count=len(records), duration=perf_counter() - time_start)
