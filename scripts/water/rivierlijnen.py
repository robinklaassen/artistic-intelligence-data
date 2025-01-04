from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry.linestring import LineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from shapely.ops import nearest_points

from repo_path import REPO_PATH

SCRIPTS_OUTPUT_PATH = REPO_PATH / "scripts" / "water" / "output"

LOCATIONS_FILE = SCRIPTS_OUTPUT_PATH / "waterhoogtes_locaties.csv"
# MEASUREMENTS_FILE = SCRIPTS_OUTPUT_PATH / "waterhoogtes_long.csv"
MEASUREMENTS_FILE = REPO_PATH / "notebooks" / "output" / "waterhoogtes_long.csv"  # TODO
RIVER_LINES_FILE = REPO_PATH / "notebooks" / "data" / "rivierlijnen.zip"


def spatial_interpolate(river_vertices: MultiPoint,
                        location_code_by_index: dict[int, str],
                        measurements: pd.DataFrame,
                        the_time: str) -> pd.Series:
    """Spatial interpolation of measurement data for a single time point."""
    meas_at_time = measurements[measurements["DATUMTIJD"] == the_time]
    meas_lookup = {r["LOCATIE_CODE"]: r["WAARDE_GESCHAALD"] for _, r in meas_at_time.iterrows()}

    points = [location_code_by_index.get(i, None) for i in range(len(river_vertices.geoms))]
    values = [
        meas_lookup.get(p, np.nan)
        for p in points
    ]
    return pd.Series(values, name=the_time).interpolate()


def create_interpolated_measurements(locations: gpd.GeoDataFrame, measurements: pd.DataFrame,
                                     river_line: LineString) -> pd.DataFrame:
    river_vertices = MultiPoint([Point(xy) for xy in river_line.coords])
    river_locations = locations[locations.distance(river_line) < 1000]

    # construct dict of river_vertices index to location code
    location_code_by_index: dict[int, str] = {}
    for _, row in river_locations.iterrows():
        vertex = nearest_points(row.geometry, river_vertices)[1]
        idx = list(river_vertices.geoms).index(vertex)
        location_code_by_index[idx] = row["LOCATIE_CODE"]  # only last measurement loc per index will be kept

    return pd.concat(
        [
            spatial_interpolate(river_vertices, location_code_by_index, measurements, the_time)
            for the_time in measurements["DATUMTIJD"].unique()
        ], axis=1
    ).round(3)


def create_interpolated_locations(river_line: LineString) -> gpd.GeoDataFrame:
    river_vertices = MultiPoint([Point(xy) for xy in river_line.coords])
    locations = gpd.GeoDataFrame(geometry=list(river_vertices.geoms))

    # TODO use a generic function for this scaling
    locations["SCALED_X"] = (locations.geometry.x - 155_000) / (325_000 / 2)
    locations["SCALED_Y"] = (locations.geometry.y - 463_000) / (325_000 / 2)
    locations = locations.round(5)

    # add Torben's special request: mark the first and last row to prevent wrong visual smoothing
    sr = [1 for _ in range(len(locations))]
    sr[0] = 0
    sr[-1] = 0
    locations["SR"] = sr

    return locations.drop(columns="geometry")


if __name__ == "__main__":
    locations = pd.read_csv(LOCATIONS_FILE)
    locations = gpd.GeoDataFrame(locations, geometry=gpd.points_from_xy(locations["X"], locations["Y"]), crs=28992)
    measurements = pd.read_csv(MEASUREMENTS_FILE)
    river_lines = gpd.read_file(RIVER_LINES_FILE).to_crs(epsg=28992)

    with Pool(processes=8) as pool:
        int_locations = pool.map(create_interpolated_locations, river_lines.geometry)

    pd.concat(int_locations).to_csv("output/river_locations.csv", index=False)

    with Pool(processes=8) as pool:
        int_measurements = pool.starmap(
            create_interpolated_measurements,
            [(locations, measurements, river_line) for river_line in river_lines.geometry],
        )

    pd.concat(int_measurements).to_csv("output/river_measurements.csv", index=False)
