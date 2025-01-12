from multiprocessing import Pool
from time import perf_counter

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry.linestring import LineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from shapely.ops import nearest_points

from aid.scaling import scale_rd_to_touch
from repo_path import REPO_PATH

SCRIPTS_OUTPUT_PATH = REPO_PATH / "scripts" / "water" / "output"

LOCATIONS_FILE = SCRIPTS_OUTPUT_PATH / "waterhoogtes_locaties.csv"
# MEASUREMENTS_FILE = SCRIPTS_OUTPUT_PATH / "waterhoogtes_long.csv"
MEASUREMENTS_FILE = REPO_PATH / "notebooks" / "output" / "waterhoogtes_long.csv"  # TODO these are normalized measurements
RIVER_LINES_FILE = REPO_PATH / "notebooks" / "data" / "rivierlijnen.zip"

ENABLE_MULTIPROCESSING: bool = True


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

    # create empty dataframe with datetimes as index and river_vertices as columns
    datetime_range = measurements["DATUMTIJD"].unique()
    datetime_range.sort()
    vertex_range = [i for i in range(len(river_vertices.geoms))]
    int_measurements = pd.DataFrame(np.nan, index=datetime_range, columns=vertex_range)

    # assign measured values to columns
    for idx, code in location_code_by_index.items():
        values = measurements[measurements["LOCATIE_CODE"] == code].copy()
        values = values.set_index("DATUMTIJD")["WAARDE_GESCHAALD"]
        int_measurements[idx] = values

    int_measurements.interpolate(axis=1, limit_direction="both", inplace=True)

    return int_measurements.round(3).T  # return transposed so columns are datetimes


def create_interpolated_locations(river_line: LineString) -> gpd.GeoDataFrame:
    river_vertices = MultiPoint([Point(xy) for xy in river_line.coords])
    locations = gpd.GeoDataFrame(geometry=list(river_vertices.geoms))

    locations["SCALED_X"], locations["SCALED_Y"] = scale_rd_to_touch(locations.geometry.x, locations.geometry.y)
    locations = locations.round(5)

    # add Torben's special request: mark the first and last row to prevent wrong visual smoothing
    sr = [1 for _ in range(len(locations))]
    sr[0] = 0
    sr[-1] = 0
    locations["SR"] = sr

    return locations.drop(columns="geometry")


if __name__ == "__main__":
    start_time = perf_counter()
    locations = pd.read_csv(LOCATIONS_FILE)
    locations = gpd.GeoDataFrame(locations, geometry=gpd.points_from_xy(locations["X"], locations["Y"]), crs=28992)
    measurements = pd.read_csv(MEASUREMENTS_FILE)
    river_lines = gpd.read_file(RIVER_LINES_FILE).to_crs(epsg=28992)

    if ENABLE_MULTIPROCESSING:
        with Pool(processes=8) as pool:
            int_locations = pool.map(create_interpolated_locations, river_lines.geometry)
    else:
        int_locations = [create_interpolated_locations(river_line) for river_line in river_lines.geometry]

    pd.concat(int_locations).to_csv("output/river_locations.csv", index=False)

    if ENABLE_MULTIPROCESSING:
        with Pool(processes=8) as pool:
            int_measurements = pool.starmap(
                create_interpolated_measurements,
                [(locations, measurements, river_line) for river_line in river_lines.geometry],
            )
    else:
        int_measurements = [
            create_interpolated_measurements(locations, measurements, river_line) for river_line in river_lines.geometry
        ]

    pd.concat(int_measurements).to_csv("output/river_measurements.csv", index=False)

    print("Done in seconds: ", perf_counter() - start_time)
