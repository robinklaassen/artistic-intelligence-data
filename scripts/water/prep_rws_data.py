"""Data preparation on CSV 'waterhoogte' data from Rijkswaterstaat."""

import geopandas as gpd
import numpy as np
import pandas as pd

from aid.scaling import scale_rd_to_touch
from repo_path import REPO_PATH

INPUT_FILENAME = "20240114_015.csv"  # hoogwater rijn vorig jaar
# INPUT_FILENAME = "20250329_011.csv"  # noordwaard
# INPUT_FILENAME = "20250402_044.csv"  # laagwater augustus 2022
# INPUT_FILENAME = "20250402_047.csv"  # eind maart 2025
INPUT_PATH = REPO_PATH / "notebooks" / "data" / INPUT_FILENAME
INPUT_NORMAL_LEVELS_PATH = REPO_PATH / "notebooks" / "data" / "rws_normaalstanden.csv"

OUTPUT_BASE_NAME = "hoogwater"
OUTPUT_BASE_PATH = REPO_PATH / "scripts" / "water" / "output"
OUTPUT_LOCATIONS = OUTPUT_BASE_PATH / f"{OUTPUT_BASE_NAME}_locaties.csv"
OUTPUT_DATA_LONG = OUTPUT_BASE_PATH / f"{OUTPUT_BASE_NAME}_long.csv"
OUTPUT_DATA_PIVOTED = OUTPUT_BASE_PATH / f"{OUTPUT_BASE_NAME}_pivoted.csv"


def general_prep(data: pd.DataFrame) -> pd.DataFrame:
    data.dropna(axis="columns", how="all", inplace=True)

    # add proper datetime column
    data["DATUMTIJD"] = pd.to_datetime(
        data["WAARNEMINGDATUM"] + " " + data["WAARNEMINGTIJD (MET/CET)"],
        format="%d-%m-%Y %H:%M:%S",
    )

    # convert X and Y to proper floats
    for c in ["X", "Y"]:
        data[c] = data[c].str.replace(",", ".").astype(float)

    # remove rows where X or Y is 0
    data = data[data.X != 0]
    data = data[data.Y != 0]

    return data


def create_locations(data: pd.DataFrame) -> gpd.GeoDataFrame:
    key_col = "LOCATIE_CODE"
    attr_cols = ["MEETPUNT_IDENTIFICATIE", "X", "Y"]

    locations = data.groupby(by=key_col)[attr_cols].first()
    locations = gpd.GeoDataFrame(locations,
                                 geometry=gpd.points_from_xy(locations["X"], locations["Y"], crs="EPSG:25831"))

    locations = locations.to_crs(epsg=28992)  # convert from ETRS to RD-new
    locations["X"] = locations.geometry.x
    locations["Y"] = locations.geometry.y

    locations["SCALED_X"], locations["SCALED_Y"] = scale_rd_to_touch(locations.geometry.x, locations.geometry.y)
    locations = locations.round(5)

    return locations.drop(columns="geometry")


def create_measurements(data: pd.DataFrame) -> pd.DataFrame:
    # pivot to location per column
    meas = data.pivot_table(index="DATUMTIJD", values="NUMERIEKEWAARDE", columns="LOCATIE_CODE")
    meas.replace(999_999_999.0, np.nan, inplace=True)  # RWS marks bad values with high 9s
    meas.dropna(axis="columns", how="all", inplace=True)

    # remove outliers by looking at mean of absolute diff
    abs_diff = meas.diff().abs()
    outliers = abs_diff >= 5 * abs_diff.mean()
    meas[outliers] = np.nan

    # resample to 10 minute intervals
    meas.index = pd.to_datetime(meas.index)
    meas = meas.resample("10min").mean()

    # fill gaps by interpolation
    meas.interpolate(axis=0, method="linear", limit_direction="both", inplace=True)

    # SCALING: normalize based on first X days of data
    normalize_number_of_days = 3
    meas_first_days = meas[0:normalize_number_of_days*24*6]
    mx = meas_first_days.max()
    mn = meas_first_days.min()

    meas_norm = (meas-mn)/(mx-mn)
    meas_norm[meas_norm < 0] = 0  # avoid negative values, for touch
    meas_norm[meas_norm == np.inf] = np.nan
    meas_norm.dropna(axis="columns", how="any", inplace=True)

    # back to long table
    meas_long = meas_norm.reset_index().melt(id_vars="DATUMTIJD", value_name="WAARDE_GESCHAALD")

    return meas_long


if __name__ == "__main__":
    data = pd.read_csv(INPUT_PATH, sep=";", encoding="iso-8859-1")
    normals = pd.read_csv(INPUT_NORMAL_LEVELS_PATH, sep=",", encoding="utf-8")

    data = general_prep(data)

    locations = create_locations(data)
    locations.to_csv(
        OUTPUT_LOCATIONS,
        sep=",",
        encoding="utf-8"
    )

    measurements = create_measurements(data)
    measurements.to_csv(OUTPUT_DATA_LONG, sep=",", encoding="utf-8", index=False)

    # pivoted data for touch
    meas_piv = measurements.pivot_table(index="LOCATIE_CODE", values="WAARDE_GESCHAALD", columns="DATUMTIJD").round(4)
    meas_piv.to_csv(OUTPUT_DATA_PIVOTED, sep=",", encoding="utf-8")
