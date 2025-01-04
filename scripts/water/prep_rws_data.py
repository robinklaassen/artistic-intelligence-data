"""Data preparation on CSV 'waterhoogte' data from Rijkswaterstaat."""

import geopandas as gpd
import numpy as np
import pandas as pd

from repo_path import REPO_PATH

INPUT_FILE = REPO_PATH / "notebooks" / "data" / "20240114_015.csv"


def general_prep(data: pd.DataFrame) -> pd.DataFrame:
    data = data.dropna(axis="columns", how="all")

    # add proper datetime column
    data["DATUMTIJD"] = pd.to_datetime(data["WAARNEMINGDATUM"] + " " + data["WAARNEMINGTIJD (MET/CET)"],
                                       format="%d-%m-%Y %H:%M:%S")

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

    locations = locations.to_crs(epsg=28992)
    locations["X"] = locations.geometry.x
    locations["Y"] = locations.geometry.y

    locations["SCALED_X"] = (locations.geometry.x - 155_000) / (325_000 / 2)
    locations["SCALED_Y"] = (locations.geometry.y - 463_000) / (325_000 / 2)
    locations = locations.round(5)

    return locations.drop(columns="geometry")


def create_measurements(data: pd.DataFrame) -> pd.DataFrame:
    # pivot to location per column
    meas = data.pivot_table(index="DATUMTIJD", values="NUMERIEKEWAARDE", columns="LOCATIE_CODE")
    meas.replace(999_999_999.0, np.nan, inplace=True)  # RWS marks bad values with high 9s

    # remove outliers by looking at mean of absolute diff
    abs_diff = meas.diff().abs()
    outliers = abs_diff >= 5 * abs_diff.mean()
    meas[outliers] = np.nan

    # fill gaps by interpolation
    meas.interpolate(axis=0, method="linear", limit_direction="both", inplace=True)

    # back to long table
    meas_long = meas.astype(int).reset_index().melt(id_vars="DATUMTIJD", value_name="NUMERIEKEWAARDE")
    return meas_long


if __name__ == "__main__":
    data = pd.read_csv(INPUT_FILE, sep=";", encoding="iso-8859-1")

    data = general_prep(data)

    locations = create_locations(data)
    locations.to_csv(
        "output/waterhoogtes_locaties.csv",
        sep=",",
        encoding="utf-8"
    )

    measurements = create_measurements(data)
    measurements.to_csv("output/waterhoogtes_long.csv", sep=",", encoding="utf-8", index=False)

    # pivoted data for touch
    meas_piv = measurements.pivot_table(index="LOCATIE_CODE", values="NUMERIEKEWAARDE", columns="DATUMTIJD").astype(int)
    meas_piv.to_csv("output/waterhoogtes_pivoted.csv", sep=",", encoding="utf-8")
