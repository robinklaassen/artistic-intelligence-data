from typing import Literal

import requests

import pandas as pd
import geopandas as gpd
from geopandas import points_from_xy


def get_measurement_points() -> gpd.GeoDataFrame:
    response = requests.get(url="https://pegelonline-int.wsv.de/webservices/rest-api/v2/stations.json", params={
        "waters": "RHEIN",
        "prettyprint": "false",
    })
    response.raise_for_status()
    data = response.json()
    print(f"Retrieved {len(data)} measurement points")

    df = pd.DataFrame.from_records(data)
    df.dropna(subset=["longitude", "latitude"], how="any", inplace=True)

    gdf = gpd.GeoDataFrame(
        data=df, geometry=points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    # gdf = gdf[["uuid", "longname", "km", "longitude", "latitude"]]
    print(gdf.head())

    return gdf


def get_measurements(uuids: list[str], meas_type: Literal["W", "Q", "WT"] = "W") -> pd.DataFrame:
    """
    W = waterhoogtes in cm
    Q = debiet in m3/s
    WT = watertemperatuur in graden Celcius
    """
    dfs = []
    for uuid in uuids:
        response = requests.get(
            url=f"https://pegelonline-int.wsv.de/webservices/rest-api/v2/stations/{uuid}/{meas_type}/measurements.json",
            params={
                "start": "P7D",  # 7 days
                "prettyprint": "false",
            }
        )
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df["uuid"] = uuid
        dfs.append(df)
        print(f"{meas_type} measurements {uuid}")

    return pd.concat(dfs)


if __name__ == "__main__":
    pd.options.display.max_columns = None

    points = get_measurement_points()
    measurements = get_measurements(list(points.uuid))
    measurements.to_csv("output/waterhoogtes_rijn.csv", sep=";", encoding="utf-8", index=False)
