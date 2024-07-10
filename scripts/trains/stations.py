import os

import pandas as pd
import geopandas as gpd
import requests
from dotenv import load_dotenv

NS_STATION_URL = "https://gateway.apiportal.ns.nl/nsapp-stations/v2"
REQUEST_TIMEOUT = 5  # seconds


def main():
    """
    Get all train stations from NS API and output desired values to csv.
    """
    load_dotenv()
    pd.options.display.max_columns = None
    pd.options.display.max_rows = 20

    response = requests.get(NS_STATION_URL,
                            headers={"Ocp-Apim-Subscription-Key": os.getenv("NS_API_KEY")},
                            timeout=REQUEST_TIMEOUT,
                            )
    records = response.json()["payload"]
    print(f"Got {len(records)} station records from API")

    for rec in records:
        rec["naam"] = rec["namen"]["lang"]

    gdf = gpd.GeoDataFrame.from_records(records)
    gdf = gdf[["code", "naam", "land", "lat", "lng", "radius", "naderenRadius", "stationType"]]
    gdf = gdf.set_geometry(gpd.points_from_xy(gdf["lng"], gdf["lat"], crs="wgs84"))
    gdf = gdf.to_crs(epsg=28992)  # Rijksdriehoek

    # scale to spoorwegsymfonie CRS
    gdf["x"] = (gdf.geometry.x - 155_000) / (325_000 / 2)
    gdf["y"] = (gdf.geometry.y - 463_000) / (325_000 / 2)
    gdf = gdf.round(5)

    # print(gdf.head())

    gdf[["code", "naam_lang", "land", "stationType", "x", "y", "radius", "naderenRadius"]].to_csv("output/stations.csv",
                                                                                                  sep=",",
                                                                                                  encoding="utf-8",
                                                                                                  index=False)


if __name__ == "__main__":
    main()
