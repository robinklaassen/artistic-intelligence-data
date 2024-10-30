import pandas as pd
import geopandas as gpd

INPUT_FILE = "output/ARR.csv"
TIME_ROUNDING_FREQ = "10s"
CREATE_MAP = False


def main():
    df = pd.read_csv(INPUT_FILE)
    print("Number of records in CSV: ", len(df))

    # Some data cleaning
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # df["timestamp"] = pd.Timestamp.fromisoformat(df["timestamp"])

    df.dropna(subset=["x", "y"], inplace=True)  # errors are thrown on empty X/Y
    print("Number of non-empty records: ", len(df))

    for col in ["line_planning_number", "journey_number", "vehicle_number"]:
        print(f"Unique counts for column '{col}': {len(df[col].unique())}")

    print(len(df))
    print(df.dtypes)
    print(df.head())

    # Scale x and y to a [-1,1] square area
    # RDS range is 0 < x 280 and 300 < y < 625 (km)
    # Center Amersfoort (155, 463) to (0, 0)
    df["x"] = (df["x"] - 155_000) / (325_000 / 2)
    df["y"] = (df["y"] - 463_000) / (325_000 / 2)

    df = df.round(5)
    print(df.head())

    if CREATE_MAP:
        # Create a map of the data
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.x, df.y), crs="EPSG:28992"
        )
        print(gdf.head())

        map = gdf.explore("journey_number", legend=True, tiles="cartodb positron")
        map.save("ARR_analysis.html")

    # Bin the timestamps
    df["timestamp"] = df["timestamp"].dt.round(freq=TIME_ROUNDING_FREQ)

    # Aggregate rows with same ID and timestamp
    df = df.groupby(by=["timestamp", "journey_number"])[["x", "y"]].mean()
    df = df.reset_index(names=["timestamp", "journey_number"])

    # Pivot
    df = df.melt(id_vars=["timestamp", "journey_number"], value_vars=["x", "y"], var_name="var")
    df = df.pivot(columns="journey_number", index=["timestamp", "var"], values="value")
    df = df.reset_index()
    # df["timestamp"] = df["timestamp"].dt.strftime("%H:%M:%S")
    print(df.head())
    df.to_csv("ARR_pivoted.csv", index=False)


if __name__ == "__main__":
    main()
