# scratch pad for getting data via zeromq from NDOV

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from gzip import GzipFile
from io import BytesIO

import geopandas
import geopandas as gpd
import numpy as np
import pandas as pd
import zmq

NAMESPACES = {
    "": "http://bison.connekt.nl/tmi8/kv6/msg"  # default namespace for KV6 document
}

TRANSPORT_PROVIDER = "ARR"
DURATION_MINUTES = 120
VISUALIZE_ON_MAP = False


def handle_xml(data: str) -> list[dict]:
    root = ET.fromstring(data)
    pos_info = root.find("KV6posinfo", NAMESPACES)
    # print(f"Pos info element: {pos_info}")
    records = []
    for element in pos_info:
        record = {
            "type": element.tag.split("}")[1],
            "timestamp": _element_value(element, "timestamp"),
            "line_planning_number": _element_value(element, "lineplanningnumber"),
            "journey_number": _element_value(element, "journeynumber"),
            "vehicle_number": _element_value(element, "vehiclenumber"),
            "x": _element_value(element, "rd-x"),
            "y": _element_value(element, "rd-y"),
        }
        records.append(record)

    return records


def _element_value(element: ET.Element, path: str) -> str | None:
    return el.text if (el := element.find(path, NAMESPACES)) is not None else None


def _create_map(df: pd.DataFrame):
    df.dropna(inplace=True)  # errors are thrown on empty X/Y
    gdf = gpd.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.x, df.y), crs="EPSG:28992"
    )

    return gdf.explore("journey_number", legend=True, tiles="cartodb positron")


if __name__ == "__main__":
    context = zmq.Context()

    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://pubsub.besteffort.ndovloket.nl:7658")
    subscriber.setsockopt_string(zmq.SUBSCRIBE, f"/{TRANSPORT_PROVIDER}/KV6posinfo")

    output = []
    time_start = datetime.now()
    time_end = time_start + timedelta(minutes=DURATION_MINUTES)

    while datetime.now() < time_end:
        multipart = subscriber.recv_multipart()
        address = multipart[0].decode("utf-8")
        contents = b''.join(multipart[1:])
        try:
            contents = GzipFile(None, 'r', 0, BytesIO(contents)).read().decode("utf-8")
            # print('GZIP', address, contents)
            records = handle_xml(contents)
            output.extend(records)
        except Exception:
            print('NOT ', address, contents)
            raise

    subscriber.close()
    context.term()

    print(f"Got {len(output)} records in output, in {(datetime.now() - time_start).total_seconds()} seconds")

    # save data to csv file
    df = pd.DataFrame.from_records(output)
    df.to_csv(f"{TRANSPORT_PROVIDER}.csv", index=False)

    if VISUALIZE_ON_MAP:
        map = _create_map(df)
        map.save(f"{TRANSPORT_PROVIDER}.html")
