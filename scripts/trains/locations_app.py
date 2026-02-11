import time
from datetime import datetime, timedelta

import polars as pl
import streamlit as st

from artistic_intelligence_data.constants import DEFAULT_TIMEZONE
from artistic_intelligence_data.trains.questdb_train_provider import QuestDBTrainProvider

TRAIN_COLOR_MAP = {
    "IC": "#0000ff",  # blue
    "SPR": "#ffff00",  # yellow
    "ARR": "#ff0000",  # red
}

TRAIN_PROVIDER = QuestDBTrainProvider()

# START APP

st.write("# TRAINS")

start_time = datetime.now(tz=DEFAULT_TIMEZONE).replace(hour=4, minute=0, second=0, microsecond=0)
end_time = start_time.replace(hour=18, minute=0, second=0, microsecond=0)

timeslider_placeholder = st.empty()
timeinfo_placeholder = st.empty()

selected_time = timeslider_placeholder.slider(
    "Select a time today",
    # value=start_time,
    min_value=start_time,
    max_value=end_time,
    step=timedelta(seconds=10),
    format="HH:mm:ss",
    key="timeslider",
)

timeinfo_placeholder.info(f"Selected time: {selected_time}")

if st.button("Play"):
    while selected_time < end_time:
        time.sleep(1)
        selected_time = selected_time + timedelta(seconds=10)
        timeslider_placeholder.slider(
            "Select a time today", start_time, end_time, timedelta(seconds=10), format="HH:mm:ss", key="timeslider"
        )
        timeinfo_placeholder.info(f"Selected time: {selected_time}")


trains = TRAIN_PROVIDER.get_train_locations(selected_time, selected_time).with_columns(
    pl.col("train_type").replace_strict(TRAIN_COLOR_MAP).alias("color")
)

st.map(data=trains.to_pandas(), latitude="lat", longitude="lng", zoom=6, color="color", size=1)

st.info(f"Number of trains at selected time: {trains.height}")
