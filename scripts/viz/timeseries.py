# generic time series plotting with streamlit

# interesting locs for 'waterhoogtes_long': CREV, BG2
from pathlib import Path

import numpy as np
import streamlit as st
import pandas as pd


@st.cache_data()
def load_data(path: Path | str, sep: str = ";") -> pd.DataFrame:
    return pd.read_csv(path, sep=sep)


def diff_data(df: pd.DataFrame, y_column: str) -> pd.DataFrame:
    out = df.copy()
    ser = out[y_column].astype(float)
    ser.replace(999_999_999, np.nan, inplace=True)  # RWS value for missing data
    diffs = ser.diff().abs()
    print(f"Mean: {diffs.mean()}")
    print(f"Median: {diffs.median()}")
    out[y_column] = diffs
    return out


def clean_data(df: pd.DataFrame, y_column: str) -> pd.DataFrame:
    out = df.copy()
    ser = out[y_column].astype(float)

    ser.replace(999_999_999, np.nan, inplace=True)  # RWS value for missing data

    abs_diff = ser.diff().abs()
    mean_abs_diff = abs_diff.mean()
    outliers = abs_diff >= 5 * mean_abs_diff
    ser[outliers] = np.nan

    ser.interpolate(method="linear", inplace=True)
    out[y_column] = ser
    return out


# APP

st.title("Generic time series plotter")
st.markdown("_For long table CSV files_")
st.subheader("Select CSV")

csv_path_options = [p for p in Path().rglob("*.csv") if "venv" not in p.parts]
dataset_path = st.selectbox("Select dataset", csv_path_options)

csv_sep = st.text_input("CSV separator", ";")

st.subheader("Data")
df = load_data(dataset_path, csv_sep)
column_names = df.columns.values.tolist()
st.write(f"Record count: {len(df)}")
st.write(f"Columns: {column_names}")

if st.checkbox("Show raw data"):
    nrows = st.slider("Row count", 0, 100, 20, 1)
    st.write(df.head(nrows))

st.subheader("Columns")
x_column = st.selectbox("X column", column_names)
y_column = st.selectbox("Y column", column_names)
group_column = st.selectbox("Group column", column_names)

st.subheader("Plotz")
how_to_show = st.selectbox("How to show", ["single", "select", "list"], index=1)

if how_to_show == "single":
    st.markdown("Raw")
    st.line_chart(df, x=x_column, y=y_column, color=group_column)

    st.markdown("Clean")
    st.line_chart(clean_data(df, y_column), x=x_column, y=y_column, color=group_column)

elif how_to_show == "select":
    groups = sorted(df[group_column].unique())
    group = st.select_slider("Group", groups)
    tdf = df[df[group_column] == group]

    st.markdown("Raw")
    st.line_chart(tdf, x=x_column, y=y_column)

    st.markdown("Diff")
    st.line_chart(diff_data(tdf, y_column), x=x_column, y=y_column)

    st.markdown("Clean")
    st.line_chart(clean_data(tdf, y_column), x=x_column, y=y_column)

elif how_to_show == "list":
    groups = sorted(df[group_column].unique())
    for group in groups:
        st.markdown("### " + group)
        tdf = df[df[group_column] == group]

        st.markdown("Raw")
        st.line_chart(tdf, x=x_column, y=y_column)

        st.markdown("Clean")
        st.line_chart(clean_data(tdf, y_column), x=x_column, y=y_column)
