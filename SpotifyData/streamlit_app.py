import os
import pandas as pd
import streamlit as st
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt


DATA_PATH = os.path.join("data", "spotify_streams.csv")


@st.cache_data
def load_data(path=DATA_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Processed data not found at {path}. Run data_processing.py first.")
    df = pd.read_csv(path, parse_dates=["endTime"]) 
    return df


def top_artists(df, top_n=10):
    s = df.groupby("artistName")["minutesPlayed"].sum().nlargest(top_n).reset_index()
    return s


def listening_over_time(df, freq="D"):
    series = df.set_index("endTime").resample(freq)["minutesPlayed"].sum().reset_index()
    return series


def hour_week_heatmap(df):
    pivot = df.groupby(["day_name", "hour"])["minutesPlayed"].sum().unstack(fill_value=0)
    # ensure weekday order
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    pivot = pivot.reindex(order)
    return pivot


def main():
    st.title("Spotify Listening Visuals")
    st.sidebar.header("Filters")
    df = load_data()

    # Filters
    min_date = pd.to_datetime(df["endTime"]).min().date()
    max_date = pd.to_datetime(df["endTime"]).max().date()
    date_range = st.sidebar.date_input("Date range", [min_date, max_date])
    if len(date_range) == 2:
        start, end = date_range
        df = df[(pd.to_datetime(df["endTime"]).dt.date >= start) & (pd.to_datetime(df["endTime"]).dt.date <= end)]

    st.sidebar.markdown("---")
    top_n = st.sidebar.slider("Top N artists/tracks", 5, 50, 10)

    st.header("Top Artists")
    ta = top_artists(df, top_n)
    fig1 = px.bar(ta, x="minutesPlayed", y="artistName", orientation="h", title=f"Top {top_n} Artists by Minutes Played")
    st.plotly_chart(fig1, use_container_width=True)

    st.header("Listening Over Time")
    freq = st.selectbox("Aggregation frequency", ["D", "W", "M"], index=0, format_func=lambda x: {"D":"Daily","W":"Weekly","M":"Monthly"}[x])
    tot = listening_over_time(df, freq=freq)
    fig2 = px.line(tot, x="endTime", y="minutesPlayed", title="Listening Over Time")
    st.plotly_chart(fig2, use_container_width=True)

    st.header("Hour vs Day Heatmap")
    pivot = hour_week_heatmap(df)
    fig3 = px.imshow(pivot, labels=dict(x="Hour", y="Day of Week", color="Minutes Played"), x=pivot.columns, y=pivot.index, aspect="auto")
    st.plotly_chart(fig3, use_container_width=True)

    st.header("Playback Duration Distribution")
    st.write("Distribution of minutes played per stream (skips appear near zero)")
    fig4 = px.violin(df, y="minutesPlayed", box=True, points="outliers", title="Minutes Played Distribution")
    st.plotly_chart(fig4, use_container_width=True)


if __name__ == "__main__":
    main()
