import os
import glob
import json
import pandas as pd


def load_streaming_history(data_dir="Spotify Account Data"):
    pattern = os.path.join(data_dir, "StreamingHistory_music_*.json")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found with pattern: {pattern}")

    dfs = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if not data:
                continue
            dfs.append(pd.DataFrame(data))

    if not dfs:
        raise ValueError("No data loaded from streaming history files")

    df = pd.concat(dfs, ignore_index=True)
    return df


def enrich_time_columns(df):
    df = df.copy()
    # required column check
    required = {"endTime", "msPlayed", "artistName", "trackName"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")

    df["endTime"] = pd.to_datetime(df["endTime"], errors="coerce")
    df = df.dropna(subset=["endTime"])  # drop rows with bad timestamps

    df["date"] = df["endTime"].dt.date
    df["year"] = df["endTime"].dt.year
    df["month"] = df["endTime"].dt.month
    df["month_name"] = df["endTime"].dt.month_name()
    df["day"] = df["endTime"].dt.day
    df["day_name"] = df["endTime"].dt.day_name()
    df["hour"] = df["endTime"].dt.hour
    df["minutesPlayed"] = df["msPlayed"].fillna(0) / 60000.0
    df["skipped"] = df["msPlayed"] == 0

    return df


def save_csv(df, out_path="data/spotify_streams.csv"):
    out_dir = os.path.dirname(out_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)
    df.to_csv(out_path, index=False)
    print(f"Saved processed data to: {out_path}")


def main():
    df = load_streaming_history()
    df = enrich_time_columns(df)
    save_csv(df)


if __name__ == "__main__":
    main()
