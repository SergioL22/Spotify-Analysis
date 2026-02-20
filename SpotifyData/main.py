import os
import sys
import pandas as pd
import spotipy
import spotipy.util as util
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import matplotlib.pyplot as plt
import json
import glob

# Load all Spotify streaming history JSON files
all_files = glob.glob(os.path.join("Spotify Account Data", "StreamingHistory_music_*.json"))

if not all_files:
    print("No streaming history files found in 'Spotify Account Data'. Check the path.")
    sys.exit(1)

dfs = []
for file in all_files:
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
        if not data:
            # skip empty files
            continue
        dfs.append(pd.DataFrame(data))

if not dfs:
    print("No data found in the matched streaming history files.")
    sys.exit(1)

try:
    spotify_df = pd.concat(dfs, ignore_index=True)
except ValueError as e:
    print("Error concatenating dataframes:", e)
    sys.exit(1)

print(f"Total Streams: {len(spotify_df)}")
print(spotify_df)   

# Load Wrapped 2025 data
w = json.load(open("Spotify Account Data/Wrapped2025.json", encoding = "utf-8"))
print(w.get("topGenres"))

# Genre enrichment using Spotify API with caching and env-var checks
cache_file = os.path.join("Spotify Account Data", "artist_genres.json")
artist_genres = {}

# Try load cache first
if os.path.exists(cache_file):
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            artist_genres = json.load(f)
    except Exception:
        artist_genres = {}

# If cache empty, and credentials available, query Spotify and save cache
if not artist_genres:
    client_id = os.environ.get("SPOTIPY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
    if client_id and client_secret:
        sp = Spotify(client_credentials_manager = SpotifyClientCredentials())
        def get_artist_genre(name):
            try:
                r = sp.search(q=f"artist:{name}", type="artist", limit=1)
                items = r.get("artists", {}).get("items", [])
                return items[0].get("genres", []) if items else []
            except Exception:
                return []

        artists = spotify_df["artistName"].unique()
        for a in artists:
            artist_genres[a] = get_artist_genre(a)
        # persist cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(artist_genres, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    else:
        print("Spotify credentials not found and no artist_genres cache available; genre enrichment skipped.")

# Map genres to dataframe. `genre_list` will store lists; `genre` stores primary genre (first) or None
spotify_df["genre_list"] = spotify_df["artistName"].map(lambda a: artist_genres.get(a, []))
spotify_df["genre"] = spotify_df["genre_list"].apply(lambda l: l[0] if l else None)


# Required columns check
required_cols = {"endTime", "msPlayed", "artistName", "trackName"}
missing = required_cols - set(spotify_df.columns)
if missing:
    print(f"Missing required columns in data: {missing}")
    sys.exit(1)

# Convert 'endTime' to datetime
spotify_df["endTime"] = pd.to_datetime(spotify_df["endTime"])

spotify_df["date"] = spotify_df["endTime"].dt.date
spotify_df["year"] = spotify_df["endTime"].dt.year
spotify_df["month"]= spotify_df["endTime"].dt.month
spotify_df["month_name"] =spotify_df["endTime"].dt.month_name()
spotify_df["day"] =spotify_df["endTime"].dt.day
spotify_df["day_name"] =spotify_df["endTime"].dt.day_name()
spotify_df["day_of_year"] = spotify_df["endTime"].dt.dayofyear
spotify_df["hour"] = spotify_df["endTime"].dt.hour

spotify_df["time"] = spotify_df["endTime"].dt.time

#Skip flag
spotify_df["skipped"] = spotify_df["msPlayed"] == 0 

print("\nData with new time columns:")
print(spotify_df.describe())

# Top artists by minutes played
artist_stats = spotify_df.groupby("artistName")["msPlayed"].sum().sort_values(ascending=False)
artist_stats_minutes = artist_stats / 60000  # Convert milliseconds to minutes
print("\n Top 10 Artists by Total Minutes Played:")
print(artist_stats_minutes.head(10))

# Top songs by minutes played
song_stats = spotify_df.groupby(["artistName", "trackName"])["msPlayed"].sum().sort_values(ascending=False)
song_stats_minutes = song_stats / 60000
print("\n Top 10 songs played by minutes:")
print(song_stats_minutes.head(10))

# Alernative: by number of streams
artist_stats_by_count = spotify_df["artistName"].value_counts().head(10)
print("\n Top 10 Artists by Number of Streams:")
print(artist_stats_by_count)

top_songs_by_count = spotify_df.groupby(["artistName", "trackName"]).size().sort_values(ascending=False).head(10)
print("\n Top 10 Songs by Number of Streams:") 
print(top_songs_by_count) 

  
#---------------------------------------------
# Listening Time by different time periods
# --------------------------------------------

# We convert msPlayed to minutes for easier interpretation
spotify_df["minutesPlayed"] = spotify_df["msPlayed"] / 60000

# Listening time by hour
print("\n Listening Time by Hour:")
hourly_stats = spotify_df.groupby("hour")["minutesPlayed"].agg(["sum","count"]).round(2)
hourly_stats.columns = ["Total Minutes", "Number of Plays"]
print(hourly_stats)

# Listening time by day of week
print("\n Listening Time by Day of Week:")
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
daily_stats = spotify_df.groupby("day_name")["minutesPlayed"].agg(["sum", "count"]).round(2)
daily_stats.columns = ["Total Minutes", "Number of Plays"]
daily_stats = daily_stats.reindex(day_order)
print(daily_stats)

# Listening time by month
print("\n Listening Time by Month:")
month_order = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
monthly_stats = spotify_df.groupby("month_name")["minutesPlayed"].agg(["sum","count"])
monthly_stats.columns = ["Total Minutes", "Number of Plays"]
monthly_stats = monthly_stats.reindex(month_order).fillna(0).round(2)
print(monthly_stats)

# Listening time by year
print("\n Listening Time by Year:")
yearly_stats = spotify_df.groupby("year")["minutesPlayed"].agg(["sum","count"])
yearly_stats.columns = ["Total Minutes", "Number of Plays"]
yearly_stats = yearly_stats.fillna(0).round(2)
print(yearly_stats)

print(f"\n Total Listening Time: {spotify_df['minutesPlayed'].sum():.2f} minutes({spotify_df['minutesPlayed'].sum() / 60:.2f} hours)")


# ------------------------------
# Skipped songs analysis
# ------------------------------

# Skipped vs Completed songs/artists
skipped_df = spotify_df[spotify_df['msPlayed'] == 0]
completed_df = spotify_df[spotify_df['msPlayed'] > 0]

# Top skipped songs 
skipped_songs = skipped_df.groupby(["artistName","trackName"]).size().sort_values(ascending=False).head(10)
print("\n Top 10 Skipped Songs:")
print(skipped_songs)

# Top completed songs
completed_songs = completed_df.groupby(["artistName","trackName"]).size().sort_values(ascending=False).head(10)
print("\n Top 10 Completed Songs:")
print(completed_songs)

# Top skipped artists
skipped_artists = skipped_df["artistName"].value_counts().head(10)
print("\n Top 10 Skipped Artists:")
print(skipped_artists)

# Top completed artists
completed_artists = completed_df["artistName"].value_counts().head(10)
print("\n Top 10 Completed Artists:")
print(completed_artists)


# ------------------------------
# Session Analysis
# ------------------------------

# Session analysis: define a session as a continuous listening period with less than 30 minutes between streams
spotify_df = spotify_df.sort_values("endTime")
spotify_df["prev_endTime"] = spotify_df["endTime"].shift(1)
spotify_df['gap_minutes'] = (spotify_df["endTime"] - spotify_df['prev_endTime']).dt.total_seconds() / 60
spotify_df["new_session"] = (spotify_df["gap_minutes"] > 30) | (spotify_df["gap_minutes"].isna())
spotify_df["session_id"] = spotify_df['new_session'].cumsum()

# Session statistics
session_lengths = spotify_df.groupby("session_id")["minutesPlayed"].sum()
print("\nSession Length Statistics:")
print(f"Average Session Length: {session_lengths.mean():.2f} minutes")
print(f"Longest Session: {session_lengths.max():.2f} minutes")
print(f"Total Number of Sessions: {session_lengths.count()}")

# ------------------------------
# Monthly Trends
# ------------------------------

# Monthly Trends
monthly_trend = spotify_df.groupby('month_name')['minutesPlayed'].sum().reindex(month_order).fillna(0)
print("\nMonthly Listening Trend (minutes):")
print(monthly_trend)

# Yearly Trends
yearly_trend = spotify_df.groupby('year')['minutesPlayed'].sum()
print("\nYearly Listening Trend (minutes):")
print(yearly_trend)

# -----------------------------
# Genre Analysis
# -----------------------------

# Genre Analysis (if available)
if 'genre' in spotify_df.columns:
    # Primary-genre summary (first genre per artist)
    genre_stats = spotify_df.groupby('genre')['minutesPlayed'].sum().sort_values(ascending=False)
    print("\nTop Genres by Listening Time (primary genre):")
    print(genre_stats.head(10))

    # Explode approach: count all genres associated with each stream
    if 'genre_list' in spotify_df.columns:
        exploded = spotify_df.explode('genre_list')
        exploded = exploded.rename(columns={'genre_list': 'exploded_genre'})
        exploded['exploded_genre'] = exploded['exploded_genre'].fillna('Unknown')
        exploded_stats = exploded.groupby('exploded_genre')['minutesPlayed'].sum().sort_values(ascending=False)
        print("\nTop Genres by Listening Time (all assigned genres, exploded):")
        print(exploded_stats.head(20))

# Playlist Analysis (if available)
if 'playlistName' in spotify_df.columns:
    playlist_stats = spotify_df.groupby('playlistName')['minutesPlayed'].sum().sort_values(ascending=False)
    print("\nTop Playlists by Listening Time:")
    print(playlist_stats.head(10))


# ----------------------------------------------
# Visualizations
# ----------------------------------------------

# We create a figure with subplots 
fig,axes = plt.subplots(2,2, figsize = (15,10))

# First subplot: Listening time by hour
hourly_data = spotify_df.groupby("hour")["minutesPlayed"].sum()
axes[0,0].bar(hourly_data.index, hourly_data.values, color="skyblue")
axes[0,0].set_xlabel("Hour of Day")
axes[0,0].set_ylabel("Total Minutes Played")
axes[0,0].set_title("Listening Time by Hour")
axes[0,0].grid(axis = "y", alpha = 0.3)

# Second subplot: Listening time by day of week
daily_data = spotify_df.groupby("day_name")["minutesPlayed"].sum().reindex(day_order)
axes[0,1].bar(range(len(daily_data)), daily_data.values, color="salmon")
axes[0,1].set_xticks(range(len(daily_data)))
axes[0,1].set_xticklabels([day[:3] for day in daily_data.index],rotation = 45)
axes[0,1].set_xlabel("Day of Week")
axes[0,1].set_ylabel("Total Minutes Played")
axes[0,1].set_title("Listening Time by Day of Week")
axes[0,1].grid(axis = "y", alpha = 0.3)

# Third subplot: Listening time by month
monthly_data = spotify_df.groupby("month_name")["minutesPlayed"].sum().reindex(month_order)
axes[1,0].bar(range(len(monthly_data)), monthly_data.values, color="lightgreen")
axes[1,0].set_xticks(range(len(monthly_data)))
axes[1,0].set_xticklabels([month[:3]for month in monthly_data.index], rotation = 45)
axes[1,0].set_xlabel("Month")
axes[1,0].set_ylabel("Total Minutes Played")
axes[1,0].set_title("Listening Time by Month")
axes[1,0].grid(axis = "y", alpha = 0.3)

# Forth subplot: Top 10 Artists by Minutes Played
# Add artist name and percentage to legend

# Calculate top artists and their percentages
top_artists = spotify_df.groupby("artistName")['minutesPlayed'].sum().nlargest(10)
pie_values = top_artists.values
pie_labels = top_artists.index
pie_percentages = pie_values / pie_values.sum() * 100
legend_labels = [f"{name} ({pct:.1f}%)" for name, pct in zip(pie_labels, pie_percentages)]
axes[1,1].pie(pie_values, autopct="%1.1f%%", startangle=90)
axes[1,1].legend(legend_labels, loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
axes[1,1].set_title("Top 10 Artists by Minutes Played")

plt.tight_layout()
plt.show()