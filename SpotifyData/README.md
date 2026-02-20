# Spotify Data Visuals

This project processes Spotify streaming history JSON files exported from your account and produces interactive visuals using Streamlit.

Quick start

1. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

2. Process your streaming history JSONs (reads files from `Spotify Account Data`):

```bash
python data_processing.py
```

3. Run the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

Files added

- `data_processing.py` — loads all `StreamingHistory_music_*.json`, enriches time columns, and writes `data/spotify_streams.csv`.
- `streamlit_app.py` — simple interactive app showing Top Artists, Listening Over Time, Hour/Day heatmap, and a minutes-played distribution.
- `requirements.txt` — Python dependencies.

Notes

- If you want audio features (valence, energy, tempo), we can enrich tracks using the Spotify Web API and save them to the processed CSV. Add `SPOTIPY_CLIENT_ID` and `SPOTIPY_CLIENT_SECRET` env vars for that step.
- If your streaming data is large, the processing step may take a minute.
