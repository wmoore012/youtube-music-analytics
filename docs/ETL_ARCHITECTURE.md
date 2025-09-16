# ETL Architecture Notes

## DSP Caching (Daily)

- Path root: `ICATALOG_CACHE_DIR` or `./cache`
- Tidal: `cache/tidal_playlists/<playlist_id>.json`
- Spotify: `cache/spotify_playlists/<playlist_id>.json`
- Validity: Current UTC day only (stored as `cache_date` in file)
- Force fresh: `TIDAL_FORCE_FRESH=1` or `SPOTIFY_FORCE_FRESH=1`
- Observability: `FRESH_TIDAL_PULL` and `FRESH_SPOTIFY_PULL` exported to `os.environ` as `"0"/"1"`

## Entry Points

- YouTube ETL (videos, metrics, comments): `web/youtube_channel_etl.py`
- Sentiment scoring + summary: `web/sentiment_job.py` and `web/etl_entrypoints.py`
- DSP refresh (Spotify/Tidal -> song_versions): `tools/run_dsp_refresh.py`

## CI / Quality Gates

- Pre-commit: Black, isort, flake8, mypy (ignore-missing-imports)
- CI: `.github/workflows/ci.yml` runs pre-commit and unit tests on pushes/PRs
- Pytest timeouts applied to prevent hangs

## Notes

- No hidden fallbacks: extractors require `spotipy` or `tidalapi` installed, else fail clearly.
- DB timeouts are set on MySQL connections to avoid hangs under failure modes.
