Artist Colors
=============

Purpose
- Keep artist colors consistent across all interactive charts.

How it works
- The code reads a mapping from either:
  - `ARTIST_COLORS_JSON` (env var with a JSON object), or
  - `ARTIST_COLORS_FILE` (env var pointing to a JSON file path).

Examples
- Env JSON:
  - `ARTIST_COLORS_JSON='{"Artist A":"#1f77b4","Artist B":"#ff7f0e"}'`
- File-based:
  - Put your mapping in `configs/artist_colors.json` (example shipped).
  - Set `ARTIST_COLORS_FILE=configs/artist_colors.json` in `.env`.

Tips
- Use 6‑digit hex codes for consistent rendering.
- Keep names exactly as in your DB (`songs.artist` or `youtube_videos.channel_title`).
