# Notebook Output Tables

Store intermediate CSV/JSON exports here so chart wiring can reuse them later.

Guidelines:
- Use snake_case file names and columns.
- Keep all columns; avoid destructive pruning.
- Prefer natural keys (artist_name, video_id, channel_id).
- Add a short note in the manifest or README when a table feeds a chart.
