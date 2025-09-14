from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import nbformat


def _has_marker(nb: nbformat.NotebookNode, marker: str) -> bool:
    for cell in nb.cells:
        src = "".join(cell.get("source", [])).lower()
        if marker.lower() in src:
            return True
    return False


def patch_etl_notebook(path: Path) -> bool:
    """Append a sentiment scoring section to ETL notebook if missing. Gated by ICATALOG_ENABLE_SENTIMENT=1."""
    if os.getenv("ICATALOG_ENABLE_SENTIMENT") != "1":
        return False
    nb = nbformat.read(str(path), as_version=4)
    marker = "Sentiment Scoring"
    if _has_marker(nb, marker):
        return False
    nb.cells.append(
        nbformat.v4.new_markdown_cell(
            "# Sentiment Scoring\n" "\n" "Run sentiment analysis on recent comments and update summaries."
        )
    )
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "from web.etl_entrypoints import run_sentiment_scoring\n"
            "stats = run_sentiment_scoring(batch_size=1000, loop=True, update_summary=True, snapshot_daily=True)\n"
            "print(stats)\n"
        )
    )
    nbformat.write(nb, str(path))
    return True


def patch_explore_notebook(path: Path) -> bool:
    """Append Sentiment vs Plays charts to Explore notebook if missing. Gated by ICATALOG_ENABLE_SENTIMENT=1."""
    if os.getenv("ICATALOG_ENABLE_SENTIMENT") != "1":
        return False
    nb = nbformat.read(str(path), as_version=4)
    marker = "Sentiment vs Plays"
    if _has_marker(nb, marker):
        return False
    nb.cells.append(
        nbformat.v4.new_markdown_cell(
            "# Sentiment vs Plays (Views)\n"
            "\n"
            "Compare average comment sentiment with maximum daily views per video."
        )
    )
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "import pandas as pd\n"
            "import numpy as np\n"
            "import matplotlib.pyplot as plt\n"
            "plt.style.use('seaborn-v0_8-whitegrid')\n"
            "from web.db_guard import get_engine\n\n"
            "eng = get_engine('private', ro=True)\n\n"
            "sql = '''\n"
            "SELECT v.video_id, v.title, v.channel_title, v.isrc,\n"
            "       s.avg_sentiment, m.max_views\n"
            "FROM youtube_sentiment_summary s\n"
            "JOIN (\n"
            "  SELECT video_id, MAX(view_count) AS max_views\n"
            "  FROM youtube_metrics\n"
            "  GROUP BY video_id\n"
            ") m USING (video_id)\n"
            "JOIN youtube_videos v ON v.video_id = s.video_id\n"
            "'''\n"
            "df_sv = pd.read_sql(sql, eng)\n"
            "print(f'Loaded {len(df_sv)} rows')\n"
        )
    )
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "# Scatter: sentiment vs log10(views)\n"
            "if not df_sv.empty:\n"
            "    dfp = df_sv.copy()\n"
            "    dfp['views_log10'] = np.log10(dfp['max_views'].clip(lower=1))\n"
            "    fig, ax = plt.subplots(figsize=(8,5))\n"
            "    sc = ax.scatter(dfp['views_log10'], dfp['avg_sentiment'],\n"
            "                     c='tab:purple', alpha=0.35, edgecolors='none')\n"
            "    ax.axhline(0.0, color='gray', lw=1, alpha=0.6)\n"
            "    ax.set_xlabel('log10(Max Views)')\n"
            "    ax.set_ylabel('Avg Sentiment (VADER)')\n"
            "    ax.set_title('Sentiment vs Popularity (per video)')\n"
            "    plt.show()\n"
            "else:\n"
            "    print('No sentiment/view data available')\n"
        )
    )
    nb.cells.append(
        nbformat.v4.new_code_cell(
            "# Bars: Top 15 by views with sentiment overlay\n"
            "if not df_sv.empty:\n"
            "    top = df_sv.sort_values('max_views', ascending=False).head(15)\n"
            "    labels = (top['channel_title'].fillna('') + ' — ' +\n"
            "              top['title'].fillna('')).str.slice(0, 40)\n"
            "    x = np.arange(len(top))\n"
            "    fig, ax = plt.subplots(figsize=(9,6))\n"
            "    bars = ax.bar(x, top['max_views'], color='tab:blue', alpha=0.7)\n"
            "    ax2 = ax.twinx()\n"
            "    ax2.plot(x, top['avg_sentiment'], color='tab:red', marker='o', linewidth=1.5, alpha=0.9)\n"
            "    ax.set_xticks(x)\n"
            "    ax.set_xticklabels(labels, rotation=45, ha='right')\n"
            "    ax.set_ylabel('Max Views')\n"
            "    ax2.set_ylabel('Avg Sentiment')\n"
            "    ax.set_title('Top 15 Videos by Views — Sentiment Overlay')\n"
            "    fig.tight_layout()\n"
            "    plt.show()\n"
            "else:\n"
            "    print('No data for top videos by views')\n"
        )
    )
    nbformat.write(nb, str(path))
    return True


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    etl_nb = root / "ETL.ipynb"
    explore_nb = root / "DB_Explore.ipynb"
    changed = False
    if etl_nb.exists():
        changed |= patch_etl_notebook(etl_nb)
    if explore_nb.exists():
        changed |= patch_explore_notebook(explore_nb)
    print("patched" if changed else "no changes")


if __name__ == "__main__":
    main()
