from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest


def test_compute_kpis_mean_median_mode():
    from icatalogviz.data import compute_kpis

    # Two artists with distinct per-video max views to test mean/median/mode
    rows = [
        # artist A, video v1 over days
        {"artist_name": "A", "video_id": "v1", "views": 10},
        {"artist_name": "A", "video_id": "v1", "views": 30},  # max 30
        # artist A, video v2
        {"artist_name": "A", "video_id": "v2", "views": 5},
        {"artist_name": "A", "video_id": "v2", "views": 5},  # max 5
        # artist B single video
        {"artist_name": "B", "video_id": "x", "views": 100},
        {"artist_name": "B", "video_id": "x", "views": 120},  # max 120
    ]
    df = pd.DataFrame(rows)
    k = compute_kpis(df)
    # Artist A: per-video max views = [30, 5] => total=35, videos=2, median=17.5, mean=17.5, mode ambiguous -> first value
    a = k[k["artist_name"] == "A"].iloc[0]
    assert a["total_views"] == 35
    assert a["videos"] == 2
    assert a["median_views"] == pytest.approx(17.5)
    assert a["mean_views"] == pytest.approx(17.5)
    # mode_views present (value could be 5 or 30 depending on pandas mode order)
    assert pd.notna(a["mode_views"])  # just existence check
    # Artist B
    b = k[k["artist_name"] == "B"].iloc[0]
    assert b["total_views"] == 120
    assert b["videos"] == 1
    assert b["median_views"] == 120
    assert b["mean_views"] == 120


def test_detect_outliers_iqr_grouped():
    from icatalogviz.data import detect_outliers_iqr

    # Two groups; one outlier high in A, one outlier low in B
    rows = []
    rows += [{"g": "A", "v": x} for x in [10, 11, 12, 50]]  # 50 should be high outlier
    rows += [{"g": "B", "v": x} for x in [100, 101, 102, 40]]  # 40 should be low outlier
    df = pd.DataFrame(rows)
    out = detect_outliers_iqr(df, value_col="v", group_col="g", factor=1.5)
    assert {tuple(x) for x in out[["g", "v"]].to_records(index=False)} == {("A", 50), ("B", 40)}


def test_box_whisker_plotly_returns_fig_or_df(monkeypatch):
    import icatalogviz.charts as charts

    df = pd.DataFrame(
        {
            "group": ["A", "A", "B", "B", "B"],
            "val": [1, 2, 2, 3, 30],
            "hover": ["h1", "h2", "h3", "h4", "h5"],
        }
    )
    # Normal path: plotly present -> returns a Figure
    fig = charts.box_whisker_plotly(df, value_col="val", group_col="group", hover_cols=["hover"])
    # Don't import plotly in tests; use duck typing
    assert hasattr(fig, "to_dict")

    # Fallback path: px None -> returns grouped stats DataFrame
    monkeypatch.setattr(charts, "px", None)
    stats = charts.box_whisker_plotly(df, value_col="val", group_col="group")
    assert isinstance(stats, pd.DataFrame)
    assert set(["group", "q1", "median", "q3"]).issubset(stats.columns)


def test_scatter_and_timeline_fallback_sort(monkeypatch):
    import icatalogviz.charts as charts

    # Force fallback path
    monkeypatch.setattr(charts, "px", None)

    # Timeline sorts by date and returns df
    tl_df = pd.DataFrame({"d": ["2025-09-12", "2025-09-10", "2025-09-11"], "y": [0.1, 0.2, 0.0]})
    out = charts.sentiment_timeline_plotly(tl_df, date_col="d", value_col="y")
    assert list(out["d"]) == ["2025-09-10", "2025-09-11", "2025-09-12"]


def test_sentiment_loaders_with_mocks(monkeypatch):
    import icatalogviz.data as data

    # Mock inspect.has_table -> False (no songs table)
    class _InspectMock:
        def __init__(self, _):
            pass

        def has_table(self, name: str) -> bool:
            return False

    monkeypatch.setattr(data, "inspect", _InspectMock)

    # Fake engine + connection context manager
    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Eng:
        def connect(self):
            return _Conn()

    monkeypatch.setattr(data, "_get_engine", lambda engine=None: _Eng())

    # Fake pandas.read_sql that returns different dfs based on SQL text
    def fake_read_sql(sql, conn, params=None, **kwargs):  # noqa: ANN001
        sql_text = str(sql)
        if "FROM youtube_sentiment_summary" in sql_text:
            return pd.DataFrame(
                {
                    "artist_name": ["A"],
                    "video_id": ["v1"],
                    "video_title": ["Song 1"],
                    "channel_title": ["ChA"],
                    "avg_sentiment": [0.2],
                    "comment_count": [123],
                    "last_updated": ["2025-09-12 10:00:00"],
                }
            )
        elif "FROM youtube_comments" in sql_text:
            return pd.DataFrame(
                {
                    "date": ["2025-09-10", "2025-09-11"],
                    "artist_name": ["A", "A"],
                    "avg_sentiment": [0.1, 0.3],
                    "comments": [10, 20],
                }
            )
        raise AssertionError("Unexpected SQL in test")

    monkeypatch.setattr(pd, "read_sql", fake_read_sql)

    ssum = data.load_sentiment_summary()
    assert set(["artist_name", "video_id", "video_title", "avg_sentiment", "comment_count", "last_updated"]).issubset(
        ssum.columns
    )
    # last_updated parsed to datetime
    assert pd.api.types.is_datetime64_any_dtype(ssum["last_updated"])

    sdaily = data.load_sentiment_daily()
    assert set(["date", "artist_name", "avg_sentiment", "comments"]).issubset(sdaily.columns)
    assert pd.api.types.is_datetime64_any_dtype(sdaily["date"])
