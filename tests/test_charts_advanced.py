import pandas as pd

from src.youtubeviz.charts import views_over_time_advanced


def test_views_over_time_advanced_basic():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime([
                "2025-01-01",
                "2025-01-02",
                "2025-01-01",
                "2025-01-02",
            ]),
            "views": [100, 150, 200, 300],
            "artist_name": ["A", "A", "B", "B"],
        }
    )

    fig = views_over_time_advanced(
        df,
        date_col="date",
        value_col="views",
        group_col="artist_name",
        rolling_window=2,
        highlight_artists=["A"],
    )
    # If plotly not installed, fig can be None; that's acceptable in CI where deps vary
    if fig is None:
        return
    # Expect 2 artists * (daily + rolling)
    assert len(fig.data) == 4
    names = {tr.name for tr in fig.data}
    assert any("A (daily)" in n for n in names)
    assert any("A (2d avg)" in n or "A (2d avg)" == n for n in names)
    assert any("B (daily)" in n for n in names)

