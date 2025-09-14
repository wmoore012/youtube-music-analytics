from __future__ import annotations

import json
import os


def test_get_artist_color_map_stable_and_env_override(monkeypatch):
    from icatalogviz.charts import get_artist_color_map

    artists = ["B", "A", "C"]
    # No env → palette assigned deterministically by sorted order
    cmap1 = get_artist_color_map(artists)
    cmap2 = get_artist_color_map(list(reversed(artists)))
    assert cmap1 == cmap2
    # Env override wins for provided keys
    override = {"A": "#000000"}
    monkeypatch.setenv("ARTIST_COLORS_JSON", json.dumps(override))
    cmap3 = get_artist_color_map(artists)
    assert cmap3["A"] == "#000000"


def test_read_rpm_from_env_and_compute(monkeypatch):
    import pandas as pd

    from icatalogviz.data import compute_estimated_revenue, read_rpm_from_env

    # Default RPM via env
    monkeypatch.setenv("REVENUE_RPM_DEFAULT", "4.0")
    monkeypatch.delenv("REVENUE_RPM_MAP_JSON", raising=False)
    default, mapping = read_rpm_from_env()
    assert default == 4.0 and mapping == {}

    # Per-artist override via env used when rpm_usd=None
    monkeypatch.setenv("REVENUE_RPM_MAP_JSON", json.dumps({"A": 5.0}))
    df = pd.DataFrame(
        {
            "artist_name": ["A", "A", "B"],
            "video_id": ["v1", "v2", "x"],
            "views": [1000, 500, 1000],
        }
    )
    out = compute_estimated_revenue(df, rpm_usd=None, per_video=True)
    # Artist A: per-video max views [1000, 500] with RPM 5.0 → (1.0+0.5)*5 = 7.5
    a = out[out["artist_name"] == "A"].iloc[0]
    assert a["est_revenue_usd"] == 7.5
    # Artist B: RPM default 4.0 → (1.0)*4 = 4.0
    b = out[out["artist_name"] == "B"].iloc[0]
    assert b["est_revenue_usd"] == 4.0
