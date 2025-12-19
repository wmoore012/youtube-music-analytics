import json
import math
from pathlib import Path

from tools.web.export_frontend_insights import build_insights, _write_json


def _assert_finite_numbers(payload):
    if isinstance(payload, dict):
        for value in payload.values():
            _assert_finite_numbers(value)
        return
    if isinstance(payload, list):
        for value in payload:
            _assert_finite_numbers(value)
        return
    if isinstance(payload, float):
        assert math.isfinite(payload)


def test_build_insights_structure(tmp_path):
    insights = build_insights(
        artist_summary_path=Path("music_analysis_tables/artist_music_summary.csv"),
        normalized_videos_path=Path("music_analysis_tables/normalized_music_videos.csv"),
        video_type_path=Path("music_analysis_tables/video_type_analysis.csv"),
        top_artist_count=5,
    )

    assert "summary" in insights
    assert "artists" in insights
    assert "top_artists" in insights
    assert "video_types" in insights
    assert "data_quality" in insights

    assert len(insights["artists"]) == 6
    assert len(insights["top_artists"]) == 5
    assert len(insights["video_types"]) >= 1

    summary = insights["summary"]
    assert summary["total_views"] > 0
    assert summary["total_videos"] > 0
    assert summary["total_est_revenue_usd"] >= 0
    assert 0 <= summary["avg_engagement_rate"] <= 100

    artist_names = {artist["display_name"] for artist in insights["artists"]}
    assert "Corook" in artist_names

    for artist in insights["artists"]:
        assert artist["avg_views_per_day"] >= 0
        assert artist["avg_engagement_rate"] >= 0

    data_quality = insights["data_quality"]
    assert 0 <= data_quality["isrc_null_rate"] <= 1

    _assert_finite_numbers(insights)

    json.dumps(insights, allow_nan=False)

    output_path = tmp_path / "artist_insights.json"
    _write_json(insights, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["summary"]["total_views"] == summary["total_views"]
