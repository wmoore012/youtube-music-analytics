import pandas as pd
import pytest

from portfolio.contracts import validate_dataframe
from portfolio.io import export_portfolio_run, load_insight_table, read_manifest


def _sample_tables():
    momentum = pd.DataFrame(
        {
            "video_id": ["v1"],
            "artist_name": ["Artist A"],
            "date": [pd.Timestamp("2025-01-01")],
            "momentum_score": [75.0],
            "state": ["breakout"],
            "warning_days_before": [2],
        }
    )

    sentiment = pd.DataFrame(
        {
            "video_id": ["v1"],
            "artist_name": ["Artist A"],
            "time_bucket": [pd.Timestamp("2025-01-01")],
            "avg_sentiment_score": [0.4],
            "net_sentiment_score": [20.0],
            "comment_volume": [42],
            "sentiment_band": ["positive"],
        }
    )

    performance = pd.DataFrame(
        {
            "video_id": ["v1"],
            "artist_name": ["Artist A"],
            "views": [1000],
            "engagement_score": [120],
            "engagement_rate": [12.0],
            "hidden_gem_flag": ["hidden_gem"],
            "quartile_bucket": ["q3"],
        }
    )

    highlights = pd.DataFrame(
        {
            "highlight_id": ["h1"],
            "category": ["momentum"],
            "title": ["Breakout confirmed"],
            "why_it_matters": ["Signals timing for campaign"],
            "supporting_table": ["momentum_insights"],
        }
    )

    return {
        "momentum_insights": momentum,
        "sentiment_insights": sentiment,
        "performance_insights": performance,
        "portfolio_highlights": highlights,
    }


def test_export_and_manifest_round_trip(tmp_path):
    cohort = "demo_cohort"
    run_id = "2025-09-26T14-30-00Z"
    dfs = _sample_tables()

    written = export_portfolio_run(cohort_slug=cohort, run_id=run_id, dfs=dfs, meta={"git_commit": "abc123"}, root=tmp_path)

    manifest = read_manifest(tmp_path, cohort, run_id)
    assert manifest["run_id"] == run_id
    assert manifest["cohort_slug"] == cohort
    assert manifest["git_commit"] == "abc123"
    names = {t["name"] for t in manifest["tables"]}
    assert names == set(dfs.keys())

    # Ensure tables are readable and validated after round-trip
    for name in dfs:
        loaded = load_insight_table(tmp_path, cohort, run_id, name)
        assert not loaded.empty
        assert set(dfs[name].columns).issubset(set(loaded.columns))


def test_booleans_not_allowed():
    bad_df = pd.DataFrame(
        {
            "video_id": ["v1"],
            "artist_name": ["Artist A"],
            "date": [pd.Timestamp("2025-01-01")],
            "momentum_score": [75.0],
            "state": ["breakout"],
            "warning_days_before": [2],
            "flag": [True],
        }
    )

    with pytest.raises(ValueError):
        validate_dataframe(bad_df, "momentum_insights")
