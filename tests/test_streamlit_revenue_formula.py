import pandas as pd

from streamlit_app import build_revenue_formula_context


def test_revenue_formula_context_same_rpm_for_all_types() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "A",
                "total_views": 100_000,
                "total_est_revenue_usd": 250.0,
            }
        ]
    )
    videos = pd.DataFrame(
        [
            {"video_type": "Short", "view_count": 40_000, "est_revenue_usd": 100.0},
            {"video_type": "Official Music Video", "view_count": 60_000, "est_revenue_usd": 150.0},
        ]
    )

    ctx = build_revenue_formula_context(summary, videos)

    assert ctx["equation"] == "Estimated revenue (USD) = (Total views / 1,000) x RPM (USD per 1,000 views)"
    assert "Current selection: (100,000 / 1,000) x $2.50 = $250" in ctx["worked_example"]
    assert "one RPM for all content types" in ctx["type_note"]
    assert "Shorts vs Other Content" in ctx["type_note"]
    assert "directional ad-revenue estimate" in ctx["scope_note"]
    assert "rough RPM ranges around $1-$5" in ctx["public_range_note"]
    assert "assumptions and caveats" in ctx["tooltip_hint"]


def test_revenue_formula_context_detects_type_based_rpm() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "A",
                "total_views": 20_000,
                "total_est_revenue_usd": 50.0,
            }
        ]
    )
    videos = pd.DataFrame(
        [
            {"video_type": "Short", "view_count": 10_000, "est_revenue_usd": 10.0},
            {"video_type": "Official Music Video", "view_count": 10_000, "est_revenue_usd": 40.0},
        ]
    )

    ctx = build_revenue_formula_context(summary, videos)

    assert "different RPM values by content type" in ctx["type_note"]
    assert "Shorts: $1.00" in ctx["type_note"]
    assert "Official Music Video: $4.00" in ctx["type_note"]


def test_revenue_formula_context_handles_missing_video_rows() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "A",
                "total_views": 1_000,
                "total_est_revenue_usd": 2.5,
            }
        ]
    )

    ctx = build_revenue_formula_context(summary, pd.DataFrame())

    assert "no type-level revenue rows are available for the current filters" in ctx["type_note"]
