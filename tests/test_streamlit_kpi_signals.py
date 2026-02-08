import pandas as pd
import pytest

from streamlit_app import (
    build_artist_content_action_rows,
    build_delta_signal_rows,
    build_focus_artist_scorecard,
    build_focus_format_lift_table,
    build_focus_trend_frame,
    build_kpi_context,
    build_kpi_red_flags,
    build_release_strategy_board,
    compute_delta_display,
    compute_pct_delta,
    format_delta_value,
    prepare_recent_release_windows,
)


def test_compute_pct_delta_hides_invalid_or_tiny_change() -> None:
    assert compute_pct_delta(100, 0) is None
    assert compute_pct_delta(100.05, 100.0) is None
    assert compute_pct_delta(float("nan"), 100.0) is None
    assert compute_pct_delta(100.0, float("inf")) is None
    assert compute_pct_delta(110, 100) == pytest.approx(10.0)


def test_format_delta_value() -> None:
    assert format_delta_value(None) is None
    assert format_delta_value(12.34) == "+12.3%"
    assert format_delta_value(-3.21) == "-3.2%"


def test_compute_delta_display_marks_new_entry_when_baseline_near_zero() -> None:
    assert compute_delta_display(current=2.0, baseline=0.2, new_entry_floor=1.0) == "NEW ENTRY"
    assert compute_delta_display(current=0.8, baseline=0.2, new_entry_floor=1.0) == "+300.0%"
    assert compute_delta_display(current=2.0, baseline=1.0, new_entry_floor=1.0) == "+100.0%"


def test_build_delta_signal_rows_only_shows_visible_deltas() -> None:
    rows = build_delta_signal_rows(
        views_per_artist=110,
        roster_views_per_artist=100,
        videos_per_artist=10,
        roster_videos_per_artist=10,  # hidden
        likes_per_artist=50,
        roster_likes_per_artist=40,
        comments_per_artist=8,
        roster_comments_per_artist=8,  # hidden
        avg_engagement=5.5,
        roster_avg_engagement=5.0,
        revenue_per_artist=220.0,
        roster_revenue_per_artist=200.0,
    )

    assert not rows.empty
    assert "Total views" in rows["KPI"].tolist()
    assert "Total likes" in rows["KPI"].tolist()
    assert "Avg engagement rate" in rows["KPI"].tolist()
    assert "Est. revenue (USD)" in rows["KPI"].tolist()
    assert "Videos analyzed" not in rows["KPI"].tolist()
    assert "Total comments" not in rows["KPI"].tolist()
    assert rows["Arithmetic"].str.contains("x 100", regex=False).all()


def test_build_delta_signal_rows_marks_video_new_entry() -> None:
    rows = build_delta_signal_rows(
        views_per_artist=110.0,
        roster_views_per_artist=100.0,
        videos_per_artist=2.0,
        roster_videos_per_artist=0.2,
        likes_per_artist=50.0,
        roster_likes_per_artist=40.0,
        comments_per_artist=8.0,
        roster_comments_per_artist=7.0,
        avg_engagement=5.0,
        roster_avg_engagement=4.0,
        revenue_per_artist=220.0,
        roster_revenue_per_artist=200.0,
    )

    video_row = rows.loc[rows["KPI"] == "Videos analyzed"].iloc[0]
    assert video_row["Delta"] == "NEW ENTRY"
    assert "new entry floor" in video_row["Arithmetic"]


def test_build_artist_content_action_rows_returns_actionable_rows() -> None:
    df = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_type": "Short",
                "video_id": "a1",
                "view_count": 1000,
                "views_per_day": 200.0,
                "engagement_rate": 4.0,
            },
            {
                "artist_name": "Artist A",
                "video_type": "Official Music Video",
                "video_id": "a2",
                "view_count": 3000,
                "views_per_day": 120.0,
                "engagement_rate": 6.0,
            },
            {
                "artist_name": "Artist B",
                "video_type": "Official Music Video",
                "video_id": "b1",
                "view_count": 4000,
                "views_per_day": 80.0,
                "engagement_rate": 2.5,
            },
        ]
    )

    rows = build_artist_content_action_rows(df)
    assert len(rows) == 2
    assert set(rows.columns) == {"Artist", "Best Reach Format", "Best Engagement Format", "Action Plan"}
    assert rows["Action Plan"].str.len().min() > 20


def test_build_kpi_context_uses_window_scoped_video_rows() -> None:
    summary = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "total_views": 1000,
                "total_videos": 10,
                "total_likes": 100,
                "total_comments": 20,
                "total_est_revenue_usd": 5.0,
                "avg_engagement_rate": 5.0,
            },
            {
                "artist_name": "Artist B",
                "total_views": 900,
                "total_videos": 9,
                "total_likes": 90,
                "total_comments": 18,
                "total_est_revenue_usd": 4.5,
                "avg_engagement_rate": 4.0,
            },
        ]
    )

    selected_rows = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_id": "a1",
                "view_count": 120,
                "like_count": 12,
                "comment_count": 4,
                "est_revenue_usd": 0.6,
                "engagement_rate": 13.0,
            },
            {
                "artist_name": "Artist A",
                "video_id": "a2",
                "view_count": 80,
                "like_count": 8,
                "comment_count": 2,
                "est_revenue_usd": 0.4,
                "engagement_rate": 12.0,
            },
        ]
    )

    roster_rows = pd.concat(
        [
            selected_rows,
            pd.DataFrame(
                [
                    {
                        "artist_name": "Artist B",
                        "video_id": "b1",
                        "view_count": 200,
                        "like_count": 20,
                        "comment_count": 5,
                        "est_revenue_usd": 1.0,
                        "engagement_rate": 8.0,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    context = build_kpi_context(
        summary,
        artists=["Artist A"],
        videos=selected_rows,
        roster_videos=roster_rows,
    )

    assert context["total_views"] == 200
    assert context["total_videos"] == 2
    assert context["total_likes"] == 20
    assert context["total_comments"] == 6
    assert context["total_revenue"] == pytest.approx(1.0)
    assert context["avg_engagement"] == pytest.approx(12.5)
    assert context["selected_artist_count"] == 1
    assert context["roster_views_per_artist"] == pytest.approx(200.0)
    assert context["roster_videos_per_artist"] == pytest.approx(1.5)
    assert context["roster_likes_per_artist"] == pytest.approx(20.0)
    assert context["roster_comments_per_artist"] == pytest.approx(5.5)
    assert context["roster_revenue_per_artist"] == pytest.approx(1.0)
    assert context["roster_avg_engagement"] == pytest.approx(10.25)


def test_build_kpi_red_flags_for_zero_kpis_and_stale_data() -> None:
    flags = build_kpi_red_flags(
        total_videos=50,
        total_likes=0,
        total_comments=0,
        latest_metrics_date=pd.Timestamp("2025-06-27").date(),
        mode="demo",
        reference_date=pd.Timestamp("2026-02-07").date(),
    )

    assert any("Likes are zero" in flag for flag in flags)
    assert any("Comments are zero" in flag for flag in flags)
    assert any("Demo snapshot is" in flag for flag in flags)


def test_build_kpi_red_flags_uses_etl_heartbeat_for_production_freshness() -> None:
    flags = build_kpi_red_flags(
        total_videos=20,
        total_likes=1200,
        total_comments=250,
        latest_metrics_date=pd.Timestamp("2025-06-27").date(),
        latest_etl_run_date=pd.Timestamp("2026-02-07").date(),
        mode="production",
        reference_date=pd.Timestamp("2026-02-07").date(),
    )

    # ETL heartbeat is fresh, so old metrics_date alone should not trigger stale flag.
    assert flags == []


def test_build_kpi_red_flags_reports_stale_etl_heartbeat() -> None:
    flags = build_kpi_red_flags(
        total_videos=20,
        total_likes=1200,
        total_comments=250,
        latest_metrics_date=pd.Timestamp("2026-02-06").date(),
        latest_etl_run_date=pd.Timestamp("2025-12-01").date(),
        mode="production",
        reference_date=pd.Timestamp("2026-02-07").date(),
    )

    assert any("ETL heartbeat" in flag for flag in flags)


def test_build_kpi_red_flags_empty_when_metrics_are_healthy() -> None:
    flags = build_kpi_red_flags(
        total_videos=20,
        total_likes=1200,
        total_comments=250,
        latest_metrics_date=pd.Timestamp("2026-02-06").date(),
        mode="production",
        reference_date=pd.Timestamp("2026-02-07").date(),
    )
    assert flags == []


def test_prepare_recent_release_windows_limits_to_latest_n_per_artist() -> None:
    rows = []
    for idx in range(4):
        rows.append(
            {
                "artist_name": "Artist A",
                "video_id": f"official-{idx}",
                "title": f"Official {idx}",
                "video_type": "Official Music Video",
                "view_count": 1000 + idx,
                "views_per_day": 100.0 + idx,
                "engagement_rate": 2.0 + idx,
                "published_at": pd.Timestamp("2026-02-07") - pd.Timedelta(days=idx),
                "metrics_date": pd.Timestamp("2026-02-07"),
            }
        )
    for idx in range(3):
        rows.append(
            {
                "artist_name": "Artist A",
                "video_id": f"other-{idx}",
                "title": f"Other {idx}",
                "video_type": "Shorts",
                "view_count": 500 + idx,
                "views_per_day": 80.0 + idx,
                "engagement_rate": 3.0 + idx,
                "published_at": pd.Timestamp("2026-01-20") - pd.Timedelta(days=idx),
                "metrics_date": pd.Timestamp("2026-02-07"),
            }
        )
    df = pd.DataFrame(rows)

    official, other = prepare_recent_release_windows(df, per_artist_limit=2)

    assert official["video_id"].tolist() == ["official-0", "official-1"]
    assert other["video_id"].tolist() == ["other-0", "other-1"]


def test_build_release_strategy_board_computes_music_video_lift() -> None:
    df = pd.DataFrame(
        [
            {
                "artist_name": "Artist A",
                "video_id": "mv1",
                "title": "Official Music Video 1",
                "video_type": "Official Music Video",
                "view_count": 20_000,
                "views_per_day": 400.0,
                "engagement_rate": 4.0,
                "published_at": pd.Timestamp("2026-02-01"),
                "metrics_date": pd.Timestamp("2026-02-07"),
            },
            {
                "artist_name": "Artist A",
                "video_id": "mv2",
                "title": "Official Music Video 2",
                "video_type": "Official Music Video",
                "view_count": 22_000,
                "views_per_day": 420.0,
                "engagement_rate": 4.1,
                "published_at": pd.Timestamp("2026-01-28"),
                "metrics_date": pd.Timestamp("2026-02-07"),
            },
            {
                "artist_name": "Artist A",
                "video_id": "oa1",
                "title": "Official Audio 1",
                "video_type": "Official Audio",
                "view_count": 8_000,
                "views_per_day": 200.0,
                "engagement_rate": 2.0,
                "published_at": pd.Timestamp("2026-01-22"),
                "metrics_date": pd.Timestamp("2026-02-07"),
            },
            {
                "artist_name": "Artist A",
                "video_id": "short1",
                "title": "Short 1",
                "video_type": "Shorts",
                "view_count": 3_000,
                "views_per_day": 120.0,
                "engagement_rate": 5.5,
                "published_at": pd.Timestamp("2026-02-05"),
                "metrics_date": pd.Timestamp("2026-02-07"),
            },
            {
                "artist_name": "Artist A",
                "video_id": "short2",
                "title": "Short 2",
                "video_type": "Shorts",
                "view_count": 2_500,
                "views_per_day": 100.0,
                "engagement_rate": 5.0,
                "published_at": pd.Timestamp("2026-02-03"),
                "metrics_date": pd.Timestamp("2026-02-07"),
            },
        ]
    )

    board = build_release_strategy_board(df, per_artist_limit=10)
    row = board.iloc[0]

    # MV avg=(400+420)/2=410, non-MV official avg=200 => +105%
    assert row["MV vs other official lift (%)"] == pytest.approx(105.0)
    # Official avg=(400+420+200)/3=340, other avg=(120+100)/2=110 => +209.09%
    assert row["Official vs Other lift (%)"] == pytest.approx(209.0909, rel=1e-3)
    assert row["Official release count"] == 3
    assert row["Other content count"] == 2
    assert isinstance(row["Today action"], str)
    assert len(row["Today action"]) > 20


def test_build_focus_artist_scorecard_uses_peer_average_baseline() -> None:
    board = pd.DataFrame(
        [
            {
                "Artist": "Artist A",
                "Official avg views/day": 300.0,
                "Other avg views/day": 100.0,
                "Official vs Other lift (%)": 200.0,
                "MV vs other official lift (%)": 50.0,
                "Shorts share in other content (%)": 60.0,
                "Official cadence (days)": 14.0,
            },
            {
                "Artist": "Artist B",
                "Official avg views/day": 150.0,
                "Other avg views/day": 75.0,
                "Official vs Other lift (%)": 100.0,
                "MV vs other official lift (%)": 25.0,
                "Shorts share in other content (%)": 40.0,
                "Official cadence (days)": 21.0,
            },
            {
                "Artist": "Artist C",
                "Official avg views/day": 200.0,
                "Other avg views/day": 100.0,
                "Official vs Other lift (%)": 100.0,
                "MV vs other official lift (%)": 30.0,
                "Shorts share in other content (%)": 20.0,
                "Official cadence (days)": 28.0,
            },
        ]
    )

    scorecard = build_focus_artist_scorecard(board, "Artist A")
    row = scorecard.loc[scorecard["Metric"] == "Official avg views/day"].iloc[0]
    assert row["Focus value"] == pytest.approx(300.0)
    assert row["Benchmark avg"] == pytest.approx(175.0)
    assert row["Lift vs benchmark (%)"] == pytest.approx(((300.0 / 175.0) - 1.0) * 100.0)


def test_build_focus_trend_frame_returns_focus_and_benchmark_series() -> None:
    df = pd.DataFrame(
        [
            {"artist_name": "Artist A", "metrics_date": "2026-02-06", "views_per_day": 200.0},
            {"artist_name": "Artist A", "metrics_date": "2026-02-07", "views_per_day": 220.0},
            {"artist_name": "Artist B", "metrics_date": "2026-02-06", "views_per_day": 100.0},
            {"artist_name": "Artist B", "metrics_date": "2026-02-07", "views_per_day": 120.0},
            {"artist_name": "Artist C", "metrics_date": "2026-02-07", "views_per_day": 140.0},
        ]
    )

    trend = build_focus_trend_frame(df, "Artist A")
    assert set(trend["Series"].unique().tolist()) == {"Focus artist", "Benchmark average"}
    benchmark_day = trend.loc[
        (trend["Series"] == "Benchmark average") & (trend["metrics_date"] == pd.Timestamp("2026-02-07"))
    ].iloc[0]
    assert benchmark_day["views_per_day"] == pytest.approx(130.0)


def test_build_focus_format_lift_table_compares_focus_vs_benchmark() -> None:
    df = pd.DataFrame(
        [
            {"artist_name": "Artist A", "video_type": "Shorts", "video_id": "a1", "views_per_day": 300.0},
            {"artist_name": "Artist A", "video_type": "Official Music Video", "video_id": "a2", "views_per_day": 200.0},
            {"artist_name": "Artist B", "video_type": "Shorts", "video_id": "b1", "views_per_day": 150.0},
            {"artist_name": "Artist B", "video_type": "Official Music Video", "video_id": "b2", "views_per_day": 100.0},
        ]
    )

    table = build_focus_format_lift_table(df, "Artist A")
    shorts = table.loc[table["Format"] == "Shorts"].iloc[0]
    assert shorts["Focus avg views/day"] == pytest.approx(300.0)
    assert shorts["Benchmark avg views/day"] == pytest.approx(150.0)
    assert shorts["Lift vs benchmark (%)"] == pytest.approx(100.0)
