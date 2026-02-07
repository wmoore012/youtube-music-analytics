from __future__ import annotations

from tools.core.run_focused_etl import determine_pipeline_status


def test_determine_pipeline_status_success() -> None:
    status, exit_code = determine_pipeline_status(
        bot_results={"status": "success"},
        sentiment_results={"status": "success"},
        quality_results={"quality_score": 95.0},
        notebook_results={"failed": []},
    )

    assert status == "SUCCESS"
    assert exit_code == 0


def test_determine_pipeline_status_notebook_failures_non_blocking_by_default(monkeypatch) -> None:
    monkeypatch.delenv("FOCUSED_ETL_NOTEBOOKS_REQUIRED", raising=False)
    status, exit_code = determine_pipeline_status(
        bot_results={"status": "success"},
        sentiment_results={"status": "success"},
        quality_results={"quality_score": 95.0},
        notebook_results={"failed": ["notebooks/MusicScope™_Professional_Dashboard.ipynb"]},
    )

    assert status == "SUCCESS_WITH_WARNINGS"
    assert exit_code == 0


def test_determine_pipeline_status_notebook_failures_blocking_when_flag_enabled(monkeypatch) -> None:
    monkeypatch.setenv("FOCUSED_ETL_NOTEBOOKS_REQUIRED", "true")
    status, exit_code = determine_pipeline_status(
        bot_results={"status": "success"},
        sentiment_results={"status": "success"},
        quality_results={"quality_score": 95.0},
        notebook_results={"failed": ["notebooks/MusicScope™_Professional_Dashboard.ipynb"]},
    )

    assert status == "COMPLETED_WITH_ISSUES"
    assert exit_code == 1


def test_determine_pipeline_status_core_failures_always_block(monkeypatch) -> None:
    monkeypatch.setenv("FOCUSED_ETL_NOTEBOOKS_REQUIRED", "false")
    status, exit_code = determine_pipeline_status(
        bot_results={"status": "success"},
        sentiment_results={"status": "failed"},
        quality_results={"quality_score": 99.0},
        notebook_results={"failed": []},
    )

    assert status == "COMPLETED_WITH_ISSUES"
    assert exit_code == 1
