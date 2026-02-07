from types import SimpleNamespace
from unittest.mock import mock_open

from tools.core.run_production_pipeline import EnterpriseETLPipeline


def test_run_stage_uses_configured_timeout(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={"stage_timeout_seconds": 123})
    captured: dict[str, int] = {}

    def fake_run(command, capture_output, text, timeout):  # noqa: ANN001
        captured["timeout"] = timeout
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("tools.core.run_production_pipeline.subprocess.run", fake_run)

    assert pipeline.run_stage("unit_stage", ["echo", "hi"], critical=True)
    assert captured["timeout"] == 123


def test_run_pipeline_skips_ingestion_if_already_run(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={})

    monkeypatch.setattr(pipeline, "should_run_etl", lambda: False)
    monkeypatch.setattr(pipeline, "run_data_cleanup", lambda: True)
    monkeypatch.setattr(
        pipeline,
        "run_stage",
        lambda stage_name, command, critical=False: True,
    )
    monkeypatch.setattr("tools.core.run_production_pipeline.json.dump", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.open", mock_open())

    result = pipeline.run_pipeline()

    assert result["status"] == "SUCCESS"
    assert result["stages"]["channel_ingestion"]["status"] == "SKIPPED"
    assert result["stages"]["main_etl"]["status"] == "SKIPPED"
