import os
from types import SimpleNamespace
from unittest.mock import mock_open

from tools.core import run_production_pipeline
from tools.core.run_production_pipeline import EnterpriseETLPipeline


def test_run_stage_uses_configured_timeout(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={"stage_timeout_seconds": 123})
    captured: dict[str, int] = {}

    def fake_run(command, capture_output, text, timeout, cwd, env):  # noqa: ANN001
        captured["timeout"] = timeout
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("tools.core.run_production_pipeline.subprocess.run", fake_run)

    assert pipeline.run_stage("unit_stage", ["echo", "hi"], critical=True)
    assert captured["timeout"] == 123


def test_run_stage_records_stdout_tail_when_stderr_empty(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={})

    def fake_run(command, capture_output, text, timeout, cwd, env):  # noqa: ANN001
        return SimpleNamespace(returncode=1, stdout="line1\nline2", stderr="")

    monkeypatch.setattr("tools.core.run_production_pipeline.subprocess.run", fake_run)

    success = pipeline.run_stage("failing_stage", ["python", "-c", "print('x')"], critical=True)

    assert success is False
    assert pipeline.results["status"] == "FAILED"
    assert "line1\nline2" in pipeline.results["errors"][-1]


def test_invalid_stage_timeout_env_falls_back_to_default(monkeypatch) -> None:
    monkeypatch.setenv("ETL_STAGE_TIMEOUT_SECONDS", "600s")
    pipeline = EnterpriseETLPipeline(config={"stage_timeout_seconds": 321})
    assert pipeline.stage_timeout_seconds == 321


def test_run_stage_injects_project_root_pythonpath(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={})
    captured: dict[str, object] = {}

    def fake_run(command, capture_output, text, timeout, cwd, env):  # noqa: ANN001
        captured["cwd"] = cwd
        captured["env"] = env
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("tools.core.run_production_pipeline.subprocess.run", fake_run)

    assert pipeline.run_stage("unit_stage", ["echo", "hi"], critical=True)

    assert captured["cwd"] == str(run_production_pipeline.project_root)
    pythonpath = str(captured["env"]["PYTHONPATH"])
    assert str(run_production_pipeline.project_root) in pythonpath.split(os.pathsep)


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


def test_run_pipeline_finalizes_results_on_critical_stage_failure(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={})
    log_calls: list[str] = []

    monkeypatch.setattr(pipeline, "should_run_etl", lambda: True)
    monkeypatch.setattr(pipeline, "run_data_cleanup", lambda: True)

    def fake_run_stage(stage_name, command, critical=False):  # noqa: ANN001
        if stage_name == "channel_ingestion":
            pipeline.results["status"] = "FAILED"
            pipeline.results["errors"].append("channel_ingestion failed")
            return False
        return True

    monkeypatch.setattr(pipeline, "run_stage", fake_run_stage)
    monkeypatch.setattr(pipeline, "log_etl_run", lambda status: log_calls.append(status))
    monkeypatch.setattr("tools.core.run_production_pipeline.json.dump", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.open", mock_open())

    result = pipeline.run_pipeline()

    assert result["status"] == "FAILED"
    assert "pipeline_end" in result
    assert log_calls == ["FAILED"]


def test_run_pipeline_channel_ingestion_enables_demo_snapshot_refresh(monkeypatch) -> None:
    pipeline = EnterpriseETLPipeline(config={})
    stage_commands: dict[str, list[str]] = {}

    monkeypatch.setattr(pipeline, "should_run_etl", lambda: True)
    monkeypatch.setattr(pipeline, "run_data_cleanup", lambda: True)

    def fake_run_stage(stage_name, command, critical=False):  # noqa: ANN001
        stage_commands[stage_name] = list(command)
        return True

    monkeypatch.setattr(pipeline, "run_stage", fake_run_stage)
    monkeypatch.setattr("tools.core.run_production_pipeline.json.dump", lambda *args, **kwargs: None)
    monkeypatch.setattr("builtins.open", mock_open())

    result = pipeline.run_pipeline()

    assert result["status"] == "SUCCESS"
    assert stage_commands["channel_ingestion"] == [
        "python",
        "tools/core/run_channels_from_env.py",
        "--refresh-demo-snapshot",
    ]
