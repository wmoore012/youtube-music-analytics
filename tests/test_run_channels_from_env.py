from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from tools.core import run_channels_from_env as module


def _summary() -> SimpleNamespace:
    return SimpleNamespace(
        channel_id="UC_TEST",
        uploads_playlist_id="PL_TEST",
        videos_seen=1,
        raw_upserts=1,
        metrics_upserts=1,
        errors=[],
    )


def _summary_without_updates() -> SimpleNamespace:
    return SimpleNamespace(
        channel_id="UC_TEST",
        uploads_playlist_id="PL_TEST",
        videos_seen=0,
        raw_upserts=0,
        metrics_upserts=0,
        errors=["quota exceeded"],
    )


def test_refresh_demo_snapshot_injects_project_root_into_pythonpath(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(command, capture_output, text, cwd, timeout, env):  # noqa: ANN001
        captured["command"] = command
        captured["cwd"] = cwd
        captured["timeout"] = timeout
        captured["env"] = env
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    monkeypatch.setenv("PYTHONPATH", "/tmp/existing_path")

    assert module._refresh_demo_snapshot(timeout_seconds=45) is True
    assert captured["command"] == [module.sys.executable, "scripts/refresh_demo_snapshot.py"]
    assert captured["cwd"] == str(module.PROJECT_ROOT)
    assert captured["timeout"] == 45

    pythonpath = str(captured["env"]["PYTHONPATH"])
    parts = pythonpath.split(os.pathsep)
    assert str(module.PROJECT_ROOT) in parts
    assert "/tmp/existing_path" in parts


def test_main_refreshes_demo_snapshot_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "collect_channel_urls_from_env",
        lambda: [("YT_TEST", "https://www.youtube.com/@example")],
    )
    monkeypatch.setattr(module, "run_channel_etl", lambda _url: _summary())
    calls: list[int] = []
    monkeypatch.setattr(module, "_refresh_demo_snapshot", lambda timeout_seconds: calls.append(timeout_seconds) or True)
    monkeypatch.delenv("ETL_SNAPSHOT_REFRESH_TIMEOUT_SECONDS", raising=False)

    exit_code = module.main(["--refresh-demo-snapshot"])

    assert exit_code == 0
    assert calls == [module.DEFAULT_SNAPSHOT_TIMEOUT_SECONDS]


def test_main_uses_env_flag_for_snapshot_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "collect_channel_urls_from_env",
        lambda: [("YT_TEST", "https://www.youtube.com/@example")],
    )
    monkeypatch.setattr(module, "run_channel_etl", lambda _url: _summary())
    monkeypatch.setenv("REFRESH_DEMO_SNAPSHOT_AFTER_INGEST", "1")
    monkeypatch.setenv("ETL_SNAPSHOT_REFRESH_TIMEOUT_SECONDS", "123")
    calls: list[int] = []
    monkeypatch.setattr(module, "_refresh_demo_snapshot", lambda timeout_seconds: calls.append(timeout_seconds) or True)

    exit_code = module.main([])

    assert exit_code == 0
    assert calls == [123]


def test_main_returns_failure_if_snapshot_refresh_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "collect_channel_urls_from_env",
        lambda: [("YT_TEST", "https://www.youtube.com/@example")],
    )
    monkeypatch.setattr(module, "run_channel_etl", lambda _url: _summary())
    monkeypatch.setattr(module, "_refresh_demo_snapshot", lambda _timeout_seconds: False)

    exit_code = module.main(["--refresh-demo-snapshot"])

    assert exit_code == 1


def test_main_allows_snapshot_refresh_failure_when_no_new_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "collect_channel_urls_from_env",
        lambda: [("YT_TEST", "https://www.youtube.com/@example")],
    )
    monkeypatch.setattr(module, "run_channel_etl", lambda _url: _summary_without_updates())
    monkeypatch.setattr(module, "_refresh_demo_snapshot", lambda _timeout_seconds: False)

    exit_code = module.main(["--refresh-demo-snapshot"])

    assert exit_code == 0


def test_main_skips_snapshot_refresh_when_channel_ingestion_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        module,
        "collect_channel_urls_from_env",
        lambda: [("YT_TEST", "https://www.youtube.com/@example")],
    )

    def fail_etl(_url: str) -> SimpleNamespace:
        raise RuntimeError("boom")

    monkeypatch.setattr(module, "run_channel_etl", fail_etl)
    called = {"refresh": False}
    monkeypatch.setattr(
        module,
        "_refresh_demo_snapshot",
        lambda _timeout_seconds: called.__setitem__("refresh", True),
    )

    exit_code = module.main(["--refresh-demo-snapshot"])

    assert exit_code == 1
    assert called["refresh"] is False
