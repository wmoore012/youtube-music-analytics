from __future__ import annotations

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
