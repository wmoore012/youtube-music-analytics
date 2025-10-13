from pathlib import Path

import pytest

import web.dsp_cache as dsp_cache


@pytest.fixture(autouse=True)
def restore_env(monkeypatch):
    # Ensure environment is clean per test
    for k in ["CACHE_DIR", "ICATALOG_CACHE_DIR"]:
        monkeypatch.delenv(k, raising=False)
    yield


def test_legacy_only_emits_deprecation_warning(tmp_path, monkeypatch):
    monkeypatch.setenv("ICATALOG_CACHE_DIR", str(tmp_path))
    with pytest.warns(
        DeprecationWarning,
        match=r"^ICATALOG_CACHE_DIR is deprecated; set CACHE_DIR instead for future compatibility\.$",
    ):
        root = dsp_cache._cache_root()
        assert root == tmp_path


def test_both_set_prefers_modern_and_warns(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_DIR", str(tmp_path / "modern"))
    monkeypatch.setenv("ICATALOG_CACHE_DIR", str(tmp_path / "legacy"))
    with pytest.warns(
        DeprecationWarning,
        match=r"^ICATALOG_CACHE_DIR is deprecated and will be ignored because CACHE_DIR is set\.$",
    ):
        root = dsp_cache._cache_root()
        assert root == tmp_path / "modern"


def test_modern_only_no_warning(tmp_path, monkeypatch):
    monkeypatch.setenv("CACHE_DIR", str(tmp_path))
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        root = dsp_cache._cache_root()
        assert root == tmp_path
        assert not any(issubclass(x.category, DeprecationWarning) for x in w)


def test_default_path_when_no_env(monkeypatch):
    # neither variable set; default to repo_root / cache
    repo_root = Path(__file__).resolve().parents[1]
    expected = repo_root / "cache"
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        root = dsp_cache._cache_root()
        assert root == expected
        assert not any(issubclass(x.category, DeprecationWarning) for x in w)

