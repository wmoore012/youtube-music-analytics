from pathlib import Path

import nbformat
import pytest

from tools.archive.patch_notebooks import (
    patch_etl_notebook,
    patch_explore_notebook,
)


@pytest.fixture(autouse=True)
def restore_env(monkeypatch):
    for k in ["ENABLE_SENTIMENT", "ICATALOG_ENABLE_SENTIMENT"]:
        monkeypatch.delenv(k, raising=False)
    yield


@pytest.fixture()
def empty_notebook_file(tmp_path) -> Path:
    nb = nbformat.v4.new_notebook()
    p = tmp_path / "test.ipynb"
    nbformat.write(nb, str(p))
    return p


def test_legacy_only_warns_and_patches_etl(empty_notebook_file, monkeypatch):
    monkeypatch.setenv("ICATALOG_ENABLE_SENTIMENT", "1")
    with pytest.warns(
        DeprecationWarning,
        match=r"^ICATALOG_ENABLE_SENTIMENT is deprecated; set ENABLE_SENTIMENT instead\.$",
    ):
        changed = patch_etl_notebook(empty_notebook_file)
        assert changed is True


def test_both_set_warns_and_patches_explore(empty_notebook_file, monkeypatch):
    monkeypatch.setenv("ENABLE_SENTIMENT", "1")
    monkeypatch.setenv("ICATALOG_ENABLE_SENTIMENT", "1")
    with pytest.warns(
        DeprecationWarning,
        match=r"^ICATALOG_ENABLE_SENTIMENT is deprecated and will be ignored because ENABLE_SENTIMENT is set\.$",
    ):
        changed = patch_explore_notebook(empty_notebook_file)
        assert changed is True


def test_modern_only_no_warning_and_patches(empty_notebook_file, monkeypatch):
    monkeypatch.setenv("ENABLE_SENTIMENT", "1")
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        changed = patch_etl_notebook(empty_notebook_file)
        assert changed is True
        assert not any(issubclass(x.category, DeprecationWarning) for x in w)


def test_neither_set_no_warning_and_no_patch(empty_notebook_file):
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        changed = patch_explore_notebook(empty_notebook_file)
        assert changed is False
        assert not any(issubclass(x.category, DeprecationWarning) for x in w)

