"""Regression tests for duplicate function detection in enhanced CI."""

from __future__ import annotations

import textwrap

import pytest

from scripts.enhanced_ci import EnhancedCI


def _run_detection(monkeypatch: pytest.MonkeyPatch, tmp_path, contents: str):
    sample_file = tmp_path / "sample_module.py"
    sample_file.write_text(textwrap.dedent(contents))
    monkeypatch.chdir(tmp_path)
    ci = EnhancedCI()
    ci.errors.clear()
    ci.warnings.clear()
    found = ci._detect_duplicate_code()
    return found, ci


def test_duplicate_detection_flags_top_level(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Module-level duplicates should be reported as errors."""
    contents = """
    def helper():
        return 1

    def helper():
        return 2
    """
    found, ci = _run_detection(monkeypatch, tmp_path, contents)

    assert found is True
    assert any("Duplicate top-level function 'helper'" in msg for msg in ci.errors)


def test_duplicate_detection_ignores_class_methods(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Duplicate method names inside a class should be ignored."""
    contents = """
    class Example:
        def setUp(self):
            return True

        def setUp(self):
            return False
    """
    found, ci = _run_detection(monkeypatch, tmp_path, contents)

    assert found is False
    assert all("sample_module.py" not in msg for msg in ci.errors)
