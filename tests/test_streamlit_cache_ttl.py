import os

import pytest

from streamlit_app import _read_int_env


def test_read_int_env_default(monkeypatch):
    monkeypatch.delenv("TEST_INT", raising=False)
    assert _read_int_env("TEST_INT", 900) == 900


def test_read_int_env_parses_int(monkeypatch):
    monkeypatch.setenv("TEST_INT", "123")
    assert _read_int_env("TEST_INT", 0) == 123


def test_read_int_env_raises_on_non_int(monkeypatch):
    monkeypatch.setenv("TEST_INT", "not-a-number")
    with pytest.raises(RuntimeError, match="must be an integer"):
        _read_int_env("TEST_INT", 0)


def test_read_int_env_raises_on_negative(monkeypatch):
    monkeypatch.setenv("TEST_INT", "-5")
    with pytest.raises(RuntimeError, match="must be >= 0"):
        _read_int_env("TEST_INT", 0)
