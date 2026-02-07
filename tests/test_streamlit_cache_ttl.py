import pytest
from streamlit_app import _read_float_env, _read_int_env


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


def test_read_int_env_empty_string_returns_default(monkeypatch):
    monkeypatch.setenv("TEST_INT", "")
    assert _read_int_env("TEST_INT", 7) == 7


def test_read_int_env_allows_zero(monkeypatch):
    monkeypatch.setenv("TEST_INT", "0")
    assert _read_int_env("TEST_INT", 10) == 0


def test_read_float_env_rejects_non_finite(monkeypatch):
    monkeypatch.setenv("TEST_FLOAT", "nan")
    with pytest.raises(RuntimeError, match="finite float"):
        _read_float_env("TEST_FLOAT", 1.0)
