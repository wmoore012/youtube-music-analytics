import pytest

from streamlit_app import read_float_env, read_int_env


def test_read_int_env_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_INT", raising=False)
    assert read_int_env("TEST_INT", 900) == 900


def test_read_int_env_parses_int(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INT", "123")
    assert read_int_env("TEST_INT", 0) == 123


def test_read_int_env_raises_on_non_int(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INT", "not-a-number")
    with pytest.raises(RuntimeError, match="must be an integer"):
        read_int_env("TEST_INT", 0)


def test_read_int_env_raises_on_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INT", "-5")
    with pytest.raises(RuntimeError, match="must be >= 0"):
        read_int_env("TEST_INT", 0)


def test_read_int_env_empty_string_returns_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INT", "")
    assert read_int_env("TEST_INT", 7) == 7


def test_read_int_env_allows_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_INT", "0")
    assert read_int_env("TEST_INT", 10) == 0


def test_read_float_env_rejects_non_finite(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_FLOAT", "nan")
    with pytest.raises(RuntimeError, match="finite float"):
        read_float_env("TEST_FLOAT", 1.0)
