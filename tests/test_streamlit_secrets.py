import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import streamlit as st

# Add project root to path so we can import streamlit_app
sys.path.append(str(Path(__file__).parent.parent))

from streamlit_app import _get_db_setting, _is_streamlit_cloud_runtime, _sync_db_settings_to_env, get_data_mode


def test_get_db_setting_handles_missing_secrets():
    """Verify _get_db_setting gracefully handles StreamlitSecretNotFoundError."""

    # Simulate StreamlitSecretNotFoundError when accessing st.secrets
    # We catch the specific error class from streamlit.errors if available,
    # or general Exception as the app does.

    # Note: trying to import the exact error might fail if streamlit runtime isn't fully active,
    # but the app catches Exception (BLE001), so we just need to ensure ANY error on getattr
    # is caught.

    # Mock os.environ to ensure we don't pick up real env vars
    with patch("streamlit.secrets", side_effect=Exception("Simulated Secret Not Found")), patch.dict(
        os.environ, {}, clear=True
    ):
        # Should return None, not raise
        assert _get_db_setting("DB_HOST") is None


def test_get_db_setting_reads_secrets_when_available():
    """Verify _get_db_setting reads from secrets if they exist."""

    mock_secrets = {"DB_HOST": "localhost"}

    # We need to mock the dictionary access.
    # st.secrets behaves like a dict.
    with patch("streamlit.secrets", mock_secrets):
        # The app uses `if name in st.secrets` then `st.secrets[name]`
        # For a dict, 'in' works naturally.
        assert _get_db_setting("DB_HOST") == "localhost"


def test_get_data_mode_defaults_to_demo_without_secrets():
    """Verify get_data_mode defaults to 'demo' when no secrets or env vars are present."""

    with patch("streamlit.secrets", side_effect=Exception("No secrets file")), patch.dict(
        os.environ, {}, clear=True
    ), patch("streamlit.session_state", {}):
        # Should default to demo, not crash
        assert get_data_mode() == "demo"


def test_get_data_mode_ignores_env_db_on_streamlit_cloud_by_default():
    """In Streamlit Cloud, env-only DB_* should not force Production mode."""

    env = {
        "DB_HOST": "localhost",
        "DB_PORT": "3306",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
    }

    with patch("streamlit.secrets", {}), patch("streamlit.session_state", {}), patch.dict(
        os.environ, env, clear=True
    ), patch("streamlit_app._is_streamlit_cloud_runtime", return_value=True):
        assert get_data_mode() == "demo"


def test_get_data_mode_uses_env_db_on_streamlit_cloud_when_explicitly_allowed():
    """Cloud env DB_* can be enabled explicitly with MUSICSCOPE_ALLOW_ENV_DB."""

    env = {
        "MUSICSCOPE_DATA_MODE": "production",
        "DB_HOST": "db.example.internal",
        "DB_PORT": "3306",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
        "MUSICSCOPE_ALLOW_ENV_DB": "1",
    }

    fake_conn = MagicMock()
    fake_engine = MagicMock()
    fake_engine.connect.return_value.__enter__.return_value = fake_conn

    with patch("streamlit.secrets", {}), patch("streamlit.session_state", {}), patch.dict(
        os.environ, env, clear=True
    ), patch("streamlit_app._is_streamlit_cloud_runtime", return_value=True), patch(
        "streamlit_app.get_engine", return_value=fake_engine
    ):
        assert get_data_mode() == "production"


def test_get_data_mode_cloud_defaults_demo_even_with_secrets_db_keys():
    """Cloud runtime must stay in demo unless production mode is explicitly requested."""

    mock_secrets = {
        "DB_HOST": "db.example.internal",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
    }
    with patch("streamlit.secrets", mock_secrets), patch("streamlit.session_state", {}), patch.dict(
        os.environ, {}, clear=True
    ), patch("streamlit_app._is_streamlit_cloud_runtime", return_value=True):
        assert get_data_mode() == "demo"


def test_get_data_mode_cloud_allows_production_when_explicitly_requested():
    """Cloud runtime can enter production when mode is explicitly requested."""

    mock_secrets = {
        "MUSICSCOPE_DATA_MODE": "production",
        "DB_HOST": "db.example.internal",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
    }
    fake_conn = MagicMock()
    fake_engine = MagicMock()
    fake_engine.connect.return_value.__enter__.return_value = fake_conn
    with patch("streamlit.secrets", mock_secrets), patch("streamlit.session_state", {}), patch.dict(
        os.environ, {}, clear=True
    ), patch("streamlit_app._is_streamlit_cloud_runtime", return_value=True), patch(
        "streamlit_app.get_engine", return_value=fake_engine
    ):
        assert get_data_mode() == "production"


def test_sync_db_settings_to_env_uses_streamlit_secrets():
    """Verify DB settings from st.secrets are exported for get_engine()."""

    mock_secrets = {
        "DB_HOST": "db.example.internal",
        "DB_PORT": "3306",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
    }

    with patch("streamlit.secrets", mock_secrets), patch("streamlit.session_state", {}), patch.dict(
        os.environ, {}, clear=True
    ):
        _sync_db_settings_to_env()
        assert os.environ["DB_HOST"] == "db.example.internal"
        assert os.environ["DB_PORT"] == "3306"
        assert os.environ["DB_USER"] == "etl_user"
        assert os.environ["DB_PASS"] == "secret"
        assert os.environ["DB_NAME"] == "yt_proj"


def test_sync_db_settings_to_env_respects_allow_env_false():
    """When allow_env=False, env-only DB settings should not be read."""

    with patch("streamlit.secrets", {}), patch("streamlit.session_state", {}), patch.dict(os.environ, {}, clear=True):
        _sync_db_settings_to_env(allow_env=False)
        for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME"):
            assert key not in os.environ


def test_is_streamlit_cloud_runtime_uses_cloud_env_flag(monkeypatch):
    monkeypatch.setenv("STREAMLIT_SERVER_RUNNING_IN_CLOUD", "true")
    with patch("streamlit_app.Path.cwd", return_value=Path("/tmp/local")):
        assert _is_streamlit_cloud_runtime()


def test_get_data_mode_syncs_secrets_before_connect():
    """Verify production mode syncs st.secrets values before opening engine."""

    mock_secrets = {
        "DB_HOST": "db.example.internal",
        "DB_USER": "etl_user",
        "DB_PASS": "secret",
        "DB_NAME": "yt_proj",
        "DB_PORT": "3306",
    }

    fake_conn = MagicMock()
    fake_engine = MagicMock()
    fake_engine.connect.return_value.__enter__.return_value = fake_conn

    with patch("streamlit.secrets", mock_secrets), patch("streamlit.session_state", {}), patch.dict(
        os.environ, {}, clear=True
    ), patch("streamlit_app.get_engine", return_value=fake_engine):
        assert get_data_mode() == "production"
        assert os.environ["DB_HOST"] == "db.example.internal"


def test_is_streamlit_cloud_runtime_uses_mount_src_fallback(monkeypatch):
    """Verify fallback to /mount/src/ cwd check when env var is missing."""
    # Ensure env var is NOT set
    monkeypatch.delenv("STREAMLIT_SERVER_RUNNING_IN_CLOUD", raising=False)
    monkeypatch.delenv("STREAMLIT_CLOUD", raising=False)

    with patch("streamlit_app.Path.cwd", return_value=Path("/mount/src/app")):
        assert _is_streamlit_cloud_runtime()


def test_is_streamlit_cloud_runtime_returns_false_locally(monkeypatch):
    """Verify returns False when neither env var nor cwd match."""
    monkeypatch.delenv("STREAMLIT_SERVER_RUNNING_IN_CLOUD", raising=False)
    monkeypatch.delenv("STREAMLIT_CLOUD", raising=False)

    with patch("streamlit_app.Path.cwd", return_value=Path("/Users/dev/app")):
        assert not _is_streamlit_cloud_runtime()
