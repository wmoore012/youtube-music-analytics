import pytest
from unittest.mock import patch, MagicMock
import streamlit as st
import os
import sys
from pathlib import Path

# Add project root to path so we can import streamlit_app
sys.path.append(str(Path(__file__).parent.parent))

from streamlit_app import _get_db_setting, get_data_mode


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
