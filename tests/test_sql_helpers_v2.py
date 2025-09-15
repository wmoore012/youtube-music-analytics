# tests/icatalog_public/oss/test_sql_helpers_v2.py

from unittest.mock import MagicMock, call, patch  # Import call

import pandas as pd
import pytest
from sqlalchemy import MetaData, Table, text
from sqlalchemy.engine import Connection, Engine

# Import the new module
from src.icatalog_public.oss.sql_helpers_v2 import (
    ALL_TABLE_NAMES,
    get_connection,
    get_table,
    init_tables,
    read_sql_safe,
)


def test_read_sql_safe_sqlite_raw_connection():
    # Mock an SQLite engine
    mock_engine = MagicMock()
    mock_engine.dialect = MagicMock(name="dialect_mock")
    mock_engine.dialect.name = "sqlite"

    mock_raw_connection = MagicMock()
    mock_engine.raw_connection.return_value = mock_raw_connection

    # Mock pandas.read_sql_query
    with patch("pandas.read_sql_query") as mock_read_sql_query:
        mock_read_sql_query.return_value = pd.DataFrame({"col": [1, 2]})
        result = read_sql_safe("SELECT * FROM test", mock_engine)

        mock_engine.raw_connection.assert_called_once()
        mock_read_sql_query.assert_called_once_with("SELECT * FROM test", con=mock_raw_connection)
        assert not result.empty


def test_read_sql_safe_other_dialect_connection():
    # Mock a non-SQLite engine
    mock_engine = MagicMock()
    mock_engine.dialect = MagicMock(name="dialect_mock")
    mock_engine.dialect.name = "mysql"

    mock_connection = MagicMock(spec=Connection)
    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    # Mock pandas.read_sql_query
    with patch("pandas.read_sql_query") as mock_read_sql_query:
        mock_read_sql_query.return_value = pd.DataFrame({"col": [3, 4]})
        result = read_sql_safe("SELECT * FROM test", mock_engine)

        mock_engine.connect.assert_called_once()
        mock_read_sql_query.assert_called_once_with("SELECT * FROM test", con=mock_connection)
        assert not result.empty


def test_get_connection_public_schema():
    mock_engine = MagicMock(spec=Engine)
    mock_conn = MagicMock(spec=Connection)
    mock_engine.connect.return_value = mock_conn

    with patch("src.icatalog_public.oss.sql_helpers_v2.get_engine", return_value=mock_engine) as mock_get_engine:
        with get_connection("PUBLIC") as conn:
            assert conn is mock_conn
            conn.execute(text("SELECT 1"))

        mock_get_engine.assert_called_once_with(schema="PUBLIC")
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        # Corrected assertion for text() object
        assert str(mock_conn.execute.call_args[0][0]) == str(text("SELECT 1"))


def test_get_connection_default_schema():
    mock_engine = MagicMock(spec=Engine)
    mock_conn = MagicMock(spec=Connection)
    mock_engine.connect.return_value = mock_conn

    with patch("src.icatalog_public.oss.sql_helpers_v2.get_engine", return_value=mock_engine) as mock_get_engine:
        with get_connection() as conn:
            assert conn is mock_conn
            conn.execute(text("INSERT INTO test VALUES (1)"))

        mock_get_engine.assert_called_once_with()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        # Corrected assertion for text() object
        assert str(mock_conn.execute.call_args[0][0]) == str(text("INSERT INTO test VALUES (1)"))


# Tests for init_tables and get_table
@pytest.fixture(autouse=True)
def reset_table_globals():
    # Reset global state before each test to ensure isolation
    init_tables.__globals__["_TABLES_INITIALIZED"] = False
    init_tables.__globals__["_GLOBAL_META"] = None
    init_tables.__globals__["_TABLE_HANDLES"] = {}


def test_init_tables_reflects_correctly():
    mock_engine = MagicMock(spec=Engine)
    mock_metadata = MagicMock(spec=MetaData)
    mock_table_songs = MagicMock(spec=Table, name="songs")
    mock_table_artists = MagicMock(spec=Table, name="artists")

    # Mock the reflect method to populate tables
    mock_metadata.tables = {
        "songs": mock_table_songs,
        "artists": mock_table_artists,
    }

    with patch("src.icatalog_public.oss.sql_helpers_v2.MetaData", return_value=mock_metadata) as mock_meta_constructor:
        init_tables(mock_engine)

        mock_meta_constructor.assert_called_once()
        mock_metadata.reflect.assert_called_once_with(bind=mock_engine, only=ALL_TABLE_NAMES)
        assert init_tables.__globals__["_TABLES_INITIALIZED"] is True
        assert init_tables.__globals__["_TABLE_HANDLES"]["songs"] is mock_table_songs
        assert init_tables.__globals__["_TABLE_HANDLES"]["artists"] is mock_table_artists


def test_init_tables_is_idempotent():
    mock_engine = MagicMock(spec=Engine)
    mock_metadata = MagicMock(spec=MetaData)
    mock_metadata.tables = {"songs": MagicMock(spec=Table, name="songs")}

    with patch("src.icatalog_public.oss.sql_helpers_v2.MetaData", return_value=mock_metadata) as mock_meta_constructor:
        init_tables(mock_engine)
        init_tables(mock_engine)  # Call again

        mock_meta_constructor.assert_called_once()  # Should only be called once
        mock_metadata.reflect.assert_called_once()  # Should only be called once


def test_get_table_returns_correct_table():
    mock_engine = MagicMock(spec=Engine)
    mock_metadata = MagicMock(spec=MetaData)
    mock_table_songs = MagicMock(spec=Table, name="songs")
    mock_table_artists = MagicMock(spec=Table, name="artists")

    mock_metadata.tables = {
        "songs": mock_table_songs,
        "artists": mock_table_artists,
    }

    with patch("src.icatalog_public.oss.sql_helpers_v2.MetaData", return_value=mock_metadata):
        init_tables(mock_engine)  # Initialize tables first

        songs_table = get_table("songs")
        artists_table = get_table("artists")

        assert songs_table is mock_table_songs
        assert artists_table is mock_table_artists


def test_get_table_raises_error_if_not_initialized():
    # Ensure globals are reset
    init_tables.__globals__["_TABLES_INITIALIZED"] = False
    init_tables.__globals__["_GLOBAL_META"] = None
    init_tables.__globals__["_TABLE_HANDLES"] = {}

    with pytest.raises(
        RuntimeError, match="init_tables\(engine\) must be called once at program start"
    ):  # Use double backslashes for escaped parentheses
        get_table("songs")


def test_get_table_raises_key_error_for_unknown_table():
    mock_engine = MagicMock(spec=Engine)
    mock_metadata = MagicMock(spec=MetaData)
    mock_metadata.tables = {"songs": MagicMock(spec=Table, name="songs")}

    with patch("src.icatalog_public.oss.sql_helpers_v2.MetaData", return_value=mock_metadata):
        init_tables(mock_engine)  # Initialize with some tables

        with pytest.raises(
            KeyError,
            match="Unknown table 'non_existent_table'. Check ALL_TABLE_NAMES in sql_helpers_v2.py.",
        ):  # Use double backslashes for escaped single quotes
            get_table("non_existent_table")
