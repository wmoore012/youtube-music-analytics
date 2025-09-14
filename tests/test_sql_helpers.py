from unittest.mock import MagicMock

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from src.icatalog_public.etl.sql_helpers import read_sql_safe


def test_read_sql_safe_sqlite_raw_connection():
    # Mock an SQLite engine
    mock_engine = MagicMock()
    mock_engine.dialect = MagicMock(name="dialect_mock")
    mock_engine.dialect.name = "sqlite"
    mock_raw_connection = MagicMock()
    mock_engine.raw_connection.return_value = mock_raw_connection

    # Mock pandas.read_sql_query to return a DataFrame
    pd.read_sql_query = MagicMock(return_value=pd.DataFrame({"col1": [1], "col2": ["a"]}))

    sql_query = "SELECT * FROM test_table"
    df = read_sql_safe(sql_query, mock_engine)

    # Assertions
    mock_engine.raw_connection.assert_called_once()
    pd.read_sql_query.assert_called_once_with(sql_query, con=mock_raw_connection)
    assert not df.empty


def test_read_sql_safe_other_dialect_connection():
    # Mock a non-SQLite engine
    mock_engine = MagicMock()
    mock_engine.dialect = MagicMock(name="dialect_mock")
    mock_engine.dialect.name = "mysql"
    mock_connection = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    # Mock pandas.read_sql_query to return a DataFrame
    pd.read_sql_query = MagicMock(return_value=pd.DataFrame({"col1": [1], "col2": ["a"]}))

    sql_query = "SELECT * FROM test_table"
    df = read_sql_safe(sql_query, mock_engine)

    # Assertions
    mock_engine.connect.assert_called_once()
    pd.read_sql_query.assert_called_once_with(sql_query, con=mock_connection)
    assert not df.empty
