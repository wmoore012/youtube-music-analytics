import os
import pytest
from sqlalchemy import text
from web.etl_helpers import get_engine


def test_db_connection_credentials():
    """
    TDD: Verify that the application can connect to the database using the
    credentials from the environment, specifically testing password handling.
    """
    # 1. Inspect what python is checking (redacted for safety in logs)
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")

    print(f"\n[DEBUG] Attempting connection with: User={user}, Host={host}:{port}")
    # checking length to verify it's loaded
    print(f"[DEBUG] Password length: {len(password) if password else 0}")

    # 2. Try to get the engine and connect (ETL Helpers)
    print("\n[TEST 1] Testing web.etl_helpers.get_engine...")
    try:
        engine = get_engine(echo=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
            print("[SUCCESS] etl_helpers connection successful!")
    except Exception as e:
        print(f"[FAILURE] etl_helpers connection failed: {str(e)}")
        pytest.fail(f"etl_helpers connection failed: {str(e)}")

    # 3. Try to get the engine and connect (DB Guard)
    print("\n[TEST 2] Testing web.db_guard.get_engine...")
    try:
        from web.db_guard import get_engine as get_guard_engine

        engine_guard = get_guard_engine(echo=True)
        # db_guard executes a query on init, so just getting the engine might fail
        # but let's try a query too
        with engine_guard.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
            print("[SUCCESS] db_guard connection successful!")
    except Exception as e:
        print(f"[FAILURE] db_guard connection failed: {str(e)}")
        pytest.fail(f"db_guard connection failed: {str(e)}")


if __name__ == "__main__":
    test_db_connection_credentials()
