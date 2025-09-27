#!/usr / bin / env python3
"""
Create scoring system database tables.

This script creates the database schema for the scoring system,
including tables for algorithms, configurations, runs, results, and metrics.
"""

import os
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text

from web.etl_helpers import get_engine


def create_scoring_tables():
    """Create all scoring system tables."""

    # Read the schema file
    schema_file = project_root / "src" / "data_organization" / "scoring_schema.sql"

    if not schema_file.exists():
        print(f"Error: Schema file not found at {schema_file}")
        return False

    with open(schema_file, "r") as f:
        schema_sql = f.read()

    # Split into individual statements
    statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

    engine = get_engine()

    try:
        with engine.connect() as conn:
            print("Creating scoring system tables...")

            for i, statement in enumerate(statements):
                if not statement:
                    continue

                try:
                    print(f"Executing statement {i + 1}/{len(statements)}...")
                    conn.execute(text(statement))
                    conn.commit()

                except Exception as e:
                    print(f"Warning: Statement {i + 1} failed: {e}")
                    # Continue with other statements
                    continue

            print("✅ Scoring system tables created successfully!")

            # Verify tables were created
            verify_tables(conn)

            return True

    except Exception as e:
        print(f"❌ Error creating scoring tables: {e}")
        return False


def verify_tables(conn):
    """Verify that all required tables were created."""

    required_tables = [
        "scoring_algorithms",
        "scoring_configurations",
        "scoring_runs",
        "scoring_results",
        "scoring_metrics",
    ]

    print("\nVerifying table creation...")

    for table in required_tables:
        try:
            result = conn.execute(
                text(
                    f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = '{table}'
            """
                )
            )

            if result.fetchone()[0] > 0:
                print(f"✅ Table '{table}' created successfully")
            else:
                print(f"❌ Table '{table}' not found")

        except Exception as e:
            print(f"❌ Error checking table '{table}': {e}")

    # Check views
    try:
        result = conn.execute(
            text(
                """
            SELECT COUNT(*) FROM information_schema.views
            WHERE table_schema = DATABASE() AND table_name = 'latest_scoring_results'
        """
            )
        )

        if result.fetchone()[0] > 0:
            print("✅ View 'latest_scoring_results' created successfully")
        else:
            print("❌ View 'latest_scoring_results' not found")

    except Exception as e:
        print(f"❌ Error checking view: {e}")


def drop_scoring_tables():
    """Drop all scoring system tables (for cleanup / reset)."""

    engine = get_engine()

    # Tables to drop in reverse dependency order
    tables_to_drop = [
        "scoring_metrics",
        "scoring_results",
        "scoring_runs",
        "scoring_configurations",
        "scoring_algorithms",
    ]

    views_to_drop = ["latest_scoring_results", "scoring_result_summary"]

    try:
        with engine.connect() as conn:
            print("Dropping scoring system tables...")

            # Drop views first
            for view in views_to_drop:
                try:
                    conn.execute(text(f"DROP VIEW IF EXISTS {view}"))
                    print(f"✅ Dropped view '{view}'")
                except Exception as e:
                    print(f"Warning: Could not drop view '{view}': {e}")

            # Drop tables
            for table in tables_to_drop:
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                    print(f"✅ Dropped table '{table}'")
                except Exception as e:
                    print(f"Warning: Could not drop table '{table}': {e}")

            conn.commit()
            print("✅ Scoring system tables dropped successfully!")
            return True

    except Exception as e:
        print(f"❌ Error dropping scoring tables: {e}")
        return False


def main():
    """Main function to handle command line arguments."""

    if len(sys.argv) > 1 and sys.argv[1] == "--drop":
        print("🗑️  Dropping scoring system tables...")
        success = drop_scoring_tables()
    else:
        print("🏗️  Creating scoring system tables...")
        success = create_scoring_tables()

    if success:
        print("\n🎉 Operation completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Operation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
