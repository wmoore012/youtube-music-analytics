#!/usr/bin/env python3
"""
Classify Real YouTube Comments

Fetches real comments from your database and helps you classify them
using the smart ML-powered classification assistant.
"""

import sys

sys.path.insert(0, "src")

import pandas as pd
from sqlalchemy import text

from src.youtubeviz.smart_comment_classifier import InteractiveClassifier, SmartCommentClassifier

try:
    from web.etl_helpers import get_engine
except ImportError:

    def get_engine():
        from sqlalchemy import create_engine

        return create_engine("sqlite:///:memory:")


class RealCommentClassifier(InteractiveClassifier):
    """Classification interface that uses real YouTube comments."""

    def __init__(self):
        super().__init__()
        self.engine = None

    def get_sample_comments(self, count: int) -> list:
        """Get UNIQUE real comments from the YouTube database."""
        try:
            from src.youtubeviz.unique_comment_manager import get_unique_comments_for_classification

            comments = get_unique_comments_for_classification(count)

            if not comments:
                print("❌ No unique unclassified comments available.")
                print("Either all comments are classified/allocated or database is empty.")
                return []

            print(f"✅ Allocated {len(comments)} UNIQUE real comments from database")
            return comments

        except Exception as e:
            print(f"❌ Could not fetch unique comments: {e}")
            print("Make sure your database is accessible and has youtube_comments table.")
            return []

    def setup_session(self):
        """Enhanced setup that shows database connection status."""
        print("🎯 REAL COMMENT CLASSIFICATION ASSISTANT")
        print("=" * 60)

        # Test database connection
        try:
            self.engine = get_engine()
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM youtube_comments"))
                total_comments = result.scalar()
                print(f"🗄️  Connected to database: {total_comments:,} total comments")

                # Check for already classified
                result = conn.execute(
                    text(
                        """
                    SELECT COUNT(*) FROM youtube_comments c
                    WHERE c.comment_text IN (
                        SELECT comment_text FROM comment_classifications
                    )
                """
                    )
                )
                classified_count = result.scalar()
                print(f"📊 Already classified: {classified_count:,} comments")
                print(f"🎯 Available to classify: {total_comments - classified_count:,} comments")

        except Exception as e:
            print(f"❌ Database connection issue: {e}")
            print("Cannot proceed without database connection.")

        return super().setup_session()


def main():
    """Main entry point for real comment classification."""
    print("🚀 YOUTUBE COMMENT CLASSIFICATION SYSTEM")
    print("=" * 60)
    print("This tool helps you classify real YouTube comments from your database.")
    print("The AI learns from your classifications and gets better over time!")
    print()

    try:
        classifier = RealCommentClassifier()
        classifier.run_session()
    except KeyboardInterrupt:
        print(f"\n👋 Classification session interrupted. Progress saved!")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
