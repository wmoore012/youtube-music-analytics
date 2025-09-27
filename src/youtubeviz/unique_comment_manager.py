#!/usr / bin / env python3
"""
Unique Comment Manager

Ensures all comment sampling across the codebase uses UNIQUE comments.
Prevents data leakage between training, testing, classification, and benchmarking.
"""

from datetime import datetime
import hashlib
import re
import sqlite3
import sys
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, ".")
sys.path.insert(0, "src")

try:
    from web.etl_helpers import get_engine
except ImportError:

    def get_engine():
        from sqlalchemy import create_engine

        return create_engine("sqlite:///:memory:")


# Import ML data models for type safety
try:
    from youtubeviz.ml_data_models import (
        CommentMetadata,
        DataQualityReport,
        DataSplit,
        MLComment,
        MLDataset,
        MLExportFormat,
        MusicDomain,
        SentimentLabel,
    )
except ImportError:
    # Fallback for when models aren't available
    MLComment = None
    MLDataset = None


class UniqueCommentManager:
    """
    Manages unique comment allocation across all systems.

    Ensures no comment is used in multiple contexts (training, testing, classification, etc.)
    """

    def __init__(self, db_path: str = "unique_comment_tracking.db"):
        self.db_path = db_path
        self.init_tracking_db()

    def init_tracking_db(self):
        """Initialize the comment tracking database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comment_usage (
                comment_hash TEXT PRIMARY KEY,
                comment_text TEXT NOT NULL,
                usage_type TEXT NOT NULL,  -- 'classification', 'training', 'testing', 'benchmark', 'evaluation'
                system_name TEXT NOT NULL,  -- 'smart_classifier', 'model_benchmark', 'vader_evaluation', etc.
                allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                video_id TEXT,
                channel_title TEXT,
                like_count INTEGER,
                notes TEXT
            )
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_usage_type ON comment_usage(usage_type)
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_system_name ON comment_usage(system_name)
        """
        )

        conn.commit()
        conn.close()

    def _hash_comment(self, comment_text: str) -> str:
        """Create a consistent hash for a comment."""
        # Normalize text for consistent hashing
        normalized = comment_text.strip().lower()
        return hashlib.sha256(normalized.encode("utf - 8")).hexdigest()[:16]

    def is_comment_used(self, comment_text: str) -> bool:
        """Check if a comment has already been used."""
        comment_hash = self._hash_comment(comment_text)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM comment_usage WHERE comment_hash = ?", (comment_hash,))
        result = cursor.fetchone()

        conn.close()
        return result is not None

    def get_comment_usage(self, comment_text: str) -> Optional[Dict]:
        """Get usage information for a comment."""
        comment_hash = self._hash_comment(comment_text)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT usage_type, system_name, allocated_at, notes
            FROM comment_usage
            WHERE comment_hash = ?
        """,
            (comment_hash,),
        )

        result = cursor.fetchone()
        conn.close()

        if result:
            return {"usage_type": result[0], "system_name": result[1], "allocated_at": result[2], "notes": result[3]}
        return None

    def allocate_comment(
        self,
        comment_text: str,
        usage_type: str,
        system_name: str,
        video_id: str = None,
        channel_title: str = None,
        like_count: int = None,
        notes: str = "",
    ) -> bool:
        """
        Allocate a comment for a specific use.

        Returns True if successfully allocated, False if already used.
        """
        comment_hash = self._hash_comment(comment_text)

        # Check if already used
        if self.is_comment_used(comment_text):
            return False

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO comment_usage
            (comment_hash, comment_text, usage_type, system_name, video_id, channel_title, like_count, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (comment_hash, comment_text, usage_type, system_name, video_id, channel_title, like_count, notes),
        )

        conn.commit()
        conn.close()
        return True

    def get_unique_comments_for_system(
        self,
        system_name: str,
        usage_type: str,
        count: int,
        min_like_count: int = 1,
        max_length: int = 200,
        min_length: int = 10,
    ) -> List[Dict]:
        """
        Get unique comments for a specific system, ensuring no overlap with other systems.

        Returns list of comment dictionaries with metadata.
        """
        try:
            engine = get_engine()

            # Get all already used comment hashes
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT comment_hash FROM comment_usage")
            used_hashes = {row[0] for row in cursor.fetchall()}
            conn.close()

            # Fetch comments from database
            query = text(
                """
                SELECT c.comment_text, c.video_id, v.channel_title, c.like_count, c.published_at
                FROM youtube_comments c
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                    AND LENGTH(c.comment_text) >= :min_length
                    AND LENGTH(c.comment_text) <= :max_length
                    AND c.like_count >= :min_like_count
                ORDER BY c.like_count DESC, RAND()
                LIMIT :fetch_limit
            """
            )

            # Fetch more than needed to account for duplicates
            fetch_limit = min(count * 5, 50000)  # Increased multiplier for better success rate

            with engine.connect() as conn:
                result = conn.execute(
                    query,
                    {
                        "min_length": min_length,
                        "max_length": max_length,
                        "min_like_count": min_like_count,
                        "fetch_limit": fetch_limit,
                    },
                )
                raw_comments = result.fetchall()

            # Filter out already used comments
            unique_comments = []
            for row in raw_comments:
                comment_text_item = row[0]
                comment_hash = self._hash_comment(comment_text_item)

                if comment_hash not in used_hashes:
                    # Allocate this comment
                    if self.allocate_comment(
                        comment_text=comment_text_item,
                        usage_type=usage_type,
                        system_name=system_name,
                        video_id=row[1],
                        channel_title=row[2],
                        like_count=row[3],
                        notes=f"Auto - allocated for {system_name}",
                    ):
                        unique_comments.append(
                            {
                                "comment_text": comment_text_item,
                                "video_id": row[1],
                                "channel_title": row[2],
                                "like_count": row[3],
                                "published_at": row[4],
                            }
                        )
                        used_hashes.add(comment_hash)

                        if len(unique_comments) >= count:
                            break

            if len(unique_comments) > 0:
                print(f"✅ Allocated {len(unique_comments)} unique comments for {system_name} ({usage_type})")
            else:
                print(f"⚠️  No unique comments available for {system_name} ({usage_type}) - all may be allocated")
            return unique_comments

        except Exception as e:
            print(f"❌ Error fetching unique comments: {e}")
            return []

    def get_usage_stats(self) -> Dict:
        """Get statistics on comment usage across systems."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Total usage
        cursor.execute("SELECT COUNT(*) FROM comment_usage")
        total = cursor.fetchone()[0]

        # By usage type
        cursor.execute(
            """
            SELECT usage_type, COUNT(*)
            FROM comment_usage
            GROUP BY usage_type
        """
        )
        by_usage_type = dict(cursor.fetchall())

        # By system
        cursor.execute(
            """
            SELECT system_name, COUNT(*)
            FROM comment_usage
            GROUP BY system_name
        """
        )
        by_system = dict(cursor.fetchall())

        # Recent allocations
        cursor.execute(
            """
            SELECT system_name, usage_type, COUNT(*)
            FROM comment_usage
            WHERE allocated_at >= datetime('now', '-24 hours')
            GROUP BY system_name, usage_type
        """
        )
        recent = cursor.fetchall()

        conn.close()

        return {"total_allocated": total, "by_usage_type": by_usage_type, "by_system": by_system, "recent_24h": recent}

    def reset_system_allocation(self, system_name: str) -> int:
        """Reset all allocations for a specific system. Returns count of freed comments."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM comment_usage WHERE system_name = ?", (system_name,))
        count = cursor.fetchone()[0]

        cursor.execute("DELETE FROM comment_usage WHERE system_name = ?", (system_name,))

        conn.commit()
        conn.close()

        print(f"🔄 Freed {count} comments from {system_name}")
        return count

    def reset_all_allocations(self) -> int:
        """Reset ALL allocations. Use with caution!"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM comment_usage")
        count = cursor.fetchone()[0]

        cursor.execute("DELETE FROM comment_usage")

        conn.commit()
        conn.close()

        print(f"🔄 Freed ALL {count} comments from all systems")
        return count

    # ===== ML DATA EXPORT METHODS =====

    def get_ml_ready_comments(
        self, system_name: str, usage_type: str, count: int, music_domain_filter: bool = True, min_engagement: int = 1
    ) -> List[Dict]:
        """
        Get ML - ready comments with enhanced metadata and music domain filtering.

        Args:
            system_name: Name of the system requesting data
            usage_type: Type of usage (training, testing, evaluation)
            count: Number of comments to retrieve
            music_domain_filter: Whether to filter for music - related content
            min_engagement: Minimum like count threshold

        Returns:
            List of comment dictionaries with ML - ready metadata
        """
        try:
            engine = get_engine()

            # Get already used comment hashes
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT comment_hash FROM comment_usage")
            used_hashes = {row[0] for row in cursor.fetchall()}
            conn.close()

            # Build query with music domain filtering
            base_query = """
                SELECT
                    c.comment_text,
                    c.video_id,
                    v.channel_title,
                    c.like_count,
                    c.published_at,
                    COALESCE(c.reply_count, 0) as reply_count,
                    v.title as video_title,
                    v.view_count,
                    v.published_at as video_published_at
                FROM youtube_comments c
                JOIN youtube_videos v ON c.video_id = v.video_id
                WHERE c.comment_text IS NOT NULL
                    AND LENGTH(c.comment_text) >= 10
                    AND LENGTH(c.comment_text) <= 500
                    AND c.like_count >= :min_engagement
            """

            # Add music domain filtering if requested
            if music_domain_filter:
                base_query += """
                    AND (
                        v.channel_title REGEXP '(music|records|entertainment|official)'
                        OR v.title REGEXP '(official|music video|mv|audio|lyrics)'
                        OR c.comment_text REGEXP '(song|track|beat|album|artist|vocals|lyrics)'
                    )
                """

            base_query += " ORDER BY c.like_count DESC, RAND() LIMIT :fetch_limit"

            query = text(base_query)
            fetch_limit = min(count * 3, 10000)  # Fetch extra to account for duplicates

            with engine.connect() as conn:
                result = conn.execute(query, {"min_engagement": min_engagement, "fetch_limit": fetch_limit})
                raw_comments = result.fetchall()

            # Process and filter unique comments
            ml_ready_comments = []
            for row in raw_comments:
                comment_text_item = row[0]
                comment_hash = self._hash_comment(comment_text_item)

                if comment_hash not in used_hashes:
                    # Allocate comment
                    if self.allocate_comment(
                        comment_text=comment_text_item,
                        usage_type=usage_type,
                        system_name=system_name,
                        video_id=row[1],
                        channel_title=row[2],
                        like_count=row[3],
                        notes=f"ML export for {system_name}",
                    ):
                        # Create ML - ready comment data
                        ml_comment_data = {
                            "comment_text": comment_text_item,
                            "normalized_text": self._normalize_for_ml(comment_text_item),
                            "video_id": row[1],
                            "channel_title": row[2],
                            "like_count": row[3],
                            "published_at": row[4],
                            "reply_count": row[5] or 0,
                            "video_title": row[6],
                            "view_count": row[7] or 0,
                            "video_published_at": row[8],
                            # ML - specific metadata
                            "unique_hash": comment_hash,
                            "music_domain": self._classify_music_domain(comment_text_item, row[2], row[6]),
                            "contains_music_slang": self._contains_music_slang(comment_text_item),
                            "slang_terms": self._extract_slang_terms(comment_text_item),
                            "contains_emoji": self._contains_emoji(comment_text_item),
                            "emoji_count": self._count_emoji(comment_text_item),
                            "token_count": len(comment_text_item.split()),
                            "is_spam": self._is_likely_spam(comment_text_item),
                            "language_code": "en",  # Default to English for now
                        }

                        ml_ready_comments.append(ml_comment_data)
                        used_hashes.add(comment_hash)

                        if len(ml_ready_comments) >= count:
                            break

            print(f"✅ Retrieved {len(ml_ready_comments)} ML - ready comments for {system_name}")
            return ml_ready_comments

        except Exception as e:
            print(f"❌ Error fetching ML - ready comments: {e}")
            return []

    def export_ml_dataset(
        self,
        dataset_name: str,
        train_count: int = 1000,
        val_count: int = 200,
        test_count: int = 200,
        export_format: str = "jsonl",
    ) -> Optional[str]:
        """
        Export a complete ML dataset with train / validation / test splits.

        Args:
            dataset_name: Name for the dataset
            train_count: Number of training samples
            val_count: Number of validation samples
            test_count: Number of test samples
            export_format: Export format (jsonl, csv, parquet)

        Returns:
            Path to exported dataset file or None if failed
        """
        if not MLDataset:
            print("❌ ML data models not available. Install required dependencies.")
            return None

        try:
            # Create ML dataset
            dataset = MLDataset(
                name=dataset_name, description=f"ML dataset exported from unique comments", version="1.0"
            )

            # Get training data
            print(f"📊 Collecting {train_count} training samples...")
            train_data = self.get_ml_ready_comments(
                system_name=f"ml_dataset_{dataset_name}",
                usage_type="training",
                count=train_count,
                music_domain_filter=True,
            )

            # Get validation data
            print(f"📊 Collecting {val_count} validation samples...")
            val_data = self.get_ml_ready_comments(
                system_name=f"ml_dataset_{dataset_name}",
                usage_type="validation",
                count=val_count,
                music_domain_filter=True,
            )

            # Get test data
            print(f"📊 Collecting {test_count} test samples...")
            test_data = self.get_ml_ready_comments(
                system_name=f"ml_dataset_{dataset_name}",
                usage_type="testing",
                count=test_count,
                music_domain_filter=True,
            )

            # Convert to ML comments and add to dataset
            for data, split in [
                (train_data, DataSplit.TRAIN),
                (val_data, DataSplit.VALIDATION),
                (test_data, DataSplit.TEST),
            ]:
                for comment_data in data:
                    try:
                        metadata = CommentMetadata(
                            comment_id=comment_data["unique_hash"],
                            video_id=comment_data["video_id"],
                            channel_title=comment_data["channel_title"],
                            like_count=comment_data["like_count"],
                            published_at=comment_data.get("published_at"),
                            reply_count=comment_data["reply_count"],
                            music_domain=MusicDomain(comment_data["music_domain"]),
                            contains_music_slang=comment_data["contains_music_slang"],
                            slang_terms=comment_data["slang_terms"],
                            is_spam=comment_data["is_spam"],
                            language_code=comment_data["language_code"],
                        )

                        ml_comment = MLComment(
                            text=comment_data["comment_text"],
                            normalized_text=comment_data["normalized_text"],
                            token_count=comment_data["token_count"],
                            contains_emoji=comment_data["contains_emoji"],
                            emoji_count=comment_data["emoji_count"],
                            data_split=split,
                            unique_hash=comment_data["unique_hash"],
                            metadata=metadata,
                        )

                        dataset.add_comment(ml_comment)

                    except Exception as e:
                        print(f"⚠️  Skipping invalid comment: {e}")
                        continue

            # Export dataset
            timestamp = datetime.now().strftime("%Y % m%d_ % H%M % S")
            filename = f"{dataset_name}_ml_dataset_{timestamp}.{export_format}"

            if export_format == "jsonl":
                self._export_dataset_jsonl(dataset, filename)
            elif export_format == "csv":
                self._export_dataset_csv(dataset, filename)
            elif export_format == "parquet":
                self._export_dataset_parquet(dataset, filename)
            else:
                raise ValueError(f"Unsupported export format: {export_format}")

            print(f"✅ ML dataset exported: {filename}")
            print(
                f"📊 Dataset stats: {dataset.total_comments} total, "
                f"{dataset.train_count} train, {dataset.validation_count} val, {dataset.test_count} test"
            )

            return filename

        except Exception as e:
            print(f"❌ Error exporting ML dataset: {e}")
            return None

    def generate_data_quality_report(self, dataset_id: str, comments: List[Dict]) -> Optional[Dict]:
        """
        Generate a comprehensive data quality report for ML dataset.

        Args:
            dataset_id: Unique dataset identifier
            comments: List of comment dictionaries

        Returns:
            Data quality report dictionary or None if failed
        """
        if not DataQualityReport:
            print("❌ ML data models not available")
            return None

        try:
            total_samples = len(comments)
            valid_samples = 0
            empty_text_count = 0
            duplicate_count = 0
            text_lengths = []
            seen_hashes = set()

            # Analyze each comment
            for comment in comments:
                text_item = comment.get("comment_text", "").strip()

                if not text_item:
                    empty_text_count += 1
                    continue

                # Check for duplicates
                comment_hash = comment.get("unique_hash", "")
                if comment_hash in seen_hashes:
                    duplicate_count += 1
                else:
                    seen_hashes.add(comment_hash)
                    valid_samples += 1

                text_lengths.append(len(text))

            # Calculate quality metrics
            avg_text_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            label_imbalance_score = 0.5  # Placeholder - would need actual labels

            # Create quality report
            report = DataQualityReport(
                dataset_id=dataset_id,
                total_samples=total_samples,
                valid_samples=valid_samples,
                invalid_samples=total_samples - valid_samples,
                empty_text_count=empty_text_count,
                duplicate_count=duplicate_count,
                low_confidence_count=0,  # Placeholder
                label_imbalance_score=label_imbalance_score,
                avg_text_length=avg_text_length,
            )

            # Add recommendations
            if duplicate_count > 0:
                report.add_recommendation(f"Remove {duplicate_count} duplicate comments")

            if empty_text_count > 0:
                report.add_recommendation(f"Handle {empty_text_count} empty text samples")

            if avg_text_length < 10:
                report.add_recommendation("Consider filtering very short comments")

            if avg_text_length > 200:
                report.add_recommendation("Consider truncating very long comments")

            return report.dict()

        except Exception as e:
            print(f"❌ Error generating quality report: {e}")
            return None

    # ===== HELPER METHODS FOR ML PROCESSING =====

    def _normalize_for_ml(self, text: str) -> str:
        """Normalize text for ML processing while preserving music slang."""
        # Basic normalization
        normalized = text.strip()

        # Preserve music slang terms (don't lowercase these)
        _music_slang_terms = [
            "GOAT",
            "GOATED",
            "PERIODT",
            "SLAY",
            "QUEEN",
            "KING",
            "FIRE",
            "SLAPS",
            "BANGER",
            "HITS DIFFERENT",
        ]

        # Simple normalization - more sophisticated version would use proper NLP
        normalized = re.sub(r"\s+", " ", normalized)  # Collapse whitespace
        normalized = re.sub(
            r"[^\w\s\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]",
            " ",
            normalized,
        )

        return normalized.strip()

    def _classify_music_domain(self, comment_text: str, channel_title: str, video_title: str) -> str:
        """Classify the music domain of a comment."""
        text_lower = comment_text.lower()
        channel_lower = (channel_title or "").lower()
        video_lower = (video_title or "").lower()

        # Check for live performance indicators
        if any(
            term in text_lower or term in video_lower for term in ["live", "concert", "performance", "tour", "stage"]
        ):
            return MusicDomain.LIVE_PERFORMANCE.value

        # Check for music video indicators
        if any(term in video_lower for term in ["official", "music video", "mv", "video"]):
            return MusicDomain.MUSIC_VIDEO.value

        # Check for artist content
        if any(term in channel_lower for term in ["official", "records", "music", "entertainment"]):
            return MusicDomain.ARTIST_CONTENT.value

        # Check for music discussion
        if any(term in text_lower for term in ["album", "song", "track", "artist", "music", "sound"]):
            return MusicDomain.MUSIC_DISCUSSION.value

        return MusicDomain.GENERAL.value

    def _contains_music_slang(self, text: str) -> bool:
        """Check if text contains music slang terms."""
        text_lower = text.lower()
        music_slang = [
            "slaps",
            "banger",
            "fire",
            "goated",
            "hits different",
            "goes hard",
            "chef's kiss",
            "periodt",
            "no cap",
            "lowkey",
            "highkey",
            "mid",
            "trash",
            "cringe",
        ]
        return any(term in text_lower for term in music_slang)

    def _extract_slang_terms(self, text: str) -> List[str]:
        """Extract music slang terms from text."""
        text_lower = text.lower()
        music_slang = [
            "slaps",
            "banger",
            "fire",
            "goated",
            "hits different",
            "goes hard",
            "chef's kiss",
            "periodt",
            "no cap",
            "lowkey",
            "highkey",
            "mid",
            "trash",
            "cringe",
        ]
        return [term for term in music_slang if term in text_lower]

    def _contains_emoji(self, text: str) -> bool:
        """Check if text contains emoji characters."""
        emoji_pattern = re.compile(
            r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]+"
        )
        return bool(emoji_pattern.search(text))

    def _count_emoji(self, text: str) -> int:
        """Count emoji characters in text."""
        emoji_pattern = re.compile(
            r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]"
        )
        return len(emoji_pattern.findall(text))

    def _is_likely_spam(self, text: str) -> bool:
        """Simple spam detection based on patterns."""
        text_lower = text.lower()
        spam_indicators = ["subscribe", "like and subscribe", "check out my", "follow me", "click here", "link in bio"]
        return any(indicator in text_lower for indicator in spam_indicators)

    def _export_dataset_jsonl(self, dataset: "MLDataset", filename: str) -> None:
        """Export dataset to JSONL format."""
        import json

        with open(filename, "w", encoding="utf - 8") as f:
            for comment in dataset.comments:
                f.write(json.dumps(comment.dict(), ensure_ascii=False) + "\n")

    def _export_dataset_csv(self, dataset: "MLDataset", filename: str) -> None:
        """Export dataset to CSV format."""
        data = []
        for comment in dataset.comments:
            row = comment.to_training_dict()
            data.append(row)

        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)

    def _export_dataset_parquet(self, dataset: "MLDataset", filename: str) -> None:
        """Export dataset to Parquet format."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            data = []
            for comment in dataset.comments:
                row = comment.to_training_dict()
                data.append(row)

            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filename)

        except ImportError:
            print("⚠️  PyArrow not available, falling back to CSV")
            self._export_dataset_csv(dataset, filename.replace(".parquet", ".csv"))


# Global instance for easy access
comment_manager = UniqueCommentManager()


def get_unique_comments_for_classification(count: int) -> List[str]:
    """Get unique comments for manual classification."""
    comments_data = comment_manager.get_unique_comments_for_system(
        system_name="smart_classifier", usage_type="classification", count=count
    )
    return [c["comment_text"] for c in comments_data]


def get_unique_comments_for_training(count: int) -> List[str]:
    """Get unique comments for ML training."""
    comments_data = comment_manager.get_unique_comments_for_system(
        system_name="ml_training", usage_type="training", count=count
    )
    return [c["comment_text"] for c in comments_data]


def get_unique_comments_for_testing(count: int) -> List[str]:
    """Get unique comments for ML testing."""
    comments_data = comment_manager.get_unique_comments_for_system(
        system_name="ml_testing", usage_type="testing", count=count
    )
    return [c["comment_text"] for c in comments_data]


def get_unique_comments_for_benchmark(system_name: str, count: int) -> List[Dict]:
    """Get unique comments for benchmarking systems."""
    return comment_manager.get_unique_comments_for_system(system_name=system_name, usage_type="benchmark", count=count)


def get_unique_comments_for_evaluation(system_name: str, count: int) -> List[Dict]:
    """Get unique comments for evaluation."""
    return comment_manager.get_unique_comments_for_system(system_name=system_name, usage_type="evaluation", count=count)


if __name__ == "__main__":
    # Demo the unique comment manager
    print("🔍 UNIQUE COMMENT MANAGER DEMO")
    print("=" * 50)

    manager = UniqueCommentManager()

    # Show current stats
    stats = manager.get_usage_stats()
    print(f"📊 Current allocation stats:")
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   By usage type: {stats['by_usage_type']}")
    print(f"   By system: {stats['by_system']}")

    # Test allocation
    print(f"\n🧪 Testing unique allocation...")
    comments = get_unique_comments_for_classification(5)
    print(f"Got {len(comments)} unique comments for classification")

    # Show updated stats
    stats = manager.get_usage_stats()
    print(f"\n📊 Updated stats:")
    print(f"   Total allocated: {stats['total_allocated']}")
    print(f"   By system: {stats['by_system']}")
