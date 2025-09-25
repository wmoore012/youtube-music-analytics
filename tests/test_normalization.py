#!/usr/bin/env python3
"""
Unit tests for music video normalization (TDD for null reduction).
Uses an in-memory sqlite engine via SQLAlchemy for fast cycles.
"""
from __future__ import annotations

from datetime import date, datetime
import unittest

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from youtubeviz.normalization import build_normalized_rows, upsert_normalized


class TestNormalization(unittest.TestCase):
    def setUp(self) -> None:
        self.engine: Engine = create_engine("sqlite+pysqlite:///:memory:")
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE youtube_videos (
                        video_id TEXT PRIMARY KEY,
                        isrc TEXT,
                        title TEXT,
                        channel_title TEXT,
                        published_at DATETIME
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE youtube_metrics (
                        video_id TEXT NOT NULL,
                        view_count INTEGER,
                        like_count INTEGER,
                        comment_count INTEGER,
                        metrics_date DATE NOT NULL
                    );
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE music_videos_normalized (
                        video_id TEXT PRIMARY KEY,
                        artist_name TEXT NOT NULL,
                        title TEXT,
                        isrc TEXT,
                        published_at DATETIME,
                        total_views INTEGER,
                        total_likes INTEGER,
                        total_comments INTEGER,
                        est_revenue_usd REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
            )

        # Seed minimal data
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO youtube_videos (video_id, isrc, title, channel_title, published_at) VALUES (:v,:i,:t,:c,:p)"
                ),
                {
                    "v": "vid123",
                    "i": None,
                    "t": "Artist X - Song Y [Official Music Video]",
                    "c": "Artist X",
                    "p": datetime(2024, 5, 1, 12, 0, 0),
                },
            )
            # Two metrics rows for latest selection
            conn.execute(
                text(
                    "INSERT INTO youtube_metrics (video_id, view_count, like_count, comment_count, metrics_date) VALUES (:v,:vc,:lc,:cc,:d)"
                ),
                {"v": "vid123", "vc": 1000, "lc": 10, "cc": 1, "d": date(2024, 5, 1)},
            )
            conn.execute(
                text(
                    "INSERT INTO youtube_metrics (video_id, view_count, like_count, comment_count, metrics_date) VALUES (:v,:vc,:lc,:cc,:d)"
                ),
                {"v": "vid123", "vc": 1500, "lc": 12, "cc": 2, "d": date(2024, 5, 2)},
            )

    def test_build_and_upsert(self):
        rows = list(build_normalized_rows(self.engine))
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.video_id, "vid123")
        self.assertEqual(r.artist_name, "Artist X")
        self.assertEqual(r.total_views, 1500)
        self.assertAlmostEqual(r.est_revenue_usd, (1500 / 1000) * 2.5, places=2)

        count = upsert_normalized(self.engine, rows)
        self.assertEqual(count, 1)

        # Validate non-nulls in required columns
        df = pd.read_sql("SELECT * FROM music_videos_normalized", self.engine)
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertNotEqual(row["artist_name"], None)
        self.assertNotEqual(row["video_id"], None)
        self.assertNotEqual(row["total_views"], None)
        self.assertNotEqual(row["est_revenue_usd"], None)


if __name__ == "__main__":
    unittest.main(verbosity=2)
