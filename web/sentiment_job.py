from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


@dataclass
class SentimentStats:
    processed: int
    updated: int
    skipped: int


class YouTubeCommentSentimentJob:
    """Scores youtube_comments.sentiment_score using VADER and maintains summaries.

    - Uses env: DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME
    - Stores VADER compound in youtube_comments.sentiment_score (decimal(3,2))
    - Optionally refreshes youtube_sentiment_summary with per-video averages
    - Optionally snapshots per-video sentiment into youtube_sentiment (when ISRC is available)
    """

    def __init__(self) -> None:
        self.db_args = dict(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER") or "",
            password=os.getenv("DB_PASS") or "",
            db=os.getenv("DB_NAME") or "",
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=15,
            write_timeout=15,
        )
        self.analyzer = SentimentIntensityAnalyzer()

    def _connect(self) -> Any:
        return pymysql.connect(**self.db_args)

    @staticmethod
    def _to_decimal_2(v: float) -> float:
        # Clamp to [-1, 1] then 2 decimal places; MySQL DECIMAL(3,2) compatible
        v = max(-1.0, min(1.0, v))
        return float(f"{v:.2f}")

    def score_batch(self, limit: int = 500) -> SentimentStats:
        """Score up to `limit` comments where sentiment_score is NULL.

        Returns stats about rows processed/updated/skipped.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, comment_text
                    FROM youtube_comments
                    WHERE sentiment_score IS NULL
                      AND comment_text IS NOT NULL AND comment_text <> ''
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
                rows = cur.fetchall()

            if not rows:
                return SentimentStats(processed=0, updated=0, skipped=0)

            updates: List[Tuple[float, int]] = []
            skipped = 0
            for r in rows:
                cid = r["id"]
                text = (r.get("comment_text") or "").strip()
                if not text:
                    skipped += 1
                    continue
                scores = self.analyzer.polarity_scores(text)
                compound = self._to_decimal_2(scores.get("compound", 0.0))
                updates.append((compound, cid))

            updated = 0
            if updates:
                with conn.cursor() as cur:
                    cur.executemany(
                        "UPDATE youtube_comments SET sentiment_score=%s WHERE id=%s",
                        updates,
                    )
                    updated = cur.rowcount or 0
                conn.commit()

            return SentimentStats(processed=len(rows), updated=updated, skipped=skipped)
        finally:
            conn.close()

    def refresh_summary(self) -> int:
        """Upsert youtube_sentiment_summary from youtube_comments sentiments.

        Returns number of videos upserted.
        """
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT video_id, AVG(sentiment_score) AS avg_sentiment, COUNT(*) AS cnt
                    FROM youtube_comments
                    WHERE sentiment_score IS NOT NULL
                    GROUP BY video_id
                    """
                )
                rows = cur.fetchall()

            if not rows:
                return 0

            upserts: List[Tuple[str, float, int]] = []
            for r in rows:
                vid = r["video_id"]
                avg_sent = float(r["avg_sentiment"] or 0.0)
                cnt = int(r["cnt"] or 0)
                upserts.append((vid, avg_sent, cnt))

            sql = (
                "INSERT INTO youtube_sentiment_summary (video_id, avg_sentiment, comment_count, last_updated) "
                "VALUES (%s,%s,%s,NOW()) "
                "ON DUPLICATE KEY UPDATE avg_sentiment=VALUES(avg_sentiment), comment_count=VALUES(comment_count), last_updated=NOW()"
            )
            with conn.cursor() as cur:
                cur.executemany(sql, upserts)
            conn.commit()
            return len(upserts)
        finally:
            conn.close()

    def snapshot_daily_sentiment(self) -> int:
        """Insert a per-video daily snapshot into youtube_sentiment when ISRC exists.

        Uses the average of comment-level sentiment for each video_id.
        Returns number of rows inserted.
        """
        conn = self._connect()
        try:
            # Use date-truncated timestamp so we keep one snapshot per day per (isrc, video)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO youtube_sentiment (
                        isrc, video_id, fetch_datetime, sentiment_score, sentiment_magnitude,
                        compound_score, positive_score, neutral_score, negative_score
                    )
                    SELECT v.isrc,
                           c.video_id,
                           STR_TO_DATE(DATE(NOW()), '%Y-%m-%d') AS fetch_datetime,
                           AVG(c.sentiment_score) AS sentiment_score,
                           NULL AS sentiment_magnitude,
                           AVG(c.sentiment_score) AS compound_score,
                           NULL AS positive_score,
                           NULL AS neutral_score,
                           NULL AS negative_score
                    FROM youtube_comments c
                    JOIN youtube_videos v ON v.video_id = c.video_id
                    WHERE v.isrc IS NOT NULL AND c.sentiment_score IS NOT NULL
                    GROUP BY v.isrc, c.video_id
                    ON DUPLICATE KEY UPDATE
                        sentiment_score=VALUES(sentiment_score),
                        compound_score=VALUES(compound_score),
                        last_updated=NOW()
                    """
                )
                inserted = cur.rowcount or 0
            conn.commit()
            return inserted
        finally:
            conn.close()


def seed_version_types(values: Optional[List[str]] = None) -> int:
    """Idempotently insert version_types values.

    Returns number of rows inserted or updated (duplicates ignored).
    """
    defaults = [
        "Acoustic",
        "Chopped and Screwed",
        "Live",
        "Original",
        "Radio Edit",
        "Remix",
        "Visualizer",
    ]
    vals = values or defaults
    if not vals:
        return 0

    conn = pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER") or "",
        password=os.getenv("DB_PASS") or "",
        db=os.getenv("DB_NAME") or "",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    try:
        sql = "INSERT IGNORE INTO version_types (version_type) VALUES (%s)"
        rows = [(v,) for v in vals]
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            n = cur.rowcount or 0
        conn.commit()
        return n
    finally:
        conn.close()
