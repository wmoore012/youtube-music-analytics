from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, cast
from urllib.parse import urlparse

import pymysql
import requests


@dataclass
class ETLSummary:
    channel_url: str
    channel_id: Optional[str]
    uploads_playlist_id: Optional[str]
    videos_seen: int
    raw_upserts: int
    metrics_upserts: int
    errors: List[str]


class YouTubeChannelETL:
    """YouTube channel ETL with batch raw insert and daily-max metrics upsert.

    Minimal deps (requests + pymysql), no googleapiclient required.
    """

    # Global logging noise toggle (set via env YT_ETL_LOG_LEVEL=DEBUG/INFO/WARN)
    LOG_LEVEL = os.getenv("YT_ETL_LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if not logger.handlers:
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(_h)

    def __init__(
        self,
        *,
        api_key: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_pass: str,
        db_name: str,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not api_key:
            raise ValueError("YOUTUBE_API_KEY is required")
        self.api_key = api_key
        self.s = session or requests.Session()
        self.db_args = dict(
            host=db_host or "127.0.0.1",
            port=int(db_port or 3306),
            user=db_user,
            password=db_pass,
            db=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        # Strict local DB only — no fallbacks
        self.logger.info(f"DB target host={self.db_args['host']} port={self.db_args['port']} db={self.db_args['db']}")

    # --------------------- YouTube REST helpers ---------------------
    def _api_get(self, path: str, params: Dict[str, str]) -> Dict[str, Any]:
        url = f"https://www.googleapis.com/youtube/v3/{path}"
        p = {"key": self.api_key, **params}
        self.logger.debug(f"GET {path} params={p}")
        r = self.s.get(url, params=p, timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _last_path_component(url: str) -> str:
        p = urlparse(url)
        return p.path.rstrip("/").split("/")[-1]

    def resolve_channel_id(self, channel_url: str) -> Optional[str]:
        """Return UC... id for /channel/UC..., /@handle, or /user/ style URLs."""
        path = urlparse(channel_url).path.rstrip("/")
        m = re.match(r"^/channel/(UC[0-9A-Za-z_-]{20,})$", path)
        if m:
            return m.group(1)

        # Search by handle or last path component
        q = self._last_path_component(channel_url)
        if not q:
            return None
        data = self._api_get(
            "search",
            {"q": q, "type": "channel", "part": "snippet", "maxResults": "5"},
        )
        items = data.get("items", [])
        return items[0]["snippet"].get("channelId") if items else None

    def get_uploads_playlist(self, channel_id: str) -> Optional[str]:
        data = self._api_get("channels", {"id": channel_id, "part": "contentDetails"})
        items = data.get("items", [])
        if not items:
            return None
        return items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

    def iter_playlist_items(self, playlist_id: str) -> Iterator[Dict[str, Any]]:
        params = {
            "playlistId": playlist_id,
            "part": "contentDetails,snippet",
            "maxResults": "50",
        }
        next_token = None
        while True:
            p = dict(params)
            if next_token:
                p["pageToken"] = next_token
            data = self._api_get("playlistItems", p)
            for it in data.get("items", []):
                yield it
            next_token = data.get("nextPageToken")
            if not next_token:
                break

    def get_videos_details(self, video_ids: List[str]) -> Dict[str, Any]:
        if not video_ids:
            return {"items": []}
        return self._api_get(
            "videos",
            {
                "id": ",".join(video_ids),
                "part": "snippet,contentDetails,statistics",
                "maxResults": "50",
            },
        )

    def get_playlist_details(self, playlist_id: str) -> Dict[str, Any]:
        return self._api_get(
            "playlists",
            {
                "id": playlist_id,
                "part": "snippet,contentDetails",
                "maxResults": "1",
            },
        )

    def iter_video_comments(self, video_id: str, max_comments: int = 0) -> Iterator[Dict[str, Any]]:
        """Yield top-level comments (commentThreads) for a video, newest first.

        max_comments: 0 to skip, otherwise limit number of comments.
        """
        if max_comments <= 0:
            return
        fetched = 0
        params = {
            "videoId": video_id,
            "part": "snippet",
            "order": "time",
            "maxResults": "100",
        }
        next_token = None
        while True:
            p = dict(params)
            if next_token:
                p["pageToken"] = next_token
            try:
                data = self._api_get("commentThreads", p)
            except Exception as e:  # noqa: BLE001
                self.logger.warning(f"comments_fetch_failed video={video_id}: {e}")
                return
            items = data.get("items", [])
            for it in items:
                yield it
                fetched += 1
                if 0 < max_comments <= fetched:
                    return
            next_token = data.get("nextPageToken")
            if not next_token:
                break

    # --------------------- DB helpers ---------------------
    def _connect(self) -> Any:
        self.logger.debug("Connecting to MySQL ...")
        host = cast(str, self.db_args["host"])
        port = cast(int, self.db_args["port"])
        user = cast(str, self.db_args["user"])
        password = cast(Optional[str], self.db_args["password"]) or ""
        dbname = cast(str, self.db_args["db"])
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=dbname,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=10,
            read_timeout=15,
            write_timeout=15,
        )

    @staticmethod
    def _coerce_counts(stats: Optional[Dict[str, Any]]) -> Tuple[int, int, int]:
        stats = stats or {}
        try:
            v = int(stats.get("viewCount") or 0)
        except Exception:
            v = 0
        try:
            l = int(stats.get("likeCount") or 0)
        except Exception:
            l = 0
        try:
            c = int(stats.get("commentCount") or 0)
        except Exception:
            c = 0
        return v, l, c

    def _batch_upsert_raw(self, conn: Any, rows: List[Tuple[str, Optional[str], str]]) -> int:
        if not rows:
            return 0
        sql = (
            "INSERT INTO youtube_videos_raw (video_id, playlist_id, raw_data, fetched_at, processed) "
            "VALUES (%s,%s,%s,NOW(),0) "
            "ON DUPLICATE KEY UPDATE raw_data=VALUES(raw_data), fetched_at=NOW(), processed=0"
        )
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        return len(rows)

    def _upsert_playlist_raw(self, conn: Any, playlist_id: str, raw_json: Dict[str, Any]) -> None:
        sql = (
            "INSERT INTO youtube_playlists_raw (playlist_id, raw_data, fetched_at, processed) "
            "VALUES (%s,%s,NOW(),0) "
            "ON DUPLICATE KEY UPDATE raw_data=VALUES(raw_data), fetched_at=NOW()"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (playlist_id, json.dumps(raw_json)))

    def _upsert_videos_summary(
        self,
        conn: Any,
        rows: List[
            Tuple[
                str,  # video_id
                Optional[str],  # isrc
                Optional[str],  # title
                Optional[str],  # channel_title
                Optional[str],  # published_at SQL str
                Optional[str],  # duration ISO8601
                Optional[int],  # view_count
                Optional[int],  # like_count
                Optional[int],  # comment_count
            ]
        ],
    ) -> int:
        """Upsert into youtube_videos.

        Each row: (video_id, isrc, title, channel_title, published_at_str, view_count, like_count, comment_count)
        published_at_str should be '%Y-%m-%d %H:%M:%S' or None.
        duration is stored from raw as provided (ISO8601); we use a separate parameter here for simplicity (computed inline below).
        """
        if not rows:
            return 0
        sql = (
            "INSERT INTO youtube_videos (video_id, isrc, title, channel_title, published_at, duration, view_count, like_count, comment_count, dsp_name, fetched_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'YouTube',NOW()) "
            "ON DUPLICATE KEY UPDATE "
            "title=VALUES(title), channel_title=VALUES(channel_title), published_at=VALUES(published_at), duration=VALUES(duration), "
            "view_count=VALUES(view_count), like_count=VALUES(like_count), comment_count=VALUES(comment_count), fetched_at=NOW()"
        )
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        return len(rows)

    def _insert_comments(
        self,
        conn: Any,
        rows: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], int, Optional[str]]],
    ) -> int:
        """Insert comments; ignore duplicates via unique(comment_id).

        Each row: (video_id, comment_id, comment_text, author_name, like_count, published_at_str)
        """
        if not rows:
            return 0
        sql = (
            "INSERT IGNORE INTO youtube_comments (video_id, comment_id, comment_text, author_name, like_count, published_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)"
        )
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        return cur.rowcount or 0

    def _upsert_daily_metrics(self, conn: Any, video_id: str, v: int, l: int, c: int) -> None:
        sql = (
            "INSERT INTO youtube_metrics (video_id, view_count, like_count, dislike_count, comment_count, "
            "subscriber_count, metrics_date, fetched_at) "
            "VALUES (%s,%s,%s,%s,%s,NULL,CURDATE(),NOW()) "
            "ON DUPLICATE KEY UPDATE "
            "view_count = IF(VALUES(view_count) > view_count, VALUES(view_count), view_count), "
            "like_count = IF(VALUES(like_count) > like_count, VALUES(like_count), like_count), "
            "comment_count = IF(VALUES(comment_count) > comment_count, VALUES(comment_count), comment_count), "
            "fetched_at = NOW()"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (video_id, v, l, 0, c))

    # --------------------- Run lock helpers ---------------------
    def _acquire_daily_lock(self, conn: Any, channel_id: str) -> bool:
        """Try to acquire a per-channel per-day run lock.

        Returns True if lock acquired, False if already exists for today.
        """
        sql = (
            "INSERT IGNORE INTO youtube_etl_runs (channel_id, run_date, started_at, status) "
            "VALUES (%s, CURDATE(), NOW(), 'started')"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (channel_id,))
            # rowcount == 1 means inserted (lock acquired); 0 means existing row (lock held)
            return cur.rowcount == 1

    def _finalize_run(self, conn: Any, channel_id: str, status: str) -> None:
        sql = "UPDATE youtube_etl_runs SET finished_at=NOW(), status=%s " "WHERE channel_id=%s AND run_date=CURDATE()"
        with conn.cursor() as cur:
            cur.execute(sql, (status, channel_id))

    # --------------------- ETL phases ---------------------
    def extract(self, channel_url: str, limit: Optional[int] = None) -> Tuple[str, str, Iterable[List[Dict[str, Any]]]]:
        """Extract: resolve channel + uploads and yield details in batches of <=50.

        Returns (channel_id, uploads_playlist_id, batches_of_video_details_items)
        """
        ch_id = self.resolve_channel_id(channel_url)
        if not ch_id:
            raise ValueError("channel_id_not_found")
        uploads = self.get_uploads_playlist(ch_id)
        if not uploads:
            raise ValueError("uploads_playlist_not_found")

        def _batches() -> Iterable[List[Dict[str, Any]]]:
            batch_ids: List[str] = []
            seen = 0
            for item in self.iter_playlist_items(uploads):
                vid = item.get("contentDetails", {}).get("videoId")
                if not vid:
                    continue
                batch_ids.append(vid)
                seen += 1
                if len(batch_ids) >= 50:
                    details = self.get_videos_details(batch_ids)
                    yield details.get("items", [])
                    batch_ids.clear()
                if limit and seen >= limit:
                    break
            if batch_ids:
                details = self.get_videos_details(batch_ids)
                yield details.get("items", [])

        return ch_id, uploads, _batches()

    def transform(self, items: List[Dict[str, Any]]) -> List[Tuple[str, int, int, int, str]]:
        """Transform: coerce stats and prepare rows for load.

        Returns list of tuples (video_id, view, like, comment, raw_json_str)
        """
        out: List[Tuple[str, int, int, int, str]] = []
        for v in items:
            vid = v.get("id")
            if not isinstance(vid, str) or not vid:
                continue
            stats = v.get("statistics")
            vv, ll, cc = self._coerce_counts(stats)
            out.append((vid, vv, ll, cc, json.dumps(v)))
        return out

    def load(self, conn: Any, uploads_pid: str, rows: List[Tuple[str, int, int, int, str]]) -> Tuple[int, int]:
        """Load: batch upsert raw and daily metrics.

        Returns (raw_upserts_count, metrics_upserts_count)
        """
        raw_rows: List[Tuple[str, Optional[str], str]] = [(vid, uploads_pid, raw) for (vid, _, _, _, raw) in rows]
        raw_count = self._batch_upsert_raw(conn, raw_rows)
        metrics_count = 0
        # Prepare videos summary rows
        summary_rows: List[
            Tuple[
                str,
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[int],
                Optional[int],
                Optional[int],
            ]
        ] = []
        fetch_comments = os.getenv("YT_FETCH_COMMENTS", "0").strip() in {"1", "true", "TRUE", "yes"}
        comments_limit = int(os.getenv("YT_COMMENTS_PER_VIDEO", "0") or 0)
        comments_to_insert: List[
            Tuple[Optional[str], Optional[str], Optional[str], Optional[str], int, Optional[str]]
        ] = []
        for vid, vv, ll, cc, _ in rows:
            self._upsert_daily_metrics(conn, vid, vv, ll, cc)
            metrics_count += 1
        # Build summary rows by parsing raw JSON
        for vid, vv, ll, cc, raw in rows:
            try:
                obj: Dict[str, Any] = json.loads(raw)
            except Exception:
                obj = {}
            snippet: Dict[str, Any] = cast(Dict[str, Any], obj.get("snippet", {}))
            content: Dict[str, Any] = cast(Dict[str, Any], obj.get("contentDetails", {}))
            title = cast(Optional[str], snippet.get("title"))
            channel_title = cast(Optional[str], snippet.get("channelTitle"))
            published_at = cast(Optional[str], snippet.get("publishedAt"))  # e.g., 2020-01-01T12:34:56Z
            published_at_sql: Optional[str] = None
            if isinstance(published_at, str):
                try:
                    dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
                    published_at_sql = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    published_at_sql = None
            # Optionally use duration if needed later
            _duration = cast(Optional[str], content.get("duration"))
            summary_rows.append((vid, None, title, channel_title, published_at_sql, vv, ll, cc))

            # Optionally fetch comments for each video
            if fetch_comments and comments_limit > 0:
                try:
                    for citem in self.iter_video_comments(vid, max_comments=comments_limit):
                        top = citem.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                        comment_id = citem.get("id")
                        comment_text = top.get("textOriginal")
                        author_name = top.get("authorDisplayName")
                        like_count = int(top.get("likeCount") or 0)
                        published_at_c = top.get("publishedAt")
                        published_at_sql_c: Optional[str] = None
                        if isinstance(published_at_c, str):
                            try:
                                dtc = datetime.strptime(published_at_c, "%Y-%m-%dT%H:%M:%SZ")
                                published_at_sql_c = dtc.strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                published_at_sql_c = None
                        comments_to_insert.append(
                            (vid, comment_id, comment_text, author_name, like_count, published_at_sql_c)
                        )
                except Exception as e:  # noqa: BLE001
                    self.logger.warning(f"comments_collect_failed video={vid}: {e}")

        # Upsert videos summary (youtube_videos)
        # We also need duration; compute from raw contentDetails above if present.
        # Modify summary rows to include duration by re-parsing raw to keep code simple.
        videos_rows_with_duration: List[
            Tuple[
                str,
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[int],
                Optional[int],
                Optional[int],
            ]
        ] = []
        for (vid, isrc, title, channel_title, published_at_sql, view_count, like_count, comment_count), (
            _,
            _,
            _,
            _,
            raw,
        ) in zip(summary_rows, rows):
            try:
                obj: Dict[str, Any] = json.loads(raw)
                duration = cast(Optional[str], obj.get("contentDetails", {}).get("duration"))
            except Exception:
                duration = None
            videos_rows_with_duration.append(
                (vid, isrc, title, channel_title, published_at_sql, duration, view_count, like_count, comment_count)
            )
        if videos_rows_with_duration:
            self._upsert_videos_summary(conn, videos_rows_with_duration)

        # Insert comments if any collected
        if comments_to_insert:
            self._insert_comments(conn, comments_to_insert)
        return raw_count, metrics_count

    # --------------------- Public API ---------------------
    def run_for_channel(self, channel_url: str, limit: Optional[int] = None) -> ETLSummary:
        errors: List[str] = []
        videos_seen = 0
        raw_total = 0
        metrics_total = 0
        try:
            ch_id, uploads, batches = self.extract(channel_url, limit)
        except Exception as e:
            return ETLSummary(channel_url, None, None, 0, 0, 0, [str(e)])

        conn = self._connect()
        try:
            # Acquire daily run lock per channel
            got_lock = self._acquire_daily_lock(conn, ch_id)
            conn.commit()
            if not got_lock:
                # Already ran today
                return ETLSummary(channel_url, ch_id, uploads, 0, 0, 0, ["already_ran_today"])
            # Upsert playlist raw details once at start
            try:
                pl = self.get_playlist_details(uploads)
                self._upsert_playlist_raw(conn, uploads, pl)
                conn.commit()
            except Exception as e:  # noqa: BLE001
                self.logger.warning(f"playlist_details_upsert_failed playlist={uploads}: {e}")
            for items in batches:
                videos_seen += len(items)
                rows = self.transform(items)
                raw_n, metrics_n = self.load(conn, uploads, rows)
                conn.commit()
                raw_total += raw_n
                metrics_total += metrics_n

            # Finalize success
            self._finalize_run(conn, ch_id, "success")
            conn.commit()
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            errors.append(str(e))
            try:
                self._finalize_run(conn, ch_id, "error")
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()

        return ETLSummary(
            channel_url=channel_url,
            channel_id=ch_id,
            uploads_playlist_id=uploads,
            videos_seen=videos_seen,
            raw_upserts=raw_total,
            metrics_upserts=metrics_total,
            errors=errors,
        )
