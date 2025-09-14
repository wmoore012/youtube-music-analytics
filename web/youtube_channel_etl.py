from __future__ import annotations

import json
import logging
import os
import time
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
    """Minimal, MySQL-only YouTube ETL used by tests and notebooks.

    - No local fallbacks; DB creds must come from environment and be valid.
    - Uses PyMySQL DictCursor, explicit transactions.
    - YouTube API calls are simple wrappers (tests monkeypatch them).
    """

    def __init__(
        self,
        api_key: str,
        db_host: str,
        db_port: int,
        db_user: str,
        db_pass: str,
        db_name: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_key = api_key
        self.db_args: Dict[str, Any] = {
            "host": db_host,
            "port": int(db_port),
            "user": db_user,
            "password": db_pass,
            "db": db_name,
        }
        self.logger = logger or logging.getLogger(__name__)

    # --------------------- YouTube API helpers ---------------------
    def _api_get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        base = "https://www.googleapis.com/youtube/v3/"
        p = dict(params)
        p.setdefault("key", self.api_key)
        url = base + path
        backoff = 0.8
        for attempt in range(4):
            resp = requests.get(url, params=p, timeout=20)
            # Fail fast on daily quota exhaustion
            if resp.status_code == 403 and "quotaExceeded" in resp.text:
                raise RuntimeError("youtube_quota_exceeded")
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == 3:
                    resp.raise_for_status()
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return cast(Dict[str, Any], resp.json())
        # Unreachable
        raise RuntimeError("unreachable_api_path")

    @staticmethod
    def _extract_channel_id_from_url(url: str) -> Optional[str]:
        try:
            p = urlparse(url)
        except Exception:
            return None
        parts = [x for x in p.path.split("/") if x]
        if not parts:
            return None
        if parts[0].lower() == "channel" and len(parts) >= 2:
            return parts[1]
        return None

    def resolve_channel_id(self, channel_url: str) -> Optional[str]:
        # If URL already contains /channel/UCxxxx, use it; otherwise resolve handles via channels.list(forHandle)
        cid = self._extract_channel_id_from_url(channel_url)
        if cid:
            return cid
        # Try handle
        try:
            p = urlparse(channel_url)
            parts = [x for x in p.path.split("/") if x]
            handle: Optional[str] = None
            if parts and parts[0].startswith("@"):
                handle = parts[0]
            elif "@" in channel_url:
                handle = "@" + channel_url.split("@", 1)[1].split("/", 1)[0]
            if not handle:
                return None
        except Exception:
            return None

        try:
            data = self._api_get("channels", {"forHandle": handle, "part": "id"})
            items = data.get("items", [])
            if items:
                # API may return {"id": {"channelId": "UC..."}} or a flat id
                return items[0].get("id", {}).get("channelId") or items[0].get("id")
        except Exception:
            return None
        return None

    def get_uploads_playlist(self, channel_id: str) -> Optional[str]:
        data = self._api_get("channels", {"id": channel_id, "part": "contentDetails"})
        items = data.get("items", [])
        if not items:
            return None
        return items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

    def iter_playlist_items(self, playlist_id: str) -> Iterator[Dict[str, Any]]:
        params = {"playlistId": playlist_id, "part": "contentDetails,snippet", "maxResults": "50"}
        next_token: Optional[str] = None
        while True:
            p = dict(params)
            if next_token:
                p["pageToken"] = next_token
            data = self._api_get("playlistItems", p)
            for it in data.get("items", []):
                yield it
            next_token = cast(Optional[str], data.get("nextPageToken"))
            if not next_token:
                break

    def get_videos_details(self, video_ids: List[str]) -> Dict[str, Any]:
        if not video_ids:
            return {"items": []}
        return self._api_get(
            "videos",
            {"id": ",".join(video_ids), "part": "snippet,contentDetails,statistics"},
        )

    def get_playlist_details(self, playlist_id: str) -> Dict[str, Any]:
        return self._api_get("playlists", {"id": playlist_id, "part": "snippet,contentDetails", "maxResults": "1"})

    def iter_video_comments(self, video_id: str, max_comments: int = 0) -> Iterator[Dict[str, Any]]:
        if max_comments <= 0:
            return iter(())

        def _gen() -> Iterator[Dict[str, Any]]:
            fetched = 0
            params = {"videoId": video_id, "part": "snippet", "order": "time", "maxResults": "100"}
            next_token: Optional[str] = None
            while True:
                p = dict(params)
                if next_token:
                    p["pageToken"] = next_token
                try:
                    data = self._api_get("commentThreads", p)
                except Exception as e:  # noqa: BLE001
                    self.logger.warning(f"comments_fetch_failed video={video_id}: {e}")
                    return
                for it in data.get("items", []):
                    yield it
                    fetched += 1
                    if 0 < max_comments <= fetched:
                        return
                next_token_local = cast(Optional[str], data.get("nextPageToken"))
                next_token = next_token_local
                if not next_token:
                    break

        return _gen()

    # --------------------- DB helpers ---------------------
    def _connect(self) -> Any:
        self.logger.debug("mysql_connect start")
        return pymysql.connect(
            host=cast(str, self.db_args["host"]),
            port=cast(int, self.db_args["port"]),
            user=cast(str, self.db_args["user"]),
            password=cast(str, self.db_args["password"]),
            db=cast(str, self.db_args["db"]),
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
        ],
    ) -> int:
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
        if not rows:
            return 0
        sql = (
            "INSERT INTO youtube_comments (video_id, comment_id, comment_text, author_name, like_count, published_at) "
            "VALUES (%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE "
            "comment_text=VALUES(comment_text), author_name=VALUES(author_name), "
            "like_count=VALUES(like_count), published_at=VALUES(published_at)"
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
        # Ensure run-tracking table exists
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS youtube_etl_runs (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    channel_id VARCHAR(100) NOT NULL,
                    run_date DATE NOT NULL,
                    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at DATETIME NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'started',
                    UNIQUE KEY uniq_channel_day (channel_id, run_date)
                )
                """
            )
        # In development mode, clear any existing lock for today to allow repeatable runs
        if os.getenv("DEVELOPMENT_MODE", "").strip().lower() == "true":
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM youtube_etl_runs WHERE channel_id=%s AND run_date=CURDATE()",
                    (channel_id,),
                )
        sql = (
            "INSERT IGNORE INTO youtube_etl_runs (channel_id, run_date, started_at, status) "
            "VALUES (%s, CURDATE(), NOW(), 'started')"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (channel_id,))
            return cur.rowcount == 1

    def _finalize_run(self, conn: Any, channel_id: str, status: str) -> None:
        sql = "UPDATE youtube_etl_runs SET finished_at=NOW(), status=%s WHERE channel_id=%s AND run_date=CURDATE()"
        with conn.cursor() as cur:
            cur.execute(sql, (status, channel_id))

    # --------------------- ETL phases ---------------------
    def extract(self, channel_url: str, limit: Optional[int] = None) -> Tuple[str, str, Iterable[List[Dict[str, Any]]]]:
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

    def transform(self, items: List[Dict[str, Any]]) -> List[Tuple[str, int, int, int, str, Optional[str]]]:
        out: List[Tuple[str, int, int, int, str, Optional[str]]] = []
        for v in items:
            vid = v.get("id")
            if not isinstance(vid, str) or not vid:
                continue
            stats = cast(Optional[Dict[str, Any]], v.get("statistics"))
            vv, ll, cc = self._coerce_counts(stats)
            duration = cast(Optional[str], v.get("contentDetails", {}).get("duration"))
            out.append((vid, vv, ll, cc, json.dumps(v), duration))
        return out

    def load(
        self, conn: Any, uploads_pid: str, rows: List[Tuple[str, int, int, int, str, Optional[str]]]
    ) -> Tuple[int, int]:
        raw_rows: List[Tuple[str, Optional[str], str]] = [(vid, uploads_pid, raw) for (vid, *_rest, raw, _dur) in rows]
        raw_count = self._batch_upsert_raw(conn, raw_rows)
        metrics_count = 0

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

        # Upsert daily metrics and prepare summary rows
        for vid, vv, ll, cc, raw, duration in rows:
            self._upsert_daily_metrics(conn, vid, vv, ll, cc)
            metrics_count += 1
            try:
                obj: Dict[str, Any] = json.loads(raw)
            except Exception:
                obj = {}
            snippet = cast(Dict[str, Any], obj.get("snippet", {}))
            title = cast(Optional[str], snippet.get("title"))
            channel_title = cast(Optional[str], snippet.get("channelTitle"))
            published_at = cast(Optional[str], snippet.get("publishedAt"))
            published_at_sql: Optional[str] = None
            if isinstance(published_at, str):
                try:
                    dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
                    published_at_sql = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    published_at_sql = None
            summary_rows.append((vid, None, title, channel_title, published_at_sql, vv, ll, cc))

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

        # Upsert videos summary using duration provided by transform
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
        # Map video_id -> duration from rows (last value wins; duplicates unlikely in one batch)
        duration_map: Dict[str, Optional[str]] = {vid: duration for (vid, *_rest, _raw, duration) in rows}
        for vid, isrc, title, channel_title, published_at_sql, view_count, like_count, comment_count in summary_rows:
            duration_val = duration_map.get(vid)
            videos_rows_with_duration.append(
                (vid, isrc, title, channel_title, published_at_sql, duration_val, view_count, like_count, comment_count)
            )
        if videos_rows_with_duration:
            self._upsert_videos_summary(conn, videos_rows_with_duration)

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
        except Exception as e:  # noqa: BLE001
            return ETLSummary(channel_url, None, None, 0, 0, 0, [str(e)])

        conn = self._connect()
        try:
            got_lock = self._acquire_daily_lock(conn, ch_id)
            conn.commit()
            if not got_lock:
                return ETLSummary(channel_url, ch_id, uploads, 0, 0, 0, ["already_ran_today"])

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
