"""Persistence layer for storing lineup runs and merge reports."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass
class RunRecord:
    run_id: str
    created_at: datetime
    site: str
    sport: str
    request: dict
    report: dict
    lineups: List[dict]
    players_mapping: dict
    projection_mapping: dict


@dataclass
class RunJob:
    run_id: str
    state: str
    site: str
    sport: str
    created_at: datetime
    updated_at: datetime
    message: Optional[str]
    cancel_requested_at: Optional[datetime]
    completed_at: Optional[datetime]


class RunStore:
    """Simple SQLite-backed store for lineup runs."""

    def __init__(self, db_path: Path | str):
        self._use_uri = False
        env_db = os.getenv('PYDFS_DB_PATH')
        if env_db:
            if env_db.startswith('file:'):
                self.db_path = env_db
                self._use_uri = True
            else:
                self.db_path = Path(env_db)
        elif os.getenv('PYTEST_CURRENT_TEST'):
            test_dir = Path(tempfile.gettempdir()) / 'pydfs-test'
            test_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = test_dir / 'pydfs.sqlite'
            self._use_uri = False
        else:
            self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        if isinstance(self.db_path, Path):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                conn = sqlite3.connect(self.db_path)
            except sqlite3.OperationalError:
                fallback_dir = Path(tempfile.gettempdir()) / 'pydfs-runtime'
                fallback_dir.mkdir(parents=True, exist_ok=True)
                fallback = fallback_dir / 'pydfs.sqlite'
                conn = sqlite3.connect(fallback)
                self.db_path = fallback
                self._use_uri = False
                self._create_schema(conn)
        else:
            try:
                conn = sqlite3.connect(self.db_path, uri=self._use_uri)
            except sqlite3.OperationalError:
                fallback_dir = Path(tempfile.gettempdir()) / 'pydfs-runtime'
                fallback_dir.mkdir(parents=True, exist_ok=True)
                fallback = fallback_dir / 'pydfs.sqlite'
                conn = sqlite3.connect(fallback)
                self.db_path = fallback
                self._use_uri = False
                self._create_schema(conn)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            self._create_schema(conn)

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                site TEXT NOT NULL,
                sport TEXT NOT NULL,
                request_json TEXT NOT NULL,
                report_json TEXT NOT NULL,
                lineups_json TEXT NOT NULL,
                players_mapping_json TEXT NOT NULL,
                projection_mapping_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_jobs (
                id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                site TEXT NOT NULL,
                sport TEXT NOT NULL,
                message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                cancel_requested_at TEXT,
                completed_at TEXT
            )
            """
        )
        conn.commit()

    def save_run(
        self,
        *,
        run_id: str,
        site: str,
        sport: str,
        request: dict,
        report: dict,
        lineups: Iterable[dict],
        players_mapping: dict,
        projection_mapping: dict,
        created_at: Optional[datetime] = None,
    ) -> None:
        created_at = created_at or datetime.now(timezone.utc)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    id, created_at, site, sport, request_json, report_json,
                    lineups_json, players_mapping_json, projection_mapping_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    created_at.isoformat(),
                    site,
                    sport,
                    json.dumps(request),
                    json.dumps(report),
                    json.dumps(list(lineups)),
                    json.dumps(players_mapping),
                    json.dumps(projection_mapping),
                ),
            )
            conn.commit()
        try:
            self.create_job(
                run_id=run_id,
                site=site,
                sport=sport,
                state="completed",
            )
        except KeyError:
            # Job may have been deleted; ignore and continue.
            pass

    def get_run(self, run_id: str) -> Optional[RunRecord]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            return self._row_to_record(row)

    def list_runs(self, limit: int = 50) -> List[RunRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY datetime(created_at) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def create_job(
        self,
        *,
        run_id: str,
        site: str,
        sport: str,
        state: str = "running",
        message: Optional[str] = None,
    ) -> RunJob:
        mark_completed = state in {"completed", "failed", "canceled"}
        mark_cancel_requested = state == "cancel_requested"
        return self._upsert_job(
            run_id=run_id,
            site=site,
            sport=sport,
            state=state,
            message=message,
            set_completed=mark_completed,
            set_cancel_requested=mark_cancel_requested,
        )

    def update_job_state(
        self,
        run_id: str,
        *,
        state: str,
        message: Optional[str] = None,
    ) -> RunJob:
        mark_completed = state in {"completed", "failed", "canceled"}
        mark_cancel_requested = state == "cancel_requested"
        return self._upsert_job(
            run_id=run_id,
            state=state,
            message=message,
            set_completed=mark_completed,
            set_cancel_requested=mark_cancel_requested,
        )

    def mark_job_cancel_requested(
        self,
        run_id: str,
        *,
        message: Optional[str] = None,
    ) -> RunJob:
        return self.update_job_state(run_id, state="cancel_requested", message=message)

    def get_job(self, run_id: str) -> Optional[RunJob]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM run_jobs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            return self._job_row_to_record(row)

    def list_jobs(self, limit: int = 50) -> List[RunJob]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM run_jobs ORDER BY datetime(created_at) DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._job_row_to_record(row) for row in rows]

    def _row_to_record(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=row["id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            site=row["site"],
            sport=row["sport"],
            request=json.loads(row["request_json"]),
            report=json.loads(row["report_json"]),
            lineups=json.loads(row["lineups_json"]),
            players_mapping=json.loads(row["players_mapping_json"]),
            projection_mapping=json.loads(row["projection_mapping_json"]),
        )

    def _job_row_to_record(self, row: sqlite3.Row) -> RunJob:
        def _parse_ts(value: Optional[str]) -> Optional[datetime]:
            return datetime.fromisoformat(value) if value else None

        return RunJob(
            run_id=row["id"],
            state=row["state"],
            site=row["site"],
            sport=row["sport"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            message=row["message"],
            cancel_requested_at=_parse_ts(row["cancel_requested_at"]),
            completed_at=_parse_ts(row["completed_at"]),
        )

    def _upsert_job(
        self,
        *,
        run_id: str,
        state: str,
        site: Optional[str] = None,
        sport: Optional[str] = None,
        message: Optional[str] = None,
        set_cancel_requested: bool = False,
        set_completed: bool = False,
    ) -> RunJob:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM run_jobs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if existing is None:
                if site is None or sport is None:
                    raise KeyError(f"Job {run_id} not found")
                cancel_requested_at = now_iso if set_cancel_requested else None
                completed_at = now_iso if set_completed else None
                conn.execute(
                    """
                    INSERT INTO run_jobs (
                        id, state, site, sport, message, created_at,
                        updated_at, cancel_requested_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        state,
                        site,
                        sport,
                        message,
                        now_iso,
                        now_iso,
                        cancel_requested_at,
                        completed_at,
                    ),
                )
            else:
                if site is None:
                    site = existing["site"]
                if sport is None:
                    sport = existing["sport"]
                if message is None:
                    message = existing["message"]
                cancel_requested_at = existing["cancel_requested_at"]
                completed_at = existing["completed_at"]
                if set_cancel_requested:
                    cancel_requested_at = now_iso
                if set_completed:
                    completed_at = now_iso
                conn.execute(
                    """
                    UPDATE run_jobs
                    SET state = ?, site = ?, sport = ?, message = ?,
                        updated_at = ?, cancel_requested_at = ?, completed_at = ?
                    WHERE id = ?
                    """,
                    (
                        state,
                        site,
                        sport,
                        message,
                        now_iso,
                        cancel_requested_at,
                        completed_at,
                        run_id,
                    ),
                )
            conn.commit()
        job = self.get_job(run_id)
        if job is None:  # pragma: no cover - defensive, should not happen
            raise KeyError(f"Job {run_id} not found after upsert")
        return job
