"""Persistence layer for storing lineup runs and merge reports."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Mapping
from uuid import uuid4


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


@dataclass
class SlateRecord:
    slate_id: str
    created_at: datetime
    updated_at: datetime
    site: str
    sport: str
    name: str
    players_filename: str
    projections_filename: str
    players_csv: str
    projections_csv: str
    records: List[dict]
    report: dict
    players_mapping: dict
    projection_mapping: dict
    bias_factors: dict
    bias_summary: dict | None


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS slates (
                id TEXT PRIMARY KEY,
                site TEXT NOT NULL,
                sport TEXT NOT NULL,
                name TEXT,
                players_filename TEXT NOT NULL,
                projections_filename TEXT NOT NULL,
                players_csv TEXT NOT NULL,
                projections_csv TEXT NOT NULL,
                records_json TEXT NOT NULL,
                report_json TEXT NOT NULL,
                players_mapping_json TEXT NOT NULL,
                projection_mapping_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                bias_json TEXT,
                bias_summary_json TEXT
            )
            """
        )
        try:
            conn.execute("ALTER TABLE slates ADD COLUMN bias_json TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE slates ADD COLUMN bias_summary_json TEXT")
        except sqlite3.OperationalError:
            pass
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

    def save_slate(
        self,
        *,
        site: str,
        sport: str,
        name: str,
        players_filename: str,
        projections_filename: str,
        players_csv: str,
        projections_csv: str,
        records: Iterable[dict],
        report: dict,
        players_mapping: dict,
        projection_mapping: dict,
        slate_id: Optional[str] = None,
        bias_factors: Mapping[str, float] | None = None,
        bias_summary: dict | None = None,
    ) -> SlateRecord:
        slate_id = slate_id or uuid4().hex
        now = datetime.now(timezone.utc)
        bias_json = json.dumps(dict(bias_factors or {}))
        bias_summary_json = json.dumps(bias_summary or {})
        payload = (
            slate_id,
            site,
            sport,
            name,
            players_filename,
            projections_filename,
            players_csv,
            projections_csv,
            json.dumps(list(records)),
            json.dumps(report),
            json.dumps(players_mapping),
            json.dumps(projection_mapping),
            now.isoformat(),
            now.isoformat(),
            bias_json,
            bias_summary_json,
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO slates (
                    id, site, sport, name, players_filename, projections_filename,
                    players_csv, projections_csv, records_json, report_json,
                    players_mapping_json, projection_mapping_json, created_at, updated_at,
                    bias_json, bias_summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            conn.commit()
        slate = self.get_slate(slate_id)
        if slate is None:  # pragma: no cover
            raise KeyError(f"Slate {slate_id} not found after insert")
        return slate

    def update_slate(
        self,
        slate_id: str,
        *,
        name: str | None = None,
        players_filename: str | None = None,
        projections_filename: str | None = None,
        players_csv: str | None = None,
        projections_csv: str | None = None,
        records: Iterable[dict] | None = None,
        report: dict | None = None,
        players_mapping: dict | None = None,
        projection_mapping: dict | None = None,
        bias_factors: Mapping[str, float] | None = None,
        bias_summary: dict | None = None,
    ) -> SlateRecord:
        slate = self.get_slate(slate_id)
        if slate is None:
            raise KeyError(f"Slate {slate_id} not found")

        updated_name = name if name is not None else slate.name
        updated_players_filename = players_filename if players_filename is not None else slate.players_filename
        updated_projections_filename = (
            projections_filename if projections_filename is not None else slate.projections_filename
        )
        updated_players_csv = players_csv if players_csv is not None else slate.players_csv
        updated_projections_csv = projections_csv if projections_csv is not None else slate.projections_csv
        updated_records = list(records) if records is not None else slate.records
        updated_report = report if report is not None else slate.report
        updated_players_mapping = players_mapping if players_mapping is not None else slate.players_mapping
        updated_projection_mapping = projection_mapping if projection_mapping is not None else slate.projection_mapping
        updated_bias = dict(bias_factors) if bias_factors is not None else dict(slate.bias_factors)
        updated_bias_summary = bias_summary if bias_summary is not None else (slate.bias_summary or {})

        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE slates
                SET name = ?,
                    players_filename = ?,
                    projections_filename = ?,
                    players_csv = ?,
                    projections_csv = ?,
                    records_json = ?,
                    report_json = ?,
                    players_mapping_json = ?,
                    projection_mapping_json = ?,
                    bias_json = ?,
                    bias_summary_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    updated_name,
                    updated_players_filename,
                    updated_projections_filename,
                    updated_players_csv,
                    updated_projections_csv,
                    json.dumps(list(updated_records)),
                    json.dumps(updated_report),
                    json.dumps(updated_players_mapping),
                    json.dumps(updated_projection_mapping),
                    json.dumps(updated_bias),
                    json.dumps(updated_bias_summary),
                    now,
                    slate_id,
                ),
            )
            conn.commit()

        updated = self.get_slate(slate_id)
        if updated is None:  # pragma: no cover
            raise KeyError(f"Slate {slate_id} not found after update")
        return updated

    def get_slate(self, slate_id: Optional[str]) -> Optional[SlateRecord]:
        if not slate_id:
            return None
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM slates WHERE id = ?", (slate_id,)).fetchone()
            if row is None:
                return None
            return self._row_to_slate(row)

    def update_slate_bias(
        self,
        slate_id: str,
        *,
        bias_factors: Mapping[str, float] | None,
        bias_summary: dict | None,
    ) -> SlateRecord:
        return self.update_slate(
            slate_id,
            bias_factors=bias_factors or {},
            bias_summary=bias_summary or {},
        )

    def get_latest_slate(self, *, site: str | None = None, sport: str | None = None) -> Optional[SlateRecord]:
        query = "SELECT * FROM slates"
        conditions: list[str] = []
        params: list[str] = []
        if site:
            conditions.append("site = ?")
            params.append(site)
        if sport:
            conditions.append("sport = ?")
            params.append(sport)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY datetime(updated_at) DESC LIMIT 1"
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
            if row is None:
                return None
            return self._row_to_slate(row)

    def list_slates(
        self,
        *,
        site: str | None = None,
        sport: str | None = None,
        limit: int = 20,
    ) -> List[SlateRecord]:
        query = "SELECT * FROM slates"
        conditions: list[str] = []
        params: list[str | int] = []
        if site:
            conditions.append("site = ?")
            params.append(site)
        if sport:
            conditions.append("sport = ?")
            params.append(sport)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY datetime(updated_at) DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_slate(row) for row in rows]

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

    def _row_to_slate(self, row: sqlite3.Row) -> SlateRecord:
        return SlateRecord(
            slate_id=row["id"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            site=row["site"],
            sport=row["sport"],
            name=row["name"] or "",
            players_filename=row["players_filename"],
            projections_filename=row["projections_filename"],
            players_csv=row["players_csv"],
            projections_csv=row["projections_csv"],
            records=json.loads(row["records_json"]),
            report=json.loads(row["report_json"]),
            players_mapping=json.loads(row["players_mapping_json"]),
            projection_mapping=json.loads(row["projection_mapping_json"]),
            bias_factors=json.loads(row["bias_json"]) if row["bias_json"] else {},
            bias_summary=json.loads(row["bias_summary_json"]) if row["bias_summary_json"] else None,
        )
