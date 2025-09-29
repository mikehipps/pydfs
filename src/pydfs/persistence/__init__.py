"""Persistence layer for storing lineup runs and merge reports."""

from __future__ import annotations

import json
import sqlite3
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


class RunStore:
    """Simple SQLite-backed store for lineup runs."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
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

