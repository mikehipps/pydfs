"""Helpers to load projection CSVs and emit canonical records."""

from __future__ import annotations

import csv
from pathlib import Path
import re
from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

from typing import Mapping, Optional

from pydfs.config import get_rules
from pydfs.models import PlayerRecord


class ProjectionRow(BaseModel):
    raw_id: Optional[str] = None
    raw_name: str
    raw_team: str
    raw_position: Optional[str] = None
    raw_salary: str
    raw_projection: str
    ownership: float | None = Field(default=None, ge=0.0)

    @classmethod
    def from_mapping(cls, row: Mapping[str, str], mapping: Mapping[str, str]) -> "ProjectionRow":
        def extract(spec: Optional[str | Sequence[str]], *, default: Optional[str] = None) -> Optional[str]:
            if spec is None:
                return default
            if isinstance(spec, str):
                value = row.get(spec)
                return value.strip() if value is not None else default
            parts = [row.get(col, "").strip() for col in spec if row.get(col)]
            return " ".join(parts) if parts else default

        def parse_spec(key: str, default_key: Optional[str] = None) -> Optional[str | Sequence[str]]:
            spec = mapping.get(key)
            if spec is None and default_key:
                spec = default_key
            if spec is None:
                return None
            if isinstance(spec, str) and "|" in spec:
                return tuple(part.strip() for part in spec.split("|"))
            return spec

        ownership_spec = mapping.get("ownership")
        ownership_value = None
        if ownership_spec:
            raw = row.get(ownership_spec)
            try:
                ownership_value = float(raw) if raw not in (None, "") else None
            except (TypeError, ValueError):
                ownership_value = None

        data = {
            "raw_id": extract(parse_spec("player_id")),
            "raw_name": extract(parse_spec("name", "name"), default=""),
            "raw_team": extract(parse_spec("team", "team"), default=""),
            "raw_position": extract(parse_spec("position")),
            "raw_salary": extract(parse_spec("salary", "salary"), default="0"),
            "raw_projection": extract(parse_spec("projection", "projection"), default="0"),
            "ownership": ownership_value,
        }
        return cls(**data)


DEFAULT_PROJECTION_MAPPING = {
    "player_id": "player_id",
    "name": "name",
    "team": "team",
    "position": "position",
    "salary": "salary",
    "projection": "projection",
    "ownership": "ownership",
}

DEFAULT_PLAYERS_MAPPING = {
    "player_id": "Id",
    "name": "First Name|Last Name",
    "team": "Team",
    "position": "Position",
    "salary": "Salary",
    "projection": "FPPG",
}


def load_projection_csv(path: Path, *, mapping: Mapping[str, str] | None = None) -> List[ProjectionRow]:
    mapping = mapping or DEFAULT_PROJECTION_MAPPING
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [ProjectionRow.from_mapping(row, mapping) for row in reader]
    return rows


def _parse_salary(raw_salary: str) -> int:
    digits = re.sub(r"[^0-9]", "", raw_salary)
    if not digits:
        raise ValueError(f"salary '{raw_salary}' has no digits")
    return int(digits)


def _parse_projection(raw_projection: str) -> float:
    text = raw_projection.strip()
    if not text:
        return 0.0
    return float(text)


def _canonical_positions(site: str, sport: str, position: Optional[str]) -> List[str]:
    if not position:
        return []
    rules = get_rules(site, sport)
    tokens = [token.strip().upper() for token in position.split("/") if token.strip()]
    if not tokens:
        return []

    valid = set().union(*rules.slot_positions.values())
    normalized = []
    for token in tokens:
        normalized_token = token
        if token in {"DST", "D/ST", "DEF", "DEFENSE"}:
            normalized_token = "D"
        elif token == "OL":
            continue
        if normalized_token in valid:
            normalized.append(normalized_token)
    return normalized or tokens


def rows_to_records(
    rows: Sequence[ProjectionRow], *, site: str, sport: str
) -> List[PlayerRecord]:
    records: List[PlayerRecord] = []
    for row in rows:
        positions = _canonical_positions(site, sport, row.raw_position)
        metadata = {}
        if row.ownership is not None:
            metadata["projected_ownership"] = row.ownership
        records.append(
            PlayerRecord(
                player_id=row.raw_id or row.raw_name,
                name=row.raw_name,
                team=row.raw_team.upper(),
                positions=positions,
                salary=_parse_salary(row.raw_salary),
                projection=_parse_projection(row.raw_projection),
                metadata=metadata,
            )
        )
    return records


def load_records_from_csv(
    path: Path,
    *,
    site: str,
    sport: str,
    mapping: Mapping[str, str] | None = None,
) -> List[PlayerRecord]:
    return rows_to_records(load_projection_csv(path, mapping=mapping), site=site, sport=sport)


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _record_key(name: str, team: str) -> str:
    return f"{_normalize_name(name)}::{team.upper()}"


def merge_player_and_projection_files(
    *,
    players_path: Path,
    projections_path: Optional[Path],
    site: str,
    sport: str,
    players_mapping: Optional[Mapping[str, str]] = None,
    projection_mapping: Optional[Mapping[str, str]] = None,
) -> Tuple[List[PlayerRecord], MergeReport]:
    players_rows = load_projection_csv(players_path, mapping=players_mapping or DEFAULT_PLAYERS_MAPPING)
    base_records = {
        _record_key(r.raw_name, r.raw_team): rec
        for r, rec in zip(players_rows, rows_to_records(players_rows, site=site, sport=sport))
    }

    matched_keys: set[str] = set()
    unmatched_projection_rows: List[str] = []

    if projections_path is None:
        report = MergeReport(
            total_players=len(base_records),
            matched_players=0,
            players_missing_projection=[rec.name for rec in base_records.values()],
            unmatched_projection_rows=[],
        )
        return list(base_records.values()), report

    overlay_rows = load_projection_csv(projections_path, mapping=projection_mapping or DEFAULT_PROJECTION_MAPPING)
    for row in overlay_rows:
        key = _record_key(row.raw_name, row.raw_team)
        if key not in base_records:
            # create new record if enough info
            positions = _canonical_positions(site, sport, row.raw_position)
            metadata = {}
            if row.ownership is not None:
                metadata["projected_ownership"] = row.ownership
            try:
                base_records[key] = PlayerRecord(
                    player_id=row.raw_id or row.raw_name,
                    name=row.raw_name,
                    team=row.raw_team.upper(),
                    positions=positions,
                    salary=_parse_salary(row.raw_salary),
                    projection=_parse_projection(row.raw_projection),
                    metadata=metadata,
                )
            except ValueError:
                unmatched_projection_rows.append(row.raw_name)
                continue
            continue

        existing = base_records[key]
        matched_keys.add(key)
        metadata = dict(existing.metadata)
        if row.ownership is not None:
            metadata["projected_ownership"] = row.ownership

        update = {
            "projection": _parse_projection(row.raw_projection),
            "salary": _parse_salary(row.raw_salary),
            "metadata": metadata,
        }
        if row.raw_position:
            update["positions"] = _canonical_positions(site, sport, row.raw_position) or existing.positions
        if row.raw_id:
            update["player_id"] = row.raw_id

        base_records[key] = existing.model_copy(update=update)

    missing_projection = [
        base_records[key].name
        for key in base_records.keys()
        if key not in matched_keys
    ]

    report = MergeReport(
        total_players=len(base_records),
        matched_players=len(matched_keys),
        players_missing_projection=missing_projection,
        unmatched_projection_rows=unmatched_projection_rows,
    )
    return list(base_records.values()), report
@dataclass(frozen=True)
class MergeReport:
    total_players: int
    matched_players: int
    players_missing_projection: List[str]
    unmatched_projection_rows: List[str]
