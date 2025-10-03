"""Helpers to load projection CSVs and emit canonical records."""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

from pydfs.config import get_rules
from pydfs.models import PlayerRecord


logger = logging.getLogger(__name__)

NFL_TEAM_ALIAS_GROUPS: dict[str, list[str]] = {
    "ARI": ["ARI", "ARIZONA", "ARIZONA CARDINALS", "ARIZONA CARDS", "ARIZONA DST", "ARIZONA D/ST"],
    "ATL": ["ATL", "ATLANTA", "ATLANTA FALCONS", "ATLANTA DST", "ATLANTA D/ST"],
    "BAL": ["BAL", "BALTIMORE", "BALTIMORE RAVENS", "BALTIMORE DST", "BALTIMORE D/ST", "BALTIMORE RAVENS DST"],
    "BUF": ["BUF", "BUFFALO", "BUFFALO BILLS", "BUFFALO DST", "BUFFALO D/ST"],
    "CAR": ["CAR", "CAROLINA", "CAROLINA PANTHERS", "CAROLINA DST", "CAROLINA D/ST"],
    "CHI": ["CHI", "CHICAGO", "CHICAGO BEARS", "CHICAGO DST", "CHICAGO D/ST"],
    "CIN": ["CIN", "CINCINNATI", "CINCINNATI BENGALS", "CINCINNATI DST", "CINCINNATI D/ST", "BENGALS"],
    "CLE": ["CLE", "CLEVELAND", "CLEVELAND BROWNS", "CLEVELAND DST", "CLEVELAND D/ST", "BROWNS"],
    "DAL": ["DAL", "DALLAS", "DALLAS COWBOYS", "DALLAS DST", "DALLAS D/ST"],
    "DEN": ["DEN", "DENVER", "DENVER BRONCOS", "DENVER DST", "DENVER D/ST", "BRONCOS"],
    "DET": ["DET", "DETROIT", "DETROIT LIONS", "DETROIT DST", "DETROIT D/ST", "LIONS"],
    "GB": ["GB", "GNB", "GREEN BAY", "GREEN BAY PACKERS", "GREEN BAY DST", "PACKERS", "GREEN BAY D/ST"],
    "HOU": ["HOU", "HOUSTON", "HOUSTON TEXANS", "HOUSTON DST", "HOUSTON D/ST", "TEXANS"],
    "IND": ["IND", "INDIANAPOLIS", "INDIANAPOLIS COLTS", "INDIANAPOLIS DST", "INDIANAPOLIS D/ST", "COLTS"],
    "JAX": ["JAX", "JAC", "JACKSONVILLE", "JACKSONVILLE JAGUARS", "JACKSONVILLE DST", "JACKSONVILLE D/ST", "JAGUARS"],
    "KC": ["KC", "KAN", "KANSAS CITY", "KANSAS CITY CHIEFS", "KANSAS CITY DST", "KANSAS CITY D/ST", "CHIEFS"],
    "LAC": ["LAC", "LACH", "LOS ANGELES CHARGERS", "LA CHARGERS", "SAN DIEGO", "SAN DIEGO CHARGERS", "CHARGERS", "LOS ANGELES CHARGERS DST"],
    "LAR": ["LAR", "LA", "LOS ANGELES RAMS", "LA RAMS", "ST LOUIS", "ST LOUIS RAMS", "RAMS", "LOS ANGELES RAMS DST"],
    "LV": ["LV", "LVR", "LAS VEGAS", "LAS VEGAS RAIDERS", "OAKLAND", "OAKLAND RAIDERS", "RAIDERS", "LAS VEGAS DST"],
    "MIA": ["MIA", "MIAMI", "MIAMI DOLPHINS", "MIAMI DST", "MIAMI D/ST", "DOLPHINS"],
    "MIN": ["MIN", "MINNESOTA", "MINNESOTA VIKINGS", "MINNESOTA DST", "MINNESOTA D/ST", "VIKINGS"],
    "NE": ["NE", "NWE", "NEW ENGLAND", "NEW ENGLAND PATRIOTS", "NEW ENGLAND DST", "NEW ENGLAND D/ST", "PATRIOTS"],
    "NO": ["NO", "NOR", "NEW ORLEANS", "NEW ORLEANS SAINTS", "NEW ORLEANS DST", "NEW ORLEANS D/ST", "SAINTS"],
    "NYG": ["NYG", "NEW YORK", "NEW YORK GIANTS", "NY GIANTS", "GIANTS", "NEW YORK GIANTS DST"],
    "NYJ": ["NYJ", "NEW YORK JETS", "NY JETS", "JETS", "NEW YORK JETS DST"],
    "PHI": ["PHI", "PHILA", "PHILADELPHIA", "PHILADELPHIA EAGLES", "PHILADELPHIA DST", "PHILADELPHIA D/ST", "EAGLES"],
    "PIT": ["PIT", "PITTSBURGH", "PITTSBURGH STEELERS", "PITTSBURGH DST", "PITTSBURGH D/ST", "STEELERS"],
    "SEA": ["SEA", "SEATTLE", "SEATTLE SEAHAWKS", "SEATTLE DST", "SEATTLE D/ST", "SEAHAWKS"],
    "SF": ["SF", "SFO", "SAN FRANCISCO", "SAN FRANCISCO 49ERS", "SF 49ERS", "49ERS", "SAN FRANCISCO DST", "SAN FRANCISCO D/ST"],
    "TB": ["TB", "TAM", "TAMPA BAY", "TAMPA BAY BUCCANEERS", "TAMPA BAY DST", "TAMPA BAY D/ST", "BUCCANEERS", "BUCS"],
    "TEN": ["TEN", "TENNESSEE", "TENNESSEE TITANS", "TENNESSEE DST", "TENNESSEE D/ST", "TITANS"],
    "WAS": ["WAS", "WSH", "WASHINGTON", "WASHINGTON COMMANDERS", "WASHINGTON FOOTBALL TEAM", "WASHINGTON DST", "WASHINGTON D/ST", "COMMANDERS"],
}


def _team_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _build_alias_lookup() -> dict[str, dict[str, str]]:
    nfl_lookup: dict[str, str] = {}
    for abbr, variants in NFL_TEAM_ALIAS_GROUPS.items():
        for variant in variants:
            key = _team_token(variant)
            if key:
                nfl_lookup.setdefault(key, abbr)
    return {"NFL": nfl_lookup}


TEAM_ALIAS_LOOKUP = _build_alias_lookup()


class ProjectionRow(BaseModel):
    raw_id: Optional[str] = None
    raw_name: str
    raw_team: str
    raw_position: Optional[str] = None
    raw_salary: str
    raw_projection: str
    ownership: float | None = Field(default=None, ge=0.0)
    raw_injury_status: Optional[str] = None
    raw_probable_pitcher: Optional[str] = None

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
            "raw_injury_status": extract(parse_spec("injury_status")),
            "raw_probable_pitcher": extract(parse_spec("probable_pitcher")),
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
    "injury_status": "Injury Indicator",
    "probable_pitcher": "Probable Pitcher",
}


def _canonical_team(team: str, sport: str) -> str:
    token = _team_token(team)
    if not token:
        return team.upper()
    sport_key = sport.upper()
    lookup = TEAM_ALIAS_LOOKUP.get(sport_key, {})
    if token in lookup:
        return lookup[token]
    # Some data sources only provide the abbreviation already; ensure it stays uppercase.
    return team.upper()


def load_projection_csv(path: Path, *, mapping: Mapping[str, str] | None = None) -> List[ProjectionRow]:
    mapping = mapping or DEFAULT_PROJECTION_MAPPING
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [ProjectionRow.from_mapping(row, mapping) for row in reader]
    return rows


def _parse_salary(raw_salary: str, *, default: int | None = None) -> int:
    digits = re.sub(r"[^0-9]", "", raw_salary)
    if not digits:
        if default is not None:
            return default
        raise ValueError(f"salary '{raw_salary}' has no digits")
    return int(digits)


def _parse_projection(raw_projection: str) -> float:
    text = raw_projection.strip()
    if not text:
        return 0.0
    try:
        value = float(text)
    except ValueError:
        raise ValueError(f"projection '{raw_projection}' is not numeric") from None
    return max(0.0, value)


def _parse_flag(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    text = value.strip().lower()
    if not text:
        return None
    if text in {"1", "true", "t", "yes", "y", "probable", "p"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


_SINGLE_GAME_ROLE_TOKENS = {"MVP", "STAR", "PRO", "CAPTAIN"}


def infer_site_variant(site: str, sport: str, rows: Sequence["ProjectionRow"]) -> tuple[str, str]:
    """Infer adjusted site/sport keys based on roster hints in the player rows."""

    site_key = site.upper()
    sport_key = sport.upper()
    if site_key != "FD":
        return site_key, sport_key

    role_tokens: set[str] = set()
    for row in rows:
        raw = (row.raw_position or "").strip()
        if not raw:
            continue
        parts = re.split(r"[/,\\s]+", raw)
        role_tokens.update(part.upper() for part in parts if part)

    if role_tokens & _SINGLE_GAME_ROLE_TOKENS:
        return "FD_SINGLE", sport_key
    return site_key, sport_key


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
    rows: Sequence[ProjectionRow],
    *,
    site: str,
    sport: str,
    fallback_positions_by_id: Mapping[str, Sequence[str]] | None = None,
    fallback_positions_by_key: Mapping[str, Sequence[str]] | None = None,
) -> List[PlayerRecord]:
    records: List[PlayerRecord] = []
    fallback_positions_by_id = fallback_positions_by_id or {}
    fallback_positions_by_key = fallback_positions_by_key or {}
    for row in rows:
        positions = _canonical_positions(site, sport, row.raw_position)
        if not positions and _is_fanduel_single_game_defense(row, site=site, sport=sport):
            positions = ["D"]
        team_abbrev = _canonical_team(row.raw_team, sport) if row.raw_team else ""
        fallback_positions: Sequence[str] | None = None
        if not positions and row.raw_id:
            fallback_positions = fallback_positions_by_id.get(row.raw_id)
        if not positions and not fallback_positions:
            lookup_key = _record_key(row.raw_name, team_abbrev, is_defense=False)
            fallback_positions = fallback_positions_by_key.get(lookup_key)
        if not positions and not fallback_positions:
            defense_key = _record_key(row.raw_name, team_abbrev, is_defense=True)
            fallback_positions = fallback_positions_by_key.get(defense_key)
        if not positions and fallback_positions:
            positions = [pos for pos in fallback_positions if pos]
        metadata: dict[str, object] = {}
        if row.ownership is not None:
            metadata["projected_ownership"] = row.ownership
        projection_value = _parse_projection(row.raw_projection)
        metadata.setdefault("baseline_projection", projection_value)
        base_positions: Sequence[str] | None = None
        if positions:
            base_positions = positions
        elif fallback_positions:
            base_positions = fallback_positions
        if base_positions:
            metadata.setdefault("base_positions", tuple(base_positions))
        if row.raw_position is not None:
            metadata.setdefault("raw_position", row.raw_position)
        if row.raw_injury_status:
            metadata["injury_status"] = row.raw_injury_status.strip()
        flag = _parse_flag(row.raw_probable_pitcher)
        if flag is not None:
            metadata["probable_pitcher"] = flag
        records.append(
            PlayerRecord(
                player_id=row.raw_id or row.raw_name,
                name=row.raw_name,
                team=team_abbrev,
                positions=positions,
                salary=_parse_salary(row.raw_salary),
                projection=projection_value,
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


_NAME_SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}
_DEFENSE_TOKENS = {"dst", "defense", "def", "d"}
_SINGLE_GAME_DEFENSE_PATTERN = re.compile(r"\b(?:D/?ST|DST|DEF(?:ENSE)?)\b", re.IGNORECASE)


def _is_fanduel_single_game_defense(
    row: ProjectionRow, *, site: str, sport: str
) -> bool:
    if site.upper() not in {"FD", "FD_SINGLE"} or sport.upper() != "NFL":
        return False
    position = row.raw_position or ""
    if position.strip():
        return False
    if not row.raw_name:
        return False
    return bool(_SINGLE_GAME_DEFENSE_PATTERN.search(row.raw_name))


def _normalize_name(name: str, *, is_defense: bool = False, team: str | None = None) -> str:
    lowered = name.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    tokens = [tok for tok in cleaned.split() if tok]
    tokens = [tok for tok in tokens if tok not in _NAME_SUFFIX_TOKENS]

    if is_defense or any(tok in _DEFENSE_TOKENS for tok in tokens):
        if team:
            return team.lower()
        tokens = [tok for tok in tokens if tok not in _DEFENSE_TOKENS]
    return "".join(tokens)


def _record_key(name: str, team: str, *, is_defense: bool = False) -> str:
    return f"{_normalize_name(name, is_defense=is_defense, team=team)}::{team}"


def _load_single_game_position_lookup(
    players_path: Path,
    sport: str,
) -> tuple[dict[str, Sequence[str]], dict[str, Sequence[str]]]:
    """Build lookup tables of classic positions for FanDuel single-game slates."""

    from pydfs_lineup_optimizer.exceptions import LineupOptimizerIncorrectCSV
    from pydfs_lineup_optimizer.sites.fanduel.classic.importer import FanDuelCSVImporter

    by_id: dict[str, Sequence[str]] = {}
    by_key: dict[str, Sequence[str]] = {}

    try:
        importer = FanDuelCSVImporter(str(players_path))
        players = importer.import_players()
    except (FileNotFoundError, LineupOptimizerIncorrectCSV, ValueError) as exc:
        logger.debug("Unable to load FanDuel positions from %s: %s", players_path, exc)
        return by_id, by_key
    except Exception as exc:  # pragma: no cover - defensive logging only
        logger.warning(
            "Unexpected error loading FanDuel positions from %s: %s", players_path, exc
        )
        return by_id, by_key

    for player in players:
        positions = [pos.strip().upper() for pos in player.positions if pos.strip()]
        if not positions:
            continue
        by_id[player.id] = positions
        team_abbrev = _canonical_team(player.team, sport) if player.team else ""
        base_key = _record_key(player.full_name, team_abbrev, is_defense=False)
        by_key.setdefault(base_key, positions)
        defense_key = _record_key(player.full_name, team_abbrev, is_defense=True)
        by_key.setdefault(defense_key, positions)

    return by_id, by_key


def merge_player_and_projection_files(
    *,
    players_path: Path,
    projections_path: Optional[Path],
    site: str,
    sport: str,
    players_mapping: Optional[Mapping[str, str]] = None,
    projection_mapping: Optional[Mapping[str, str]] = None,
    players_rows: Sequence[ProjectionRow] | None = None,
    projection_rows: Sequence[ProjectionRow] | None = None,
) -> Tuple[List[PlayerRecord], MergeReport]:
    if players_rows is None:
        players_rows = load_projection_csv(players_path, mapping=players_mapping or DEFAULT_PLAYERS_MAPPING)

    fallback_positions_by_id: dict[str, Sequence[str]] = {}
    fallback_positions_by_key: dict[str, Sequence[str]] = {}
    if site.upper() == "FD_SINGLE":
        if players_path is not None:
            fallback_positions_by_id, fallback_positions_by_key = _load_single_game_position_lookup(
                players_path,
                sport,
            )
        else:
            logger.debug("Single-game fallback positions unavailable (players file missing)")

    base_records = {}
    for row, record in zip(
        players_rows,
        rows_to_records(
            players_rows,
            site=site,
            sport=sport,
            fallback_positions_by_id=fallback_positions_by_id,
            fallback_positions_by_key=fallback_positions_by_key,
        ),
    ):
        key = _record_key(row.raw_name, record.team, is_defense="D" in record.positions)
        base_records[key] = record

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

    overlay_rows = projection_rows or load_projection_csv(
        projections_path, mapping=projection_mapping or DEFAULT_PROJECTION_MAPPING
    )
    overlay_created_keys: set[str] = set()
    for row in overlay_rows:
        team_abbrev = _canonical_team(row.raw_team, sport)
        positions = _canonical_positions(site, sport, row.raw_position)
        if not positions and _is_fanduel_single_game_defense(row, site=site, sport=sport):
            positions = ["D"]
        fallback_positions: Sequence[str] | None = None
        if row.raw_id:
            fallback_positions = fallback_positions_by_id.get(row.raw_id)
        if not fallback_positions:
            fallback_positions = fallback_positions_by_key.get(
                _record_key(row.raw_name, team_abbrev, is_defense=False)
            )
        if not fallback_positions:
            fallback_positions = fallback_positions_by_key.get(
                _record_key(row.raw_name, team_abbrev, is_defense=True)
            )
        if not positions and fallback_positions:
            positions = [pos for pos in fallback_positions if pos]
        is_defense = "D" in positions
        key = _record_key(row.raw_name, team_abbrev, is_defense=is_defense)
        if key not in base_records:
            # create new record if enough info
            metadata = {}
            if row.ownership is not None:
                metadata["projected_ownership"] = row.ownership
            projection_value = _parse_projection(row.raw_projection)
            metadata.setdefault("baseline_projection", projection_value)
            if positions:
                metadata.setdefault("base_positions", tuple(positions))
            elif fallback_positions:
                metadata.setdefault("base_positions", tuple(fallback_positions))
            if row.raw_injury_status:
                metadata["injury_status"] = row.raw_injury_status.strip()
            flag = _parse_flag(row.raw_probable_pitcher)
            if flag is not None:
                metadata["probable_pitcher"] = flag
            try:
                base_records[key] = PlayerRecord(
                    player_id=row.raw_id or row.raw_name,
                    name=row.raw_name,
                    team=team_abbrev,
                    positions=positions,
                    salary=_parse_salary(row.raw_salary),
                    projection=projection_value,
                    metadata=metadata,
                )
            except ValueError:
                unmatched_projection_rows.append(row.raw_name)
                continue
            overlay_created_keys.add(key)
            continue

        existing = base_records[key]
        matched_keys.add(key)
        metadata = dict(existing.metadata)
        if row.ownership is not None:
            metadata["projected_ownership"] = row.ownership
        projection_value = _parse_projection(row.raw_projection)
        metadata["baseline_projection"] = projection_value
        if "base_positions" not in metadata:
            if positions:
                metadata["base_positions"] = tuple(positions)
            elif fallback_positions:
                metadata["base_positions"] = tuple(fallback_positions)
        if row.raw_position is not None and "raw_position" not in metadata:
            metadata["raw_position"] = row.raw_position
        flag = _parse_flag(row.raw_probable_pitcher)
        if flag is not None:
            metadata["probable_pitcher"] = flag
        update = {
            "projection": projection_value,
            "salary": _parse_salary(row.raw_salary, default=existing.salary),
            "metadata": metadata,
        }
        chosen_positions: Sequence[str] | None = None
        if positions:
            chosen_positions = positions
        elif fallback_positions:
            chosen_positions = fallback_positions
        if chosen_positions:
            update["positions"] = list(chosen_positions)
        if row.raw_id:
            update["player_id"] = row.raw_id
        if team_abbrev:
            update["team"] = team_abbrev

        base_records[key] = existing.model_copy(update=update)

    missing_projection = [
        base_records[key].name
        for key in base_records.keys()
        if key not in matched_keys
    ]

    sport_upper = sport.upper()
    filtered_records: List[PlayerRecord] = []
    filtered_matched = 0
    for key, record in base_records.items():
        if projections_path and key not in matched_keys and key not in overlay_created_keys:
            continue
        injury_status = str(record.metadata.get("injury_status", "")).strip().lower()
        if injury_status in {"out", "o", "inj", "injured", "ir", "il"}:
            continue
        if sport_upper == "MLB" and any(pos.startswith("P") for pos in record.positions):
            probable_flag = record.metadata.get("probable_pitcher")
            if probable_flag is False:
                continue
        if key in matched_keys or key in overlay_created_keys:
            filtered_matched += 1
        filtered_records.append(record)

    report = MergeReport(
        total_players=len(filtered_records),
        matched_players=filtered_matched,
        players_missing_projection=missing_projection,
        unmatched_projection_rows=unmatched_projection_rows,
    )
    return filtered_records, report


@dataclass(frozen=True)
class MergeReport:
    total_players: int
    matched_players: int
    players_missing_projection: List[str]
    unmatched_projection_rows: List[str]
