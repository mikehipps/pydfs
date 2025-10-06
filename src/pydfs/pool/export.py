"""Contest CSV export helpers for lineup pools."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
from typing import Iterable, Mapping, Sequence

from pydfs.api.schemas.lineup import LineupPlayerResponse, LineupResponse
from pydfs.config.roster import get_rules


class ContestExportError(RuntimeError):
    """Raised when a lineup cannot be exported for a contest template."""


@dataclass(frozen=True)
class ContestTemplate:
    """Representation of a contest export schema."""

    site: str
    sport: str
    headers: tuple[str, ...]
    slot_order: tuple[str, ...]
    include_entry_name: bool = True


_DEFAULT_HEADER_ALIASES: Mapping[str, str] = {
    "DEF": "DST",
}


def _slot_headers(slot_order: Sequence[str]) -> tuple[str, ...]:
    counts: dict[str, int] = {}
    headers: list[str] = []
    for slot in slot_order:
        key = _DEFAULT_HEADER_ALIASES.get(slot, slot)
        counts[key] = counts.get(key, 0) + 1
        if slot_order.count(slot) > 1 and key not in {"FLEX", "UTIL"}:
            headers.append(f"{key}{counts[key]}")
        else:
            headers.append(key)
    return tuple(headers)


def _resolve_template(site: str, sport: str) -> ContestTemplate:
    rules = get_rules(site, sport)
    headers = ("EntryName", *_slot_headers(rules.roster_order))
    return ContestTemplate(
        site=rules.site,
        sport=rules.sport,
        headers=headers,
        slot_order=rules.roster_order,
    )


def _assign_slots(
    lineup: LineupResponse,
    *,
    slot_order: Sequence[str],
    slot_positions: Mapping[str, Iterable[str]],
) -> list[LineupPlayerResponse]:
    """Return players matched to roster slots preserving slot order."""

    remaining = list(lineup.players)
    assignments: list[LineupPlayerResponse] = []

    for slot in slot_order:
        allowed = set(slot_positions.get(slot, {slot}))
        match_index = None
        for idx, player in enumerate(remaining):
            if allowed.intersection(player.positions):
                match_index = idx
                break
        if match_index is None:
            raise ContestExportError(
                f"Lineup {lineup.lineup_id} missing player for slot {slot}"
            )
        assignments.append(remaining.pop(match_index))

    if remaining:
        raise ContestExportError(
            f"Lineup {lineup.lineup_id} has extra players after slot assignment"
        )

    return assignments


def export_lineups_to_csv(
    lineups: Sequence[LineupResponse],
    *,
    site: str,
    sport: str,
    entry_names: Sequence[str] | None = None,
) -> str:
    """Convert lineups to a contest CSV format based on configured rules."""

    if entry_names is not None and len(entry_names) != len(lineups):
        raise ContestExportError("entry_names length must match lineups length")

    template = _resolve_template(site, sport)
    rules = get_rules(site, sport)

    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(template.headers)

    for idx, lineup in enumerate(lineups):
        entry_name = entry_names[idx] if entry_names is not None else lineup.lineup_id
        assignments = _assign_slots(
            lineup,
            slot_order=template.slot_order,
            slot_positions=rules.slot_positions,
        )
        row = [entry_name]
        for player in assignments:
            row.append(player.player_id)
        writer.writerow(row)

    return buffer.getvalue()


__all__ = [
    "ContestExportError",
    "export_lineups_to_csv",
]
