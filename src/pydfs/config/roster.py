"""Roster configuration for supported site/sport combinations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Set, Tuple, Union


@dataclass(frozen=True)
class RosterRules:
    site: str
    sport: str
    salary_cap: int
    roster_order: Tuple[str, ...]
    slot_positions: Mapping[str, Set[str]]
    team_max_players: int
    stacking_slots: Set[str]


_ROSTER_RULES: Dict[Tuple[str, str], RosterRules] = {
    ("FD", "MLB"): RosterRules(
        site="FD",
        sport="MLB",
        salary_cap=35_000,
        roster_order=("P", "C1B", "2B", "3B", "SS", "OF", "OF", "OF", "UTIL"),
        slot_positions={
            "P": {"P"},
            "C1B": {"C", "1B", "C/1B"},
            "2B": {"2B"},
            "3B": {"3B"},
            "SS": {"SS"},
            "OF": {"OF"},
            "UTIL": {"C", "1B", "2B", "3B", "SS", "OF", "C/1B"},
        },
        team_max_players=4,
        stacking_slots={"C1B", "2B", "3B", "SS", "OF", "UTIL"},
    ),
    ("FD", "NFL"): RosterRules(
        site="FD",
        sport="NFL",
        salary_cap=60_000,
        roster_order=("QB", "RB", "RB", "WR", "WR", "WR", "TE", "FLEX", "DEF"),
        slot_positions={
            "QB": {"QB"},
            "RB": {"RB"},
            "WR": {"WR"},
            "TE": {"TE"},
            "FLEX": {"RB", "WR", "TE"},
            "DEF": {"D"},
        },
        team_max_players=4,
        stacking_slots={"QB", "RB", "WR", "TE", "FLEX"},
    ),
    ("FD_SINGLE", "NFL"): RosterRules(
        site="FD_SINGLE",
        sport="NFL",
        salary_cap=60_000,
        roster_order=("MVP", "UTIL", "UTIL", "UTIL", "UTIL"),
        slot_positions={
            "MVP": {"MVP"},
            "UTIL": {"QB", "RB", "WR", "TE", "K"},
        },
        team_max_players=4,
        stacking_slots={"UTIL"},
    ),
    ("FD_SINGLE", "NBA"): RosterRules(
        site="FD_SINGLE",
        sport="NBA",
        salary_cap=60_000,
        roster_order=("MVP", "STAR", "PRO", "UTIL", "UTIL"),
        slot_positions={
            "MVP": {"MVP"},
            "STAR": {"STAR"},
            "PRO": {"PRO"},
            "UTIL": {"PG", "SG", "SF", "PF", "C"},
        },
        team_max_players=4,
        stacking_slots={"UTIL"},
    ),
    ("FD_SINGLE", "MLB"): RosterRules(
        site="FD_SINGLE",
        sport="MLB",
        salary_cap=60_000,
        roster_order=("MVP", "STAR", "UTIL", "UTIL", "UTIL"),
        slot_positions={
            "MVP": {"MVP"},
            "STAR": {"STAR"},
            "UTIL": {"1B", "2B", "3B", "SS", "OF", "C", "C/1B"},
        },
        team_max_players=4,
        stacking_slots={"UTIL"},
    ),
    ("FD_SINGLE", "NHL"): RosterRules(
        site="FD_SINGLE",
        sport="NHL",
        salary_cap=60_000,
        roster_order=("CAPTAIN", "UTIL", "UTIL", "UTIL", "UTIL"),
        slot_positions={
            "CAPTAIN": {"CAPTAIN"},
            "UTIL": {"C", "W", "D"},
        },
        team_max_players=4,
        stacking_slots={"UTIL"},
    ),
}


def iter_rules() -> Iterable[RosterRules]:
    """Return an iterator of all configured rule sets."""

    return _ROSTER_RULES.values()


def get_rules(site: str, sport: str) -> RosterRules:
    """Fetch rules for a site/sport pair, raising KeyError if missing."""

    key = (site.upper(), sport.upper())
    if key not in _ROSTER_RULES:
        raise KeyError(f"No roster rules configured for site={site!r}, sport={sport!r}")
    return _ROSTER_RULES[key]


def get_rules_by_key(site_key: Union[str, Tuple[str, str]]) -> RosterRules:
    """Resolve rules using either "SITE_SPORT" or (site, sport)."""

    if isinstance(site_key, tuple):
        site, sport = site_key
        return get_rules(site, sport)

    if not isinstance(site_key, str):
        raise TypeError("site_key must be a str or (site, sport) tuple")

    parts = site_key.split("_", 1)
    if len(parts) != 2:
        raise ValueError(f"site_key must look like 'SITE_SPORT', got {site_key!r}")

    site, sport = parts
    return get_rules(site, sport)


# Convenience mapping for legacy access patterns; intentionally read-only.
SITE_CONFIG: Mapping[str, RosterRules] = {
    f"{site}_{sport}": rules for (site, sport), rules in _ROSTER_RULES.items()
}

