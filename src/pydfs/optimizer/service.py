"""Wrapper utilities for running pydfs-lineup-optimizer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from pydfs_lineup_optimizer import Site, Sport, get_optimizer
from pydfs_lineup_optimizer.lineup import Lineup

from pydfs.models import PlayerRecord


@dataclass(frozen=True)
class LineupPlayer:
    player_id: str
    name: str
    team: str
    positions: Tuple[str, ...]
    salary: int
    projection: float
    ownership: Optional[float] = None


@dataclass(frozen=True)
class LineupResult:
    lineup_id: str
    players: Tuple[LineupPlayer, ...]
    salary: int
    projection: float


_SITE_ALIASES = {
    "FD": Site.FANDUEL,
    "FD_SINGLE": Site.FANDUEL_SINGLE_GAME,
    "DK": Site.DRAFTKINGS,
    "DK_CAPTAIN": Site.DRAFTKINGS_CAPTAIN_MODE,
    "YAHOO": Site.YAHOO,
}

_SPORT_ALIASES = {
    "MLB": Sport.BASEBALL,
    "NBA": Sport.BASKETBALL,
    "NFL": Sport.FOOTBALL,
    "NHL": Sport.HOCKEY,
    "WNBA": Sport.WNBA,
}


def _resolve_site(site: str) -> str:
    key = site.upper()
    if key not in _SITE_ALIASES:
        raise KeyError(f"Unsupported site {site!r}")
    return _SITE_ALIASES[key]


def _resolve_sport(sport: str) -> str:
    key = sport.upper()
    if key not in _SPORT_ALIASES:
        raise KeyError(f"Unsupported sport {sport!r}")
    return _SPORT_ALIASES[key]


def _split_name(full_name: str) -> Tuple[str, str]:
    parts = full_name.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _to_pydfs_players(records: Sequence[PlayerRecord]):
    from pydfs_lineup_optimizer.player import Player as PydfsPlayer

    dfs_players: List[PydfsPlayer] = []
    for record in records:
        first, last = _split_name(record.name)
        dfs_players.append(
            PydfsPlayer(
                player_id=record.player_id,
                first_name=first,
                last_name=last,
                positions=list(record.positions) or [""],
                team=record.team,
                salary=float(record.salary),
                fppg=float(record.projection),
                projected_ownership=record.metadata.get("projected_ownership"),
                fppg_floor=record.metadata.get("projection_floor"),
                fppg_ceil=record.metadata.get("projection_ceil"),
            )
        )
    return dfs_players


def _lineup_to_result(lineup: Lineup, idx: int) -> LineupResult:
    players = tuple(
        LineupPlayer(
            player_id=p.id,
            name=f"{p.first_name} {p.last_name}".strip(),
            team=p.team,
            positions=tuple(p.positions),
            salary=int(p.salary),
            projection=float(p.fppg),
            ownership=getattr(p, "projected_ownership", None),
        )
        for p in lineup.players
    )
    return LineupResult(
        lineup_id=f"L{idx + 1:03}",
        players=players,
        salary=int(lineup.salary_costs),
        projection=float(lineup.fantasy_points_projection),
    )


def build_lineups(
    records: Sequence[PlayerRecord],
    *,
    site: str,
    sport: str,
    n_lineups: int = 20,
    max_repeating_players: Optional[int] = None,
    max_from_one_team: Optional[int] = None,
    lock_player_ids: Optional[Iterable[str]] = None,
    exclude_player_ids: Optional[Iterable[str]] = None,
) -> List[LineupResult]:
    """Generate lineups from the supplied player pool."""

    optimizer = get_optimizer(_resolve_site(site), _resolve_sport(sport))

    dfs_players = _to_pydfs_players(records)
    optimizer.player_pool.load_players(dfs_players)

    pool = optimizer.player_pool

    if lock_player_ids:
        for pid in lock_player_ids:
            player = pool.get_player_by_id(pid)
            if player is not None:
                pool.lock_player(player)

    if exclude_player_ids:
        for pid in exclude_player_ids:
            player = pool.get_player_by_id(pid)
            if player is not None:
                pool.remove_player(player)

    if max_repeating_players is not None:
        optimizer.max_repeating_players = max_repeating_players

    if max_from_one_team is not None:
        optimizer.set_players_from_one_team(max_from_one_team)

    results: List[LineupResult] = []
    for idx, lineup in enumerate(optimizer.optimize(n_lineups)):
        results.append(_lineup_to_result(lineup, idx))

    return results
