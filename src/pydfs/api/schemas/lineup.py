from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class LineupPlayerResponse(BaseModel):
    player_id: str
    name: str
    team: str
    positions: List[str]
    salary: int
    projection: float
    ownership: float | None


class LineupResponse(BaseModel):
    lineup_id: str
    salary: int
    projection: float
    players: List[LineupPlayerResponse]


class LineupRequest(BaseModel):
    site: str = Field(default="FD")
    sport: str = Field(default="NFL")
    lineups: int = Field(default=20, ge=1)
    lock_player_ids: list[str] | None = None
    exclude_player_ids: list[str] | None = None
    max_repeating_players: int | None = None
    max_from_one_team: int | None = None
