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
    baseline_projection: float


class LineupResponse(BaseModel):
    lineup_id: str
    salary: int
    projection: float
    baseline_projection: float
    players: List[LineupPlayerResponse]


class LineupRequest(BaseModel):
    site: str = Field(default="FD")
    sport: str = Field(default="NFL")
    lineups: int = Field(default=20, ge=1)
    lock_player_ids: list[str] | None = None
    exclude_player_ids: list[str] | None = None
    max_repeating_players: int | None = None
    max_from_one_team: int | None = None
    parallel_jobs: int | None = Field(default=None, ge=1, le=32)
    perturbation: float | None = Field(default=None, ge=0.0, le=100.0)
    perturbation_p25: float | None = Field(default=None, ge=0.0, le=100.0)
    perturbation_p75: float | None = Field(default=None, ge=0.0, le=100.0)
    max_exposure: float | None = Field(default=0.5, ge=0.0, le=1.0)
    lineups_per_job: int | None = Field(default=None, ge=1, le=500)
    min_salary: int | None = Field(default=None, ge=0)


class PlayerUsageResponse(BaseModel):
    player_id: str
    name: str
    team: str
    positions: List[str]
    count: int
    exposure: float
