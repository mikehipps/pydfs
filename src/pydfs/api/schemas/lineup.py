from __future__ import annotations

from typing import List, Literal

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
    exposure_bias: float | None = Field(default=None, ge=0.0, le=100.0)
    exposure_bias_target: float | None = Field(default=None, ge=0.0, le=100.0)
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


class PoolFilterRequest(BaseModel):
    slate_id: str | None = None
    run_ids: List[str] | None = None
    site: str | None = None
    sport: str | None = None
    all_dates: bool = False
    min_baseline: float | None = Field(default=None, ge=0.0)
    max_baseline: float | None = Field(default=None, ge=0.0)
    min_projection: float | None = Field(default=None, ge=0.0)
    max_projection: float | None = Field(default=None, ge=0.0)
    min_salary: int | None = Field(default=None, ge=0)
    max_salary: int | None = Field(default=None, ge=0)
    min_usage_sum: float | None = Field(default=None, ge=0.0)
    max_usage_sum: float | None = Field(default=None, ge=0.0)
    min_uniqueness: float | None = Field(default=None, ge=0.0)
    max_uniqueness: float | None = Field(default=None, ge=0.0)
    include_player_ids: List[str] | None = None
    exclude_player_ids: List[str] | None = None
    include_team_codes: List[str] | None = None
    exclude_team_codes: List[str] | None = None
    limit: int | None = Field(default=20, ge=1, le=500)
    sort_by: Literal["baseline", "projection", "salary", "usage", "uniqueness"] = "baseline"
    sort_direction: Literal["asc", "desc"] = "desc"
    run_limit: int | None = Field(default=None, ge=1, le=500)


class PoolFilterSummary(BaseModel):
    available_lineups: int
    selected_lineups: int
    total_instances: int
    baseline_mean: float | None
    baseline_median: float | None
    baseline_std: float | None
    projection_mean: float | None
    usage_mean: float | None
    uniqueness_mean: float | None


class PoolFilteredLineup(BaseModel):
    rank: int
    lineup_id: str
    run_ids: List[str]
    salary: int
    projection: float
    baseline_projection: float
    usage_sum: float
    uniqueness: float
    count: int
    players: List[LineupPlayerResponse]


class PoolFilterResponse(BaseModel):
    pool_summary: PoolFilterSummary
    summary: PoolFilterSummary
    lineups: List[PoolFilteredLineup]
