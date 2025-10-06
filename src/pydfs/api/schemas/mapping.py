from __future__ import annotations

from pydantic import BaseModel, Field


class MappingPayload(BaseModel):
    players_mapping: dict[str, str] = Field(default_factory=dict)
    projection_mapping: dict[str, str] = Field(default_factory=dict)


class ManualReviewItemResponse(BaseModel):
    identifier: str
    name: str
    team: str
    team_abbreviation: str | None = None
    projection: float | None = None
    salary: int | None = None
    game: str | None = None
    reason: str


class MappingPreviewResponse(BaseModel):
    total_players: int
    matched_players: int
    players_missing_projection: list[str]
    unmatched_projection_rows: list[str]
    manual_review: list[ManualReviewItemResponse] = Field(default_factory=list)
    ignored_projection_rows: list[str] = Field(default_factory=list)
