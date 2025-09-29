from __future__ import annotations

from pydantic import BaseModel, Field


class MappingPayload(BaseModel):
    players_mapping: dict[str, str] = Field(default_factory=dict)
    projection_mapping: dict[str, str] = Field(default_factory=dict)


class MappingPreviewResponse(BaseModel):
    total_players: int
    matched_players: int
    players_missing_projection: list[str]
    unmatched_projection_rows: list[str]
