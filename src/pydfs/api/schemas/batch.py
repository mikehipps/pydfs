from __future__ import annotations

from pydantic import BaseModel

from .lineup import LineupResponse, PlayerUsageResponse
from .mapping import MappingPreviewResponse


class LineupBatchResponse(BaseModel):
    run_id: str
    report: MappingPreviewResponse
    lineups: list[LineupResponse]
    player_usage: list[PlayerUsageResponse]
    message: str | None = None
    slate_id: str | None = None
    bias_summary: dict | None = None
