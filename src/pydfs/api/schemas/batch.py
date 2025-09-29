from __future__ import annotations

from pydantic import BaseModel

from .lineup import LineupResponse
from .mapping import MappingPreviewResponse


class LineupBatchResponse(BaseModel):
    report: MappingPreviewResponse
    lineups: list[LineupResponse]
