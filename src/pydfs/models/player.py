"""Canonical player models shared across ingestion and optimizer layers."""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict


class PlayerRecord(BaseModel):
    """Normalized player payload used by optimizer pipelines."""

    player_id: str = Field(..., min_length=1)
    name: str
    team: str
    positions: List[str]
    salary: int = Field(..., ge=0)
    projection: float = Field(..., ge=0.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)
