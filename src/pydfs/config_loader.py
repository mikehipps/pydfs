"""Persist and load CLI mapping profiles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class MappingProfile:
    players_mapping: Dict[str, str]
    projection_mapping: Dict[str, str]

    @classmethod
    def load(cls, path: Path) -> "MappingProfile":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            players_mapping=data.get("players_mapping", {}),
            projection_mapping=data.get("projection_mapping", {}),
        )

    def save(self, path: Path) -> None:
        payload = {
            "players_mapping": self.players_mapping,
            "projection_mapping": self.projection_mapping,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
