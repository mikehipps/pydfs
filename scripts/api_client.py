"""Lightweight REST client for the pydfs API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx


def build_mapping(name: str) -> dict[str, str]:
    if not name:
        return {}
    try:
        return json.loads(name)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid mapping JSON: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Interact with the pydfs REST API")
    parser.add_argument("base_url", help="Base URL of the API, e.g. http://localhost:8000")
    parser.add_argument("projections", type=Path, help="Projections CSV")
    parser.add_argument("players", type=Path, help="Players CSV")
    parser.add_argument("--lineups", type=int, default=20, help="Number of lineups to request")
    parser.add_argument("--projection-mapping", default="", help="JSON mapping for projection columns")
    parser.add_argument("--players-mapping", default="", help="JSON mapping for player columns")
    parser.add_argument("--preview-only", action="store_true", help="Fetch merge diagnostics without building lineups")
    args = parser.parse_args()

    def make_files() -> dict[str, tuple[str, bytes, str]]:
        return {
            "projections": (args.projections.name, args.projections.read_bytes(), "text/csv"),
            "players": (args.players.name, args.players.read_bytes(), "text/csv"),
        }

    data = {
        "projection_mapping": args.projection_mapping or None,
        "players_mapping": args.players_mapping or None,
    }

    with httpx.Client(base_url=args.base_url) as client:
        resp = client.post("/preview", files=make_files(), data=data)
        resp.raise_for_status()
        report = resp.json()
        print("Preview report:", json.dumps(report, indent=2))

        if args.preview_only:
            return

        lineup_request = {"lineups": args.lineups}
        data["lineup_request"] = json.dumps(lineup_request)
        resp = client.post("/lineups", files=make_files(), data=data)
        resp.raise_for_status()
        payload = resp.json()
        print("Merge report:", json.dumps(payload["report"], indent=2))
        print(f"Received {len(payload['lineups'])} lineups")
        print(json.dumps(payload["lineups"][0], indent=2))


if __name__ == "__main__":
    main()
