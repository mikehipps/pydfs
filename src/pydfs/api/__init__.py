"""REST API for the pydfs optimizer."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from pydfs.api.schemas import (
    LineupBatchResponse,
    LineupPlayerResponse,
    LineupRequest,
    LineupResponse,
    MappingPreviewResponse,
)
from pydfs.config_loader import MappingProfile
from pydfs.ingest import merge_player_and_projection_files
from pydfs.optimizer import build_lineups


def _parse_mapping(mapping_str: str | None) -> dict[str, str]:
    if not mapping_str:
        return {}
    try:
        return json.loads(mapping_str)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid mapping JSON: {exc}") from exc


async def _write_temp(upload: UploadFile | None) -> Path | None:
    if upload is None:
        return None
    contents = await upload.read()
    if not contents:
        return None
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        tmp.write(contents)
        tmp.flush()
    finally:
        tmp.close()
    return Path(tmp.name)


def create_app() -> FastAPI:
    app = FastAPI(title="pydfs optimizer")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/preview", response_model=MappingPreviewResponse)
    async def preview(
        projections: UploadFile = File(...),
        players: UploadFile | None = File(None),
        projection_mapping: str | None = Form(None),
        players_mapping: str | None = Form(None),
    ) -> MappingPreviewResponse:
        proj_path = await _write_temp(projections)
        players_path = await _write_temp(players)
        if proj_path is None:
            raise HTTPException(status_code=400, detail="projections file is empty")

        try:
            if players_path:
                _, report = merge_player_and_projection_files(
                    players_path=players_path,
                    projections_path=proj_path,
                    site="FD",
                    sport="NFL",
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=_parse_mapping(projection_mapping) or None,
                )
            else:
                _, report = merge_player_and_projection_files(
                    players_path=proj_path,
                    projections_path=None,
                    site="FD",
                    sport="NFL",
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=None,
                )
        finally:
            proj_path.unlink(missing_ok=True)
            if players_path:
                players_path.unlink(missing_ok=True)

        return MappingPreviewResponse(
            total_players=report.total_players,
            matched_players=report.matched_players,
            players_missing_projection=report.players_missing_projection,
            unmatched_projection_rows=report.unmatched_projection_rows,
        )

    @app.post("/lineups", response_model=LineupBatchResponse)
    async def build(
        projections: UploadFile = File(...),
        players: UploadFile | None = File(None),
        lineup_request: str = Form("{}"),
        projection_mapping: str | None = Form(None),
        players_mapping: str | None = Form(None),
    ) -> LineupBatchResponse:
        try:
            request = LineupRequest.model_validate(json.loads(lineup_request or "{}"))
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid lineup_request JSON: {exc}") from exc

        proj_path = await _write_temp(projections)
        players_path = await _write_temp(players)
        if proj_path is None:
            raise HTTPException(status_code=400, detail="projections file is empty")

        try:
            if players_path:
                records, report = merge_player_and_projection_files(
                    players_path=players_path,
                    projections_path=proj_path,
                    site=request.site,
                    sport=request.sport,
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=_parse_mapping(projection_mapping) or None,
                )
            else:
                records, report = merge_player_and_projection_files(
                    players_path=proj_path,
                    projections_path=None,
                    site=request.site,
                    sport=request.sport,
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=None,
                )

            lineups = build_lineups(
                records,
                site=request.site,
                sport=request.sport,
                n_lineups=request.lineups,
                lock_player_ids=request.lock_player_ids,
                exclude_player_ids=request.exclude_player_ids,
                max_repeating_players=request.max_repeating_players,
                max_from_one_team=request.max_from_one_team,
            )
        finally:
            proj_path.unlink(missing_ok=True)
            if players_path:
                players_path.unlink(missing_ok=True)

        return LineupBatchResponse(
            report=MappingPreviewResponse(
                total_players=report.total_players,
                matched_players=report.matched_players,
                players_missing_projection=report.players_missing_projection,
                unmatched_projection_rows=report.unmatched_projection_rows,
            ),
            lineups=[
                LineupResponse(
                    lineup_id=lineup.lineup_id,
                    salary=lineup.salary,
                    projection=lineup.projection,
                    players=[
                        LineupPlayerResponse(
                            player_id=player.player_id,
                            name=player.name,
                            team=player.team,
                            positions=list(player.positions),
                            salary=player.salary,
                            projection=player.projection,
                            ownership=player.ownership,
                        )
                        for player in lineup.players
                    ],
                )
                for lineup in lineups
            ],
        )

    return app
