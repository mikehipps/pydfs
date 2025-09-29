"""REST API for the pydfs optimizer."""

from __future__ import annotations

import csv
import json
import tempfile
from html import escape
from io import StringIO
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Request
from fastapi.responses import HTMLResponse, Response

from pydfs.api.schemas import (
    LineupBatchResponse,
    LineupPlayerResponse,
    LineupRequest,
    LineupResponse,
    MappingPreviewResponse,
)
from pydfs.ingest import merge_player_and_projection_files
from pydfs.optimizer import build_lineups
from pydfs.persistence import RunRecord, RunStore


DEFAULT_PLAYERS_MAPPING = {
    "player_id": "Id",
    "name": "First Name|Last Name",
    "team": "Team",
    "position": "Position",
    "salary": "Salary",
    "projection": "FPPG",
}

DEFAULT_PROJECTION_MAPPING = {
    "name": "player",
    "team": "team",
    "salary": "salary",
    "projection": "fantasy",
    "ownership": "proj_own",
}


def run_record_to_dict(run: RunRecord) -> dict:
    return {
        "run_id": run.run_id,
        "created_at": run.created_at.isoformat(),
        "site": run.site,
        "sport": run.sport,
        "request": run.request,
        "report": run.report,
        "lineups": run.lineups,
        "players_mapping": run.players_mapping,
        "projection_mapping": run.projection_mapping,
    }


def _json_pretty(data: dict) -> str:
    return json.dumps(data, indent=2)


def _render_page(body: str) -> str:
    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <title>pydfs Optimizer</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f5f7fa; }}
        main {{ background: #fff; padding: 2rem; border-radius: 12px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
        nav a {{ margin-right: 1rem; color: #2563eb; text-decoration: none; }}
        form {{ display: grid; gap: 1rem; margin-bottom: 2rem; }}
        label {{ font-weight: 600; }}
        input[type=\"file\"], textarea, input[type=\"number\"] {{ width: 100%; padding: 0.5rem; border-radius: 6px; border: 1px solid #cbd5e1; }}
        button {{ padding: 0.6rem 1.2rem; border-radius: 6px; border: none; background: #2563eb; color: #fff; cursor: pointer; }}
        button.secondary {{ background: #475569; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; }}
        th, td {{ padding: 0.5rem; border: 1px solid #e2e8f0; text-align: left; }}
        .flash {{ padding: 1rem; border-radius: 6px; margin-bottom: 1rem; }}
        .flash.success {{ background: #ecfdf5; color: #047857; }}
        .flash.error {{ background: #fef2f2; color: #b91c1c; }}
        .runs-list ul {{ list-style: none; padding: 0; }}
        .runs-list li {{ margin-bottom: 0.5rem; }}
        .runs-list a {{ color: #2563eb; text-decoration: none; }}
    </style>
</head>
<body>
    <nav><a href=\"/ui\">Home</a></nav>
    <main>{body}</main>
</body>
</html>"""


def _render_index_page(
    runs: list[RunRecord],
    preview,
    result,
    error: str | None,
    success: str | None,
    players_mapping_json: str,
    projection_mapping_json: str,
    lineups_count: int,
) -> str:
    players_mapping_html = escape(players_mapping_json)
    projection_mapping_html = escape(projection_mapping_json)

    form_html = f"""
    <form method=\"post\" action=\"/ui\" enctype=\"multipart/form-data\">
        <label>Players CSV</label>
        <input type=\"file\" name=\"players\" required>

        <label>Projections CSV</label>
        <input type=\"file\" name=\"projections\" required>

        <label>Players mapping (JSON)</label>
        <textarea name=\"players_mapping\" rows=\"4\">{players_mapping_html}</textarea>

        <label>Projections mapping (JSON)</label>
        <textarea name=\"projection_mapping\" rows=\"4\">{projection_mapping_html}</textarea>

        <label>Number of lineups</label>
        <input type=\"number\" name=\"lineups\" min=\"1\" value=\"{lineups_count}\">

        <div>
            <button type=\"submit\" name=\"submit_action\" value=\"preview\">Preview</button>
            <button type=\"submit\" name=\"submit_action\" value=\"lineups\" class=\"secondary\">Build Lineups</button>
        </div>
    </form>
    """

    flash_html = ""
    if error:
        flash_html += f"<div class='flash error'>{escape(error)}</div>"
    if success:
        flash_html += f"<div class='flash success'>{escape(success)}</div>"

    preview_html = ""
    if preview:
        missing = ", ".join(map(escape, preview.players_missing_projection)) if preview.players_missing_projection else "None"
        unmatched = ", ".join(map(escape, preview.unmatched_projection_rows)) if preview.unmatched_projection_rows else "None"
        preview_html = f"""
        <section>
            <h2>Preview Report</h2>
            <table>
                <tr><th>Total Players</th><td>{preview.total_players}</td></tr>
                <tr><th>Matched Players</th><td>{preview.matched_players}</td></tr>
                <tr><th>Players Missing Projection</th><td>{len(preview.players_missing_projection)}</td></tr>
                <tr><th>Projection Rows Without Players</th><td>{len(preview.unmatched_projection_rows)}</td></tr>
            </table>
            <p><strong>Missing Projections:</strong> {missing}</p>
            <p><strong>Unmatched Projections:</strong> {unmatched}</p>
        </section>
        """

    result_html = ""
    if result:
        lineup_rows = "".join(
            f"<tr><td>{'/'.join(player.positions)}</td>"
            f"<td>{escape(player.name)}</td>"
            f"<td>{escape(player.team)}</td>"
            f"<td>{player.salary}</td>"
            f"<td>{player.projection:.2f}</td>"
            f"<td>{'-' if player.ownership is None else f'{player.ownership:.1f}'}</td></tr>"
            for player in result["lineup"].players
        )
        result_html = f"""
        <section>
            <h2>Run Saved</h2>
            <p>Run ID: <a href=\"/ui/runs/{result['run_id']}\">{result['run_id']}</a></p>
            <h3>Top Lineup</h3>
            <table>
                <thead><tr><th>Position</th><th>Player</th><th>Team</th><th>Salary</th><th>Projection</th><th>Ownership</th></tr></thead>
                <tbody>{lineup_rows}</tbody>
            </table>
        </section>
        """

    runs_html = "".join(
        f"<li><a href=\"/ui/runs/{run.run_id}\">{run.run_id}</a> – {run.created_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')} ({run.site} {run.sport})</li>"
        for run in runs
    ) or "<p>No runs yet.</p>"

    runs_section = f"""
    <section class=\"runs-list\">
        <h2>Recent Runs</h2>
        <ul>{runs_html}</ul>
    </section>
    """

    body = flash_html + form_html + preview_html + result_html + runs_section
    return _render_page(body)


def _run_to_csv(run: RunRecord) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "run_id", "lineup_id", "slot", "player_id", "name", "team", "positions", "salary", "projection", "ownership"
    ])
    for lineup_data in run.lineups:
        lineup = LineupResponse.model_validate(lineup_data)
        for slot, player in enumerate(lineup.players, start=1):
            writer.writerow([
                run.run_id,
                lineup.lineup_id,
                slot,
                player.player_id,
                player.name,
                player.team,
                "/".join(player.positions),
                player.salary,
                f"{player.projection:.4f}",
                "" if player.ownership is None else f"{player.ownership:.2f}",
            ])
    return buffer.getvalue()



def _render_run_detail_page(run: RunRecord) -> str:
    report = run.report
    lineups_html_parts = []
    for lineup_data in run.lineups:
        lineup = LineupResponse.model_validate(lineup_data)
        rows = "".join(
            f"<tr><td>{'/'.join(player.positions)}</td><td>{escape(player.name)}</td><td>{escape(player.team)}</td>"
            f"<td>{player.salary}</td><td>{player.projection:.2f}</td><td>{'-' if player.ownership is None else f'{player.ownership:.1f}'}</td></tr>"
            for player in lineup.players
        )
        lineups_html_parts.append(
            f"<h3>{lineup.lineup_id} – Salary {lineup.salary} – Projection {lineup.projection:.2f}</h3>"
            f"<table><thead><tr><th>Position</th><th>Player</th><th>Team</th><th>Salary</th><th>Projection</th><th>Ownership</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
    lineups_html = "".join(lineups_html_parts)

    body = f"""
    <section>
        <h1>Run {run.run_id}</h1>
        <p><strong>Created:</strong> {run.created_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Site/Sport:</strong> {run.site} {run.sport}</p>

        <h2>Merge Report</h2>
        <table>
            <tr><th>Total Players</th><td>{report['total_players']}</td></tr>
            <tr><th>Matched Players</th><td>{report['matched_players']}</td></tr>
            <tr><th>Missing Projections</th><td>{len(report['players_missing_projection'])}</td></tr>
            <tr><th>Unmatched Projections</th><td>{len(report['unmatched_projection_rows'])}</td></tr>
        </table>
    </section>
    <section>
        <h2>Lineups</h2>
        {lineups_html}
    </section>
    <p><a href=\"/ui\">Back to runs</a></p>
    """
    return _render_page(body)


def _run_to_csv(run: RunRecord) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "run_id", "lineup_id", "slot", "player_id", "name", "team", "positions", "salary", "projection", "ownership"
    ])
    for lineup_data in run.lineups:
        lineup = LineupResponse.model_validate(lineup_data)
        for slot, player in enumerate(lineup.players, start=1):
            writer.writerow([
                run.run_id,
                lineup.lineup_id,
                slot,
                player.player_id,
                player.name,
                player.team,
                "/".join(player.positions),
                player.salary,
                f"{player.projection:.4f}",
                "" if player.ownership is None else f"{player.ownership:.2f}",
            ])
    return buffer.getvalue()



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
    store = RunStore(Path(__file__).resolve().parent.parent / "pydfs.sqlite")
    # UI helpers rely on default mappings in JSON string form
    default_players_mapping_json = _json_pretty(DEFAULT_PLAYERS_MAPPING)
    default_projection_mapping_json = _json_pretty(DEFAULT_PROJECTION_MAPPING)

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
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
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

        parsed_players_mapping = _parse_mapping(players_mapping) or {}
        parsed_projection_mapping = _parse_mapping(projection_mapping) or {}

        try:
            if players_path:
                records, report = merge_player_and_projection_files(
                    players_path=players_path,
                    projections_path=proj_path,
                    site=request.site,
                    sport=request.sport,
                    players_mapping=parsed_players_mapping or None,
                    projection_mapping=parsed_projection_mapping or None,
                )
            else:
                records, report = merge_player_and_projection_files(
                    players_path=proj_path,
                    projections_path=None,
                    site=request.site,
                    sport=request.sport,
                    players_mapping=parsed_players_mapping or None,
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
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            proj_path.unlink(missing_ok=True)
            if players_path:
                players_path.unlink(missing_ok=True)
        lineups_payload = [
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
        ]

        run_id = uuid4().hex
        store.save_run(
            run_id=run_id,
            site=request.site,
            sport=request.sport,
            request={
                "lineups": request.lineups,
                "lock_player_ids": request.lock_player_ids,
                "exclude_player_ids": request.exclude_player_ids,
                "max_repeating_players": request.max_repeating_players,
                "max_from_one_team": request.max_from_one_team,
            },
            report={
                "total_players": report.total_players,
                "matched_players": report.matched_players,
                "players_missing_projection": report.players_missing_projection,
                "unmatched_projection_rows": report.unmatched_projection_rows,
            },
            lineups=[lineup.model_dump() for lineup in lineups_payload],
            players_mapping=parsed_players_mapping,
            projection_mapping=parsed_projection_mapping,
        )

        return LineupBatchResponse(
            run_id=run_id,
            report=MappingPreviewResponse(
                total_players=report.total_players,
                matched_players=report.matched_players,
                players_missing_projection=report.players_missing_projection,
                unmatched_projection_rows=report.unmatched_projection_rows,
            ),
            lineups=lineups_payload,
        )

    @app.get("/runs")
    async def list_runs(limit: int = 50):
        runs = store.list_runs(limit=limit)
        return [
            {
                "run_id": run.run_id,
                "created_at": run.created_at.isoformat(),
                "site": run.site,
                "sport": run.sport,
            }
            for run in runs
        ]

    def _fetch_run_or_404(run_id: str):
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return run

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str):
        run = _fetch_run_or_404(run_id)
        return run_record_to_dict(run)

    @app.post("/runs/{run_id}/rerun", response_model=LineupBatchResponse)
    async def rerun(run_id: str):
        run = _fetch_run_or_404(run_id)
        return LineupBatchResponse(
            run_id=run.run_id,
            report=MappingPreviewResponse(
                total_players=run.report["total_players"],
                matched_players=run.report["matched_players"],
                players_missing_projection=run.report["players_missing_projection"],
                unmatched_projection_rows=run.report["unmatched_projection_rows"],
            ),
            lineups=[LineupResponse.model_validate(lineup) for lineup in run.lineups],
        )

    @app.get("/runs/{run_id}/export.csv")
    async def export_csv(run_id: str):
        run = _fetch_run_or_404(run_id)
        csv_text = _run_to_csv(run)
        return Response(
            content=csv_text,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={run_id}.csv"},
        )

    @app.get("/ui", response_class=HTMLResponse)
    async def ui_index(request: Request):
        runs = store.list_runs(limit=20)
        content = _render_index_page(
            runs=runs,
            preview=None,
            result=None,
            error=None,
            success=None,
            players_mapping_json=default_players_mapping_json,
            projection_mapping_json=default_projection_mapping_json,
            lineups_count=20,
        )
        return HTMLResponse(content)

    @app.post("/ui", response_class=HTMLResponse)
    async def ui_handle(
        request: Request,
        submit_action: str = Form(...),
        players: UploadFile = File(...),
        projections: UploadFile = File(...),
        players_mapping: str = Form(""),
        projection_mapping: str = Form(""),
        lineups: int = Form(20),
    ):
        parsed_players_mapping = _parse_mapping(players_mapping) or DEFAULT_PLAYERS_MAPPING
        parsed_projection_mapping = _parse_mapping(projection_mapping) or DEFAULT_PROJECTION_MAPPING

        proj_path = await _write_temp(projections)
        players_path = await _write_temp(players)
        if proj_path is None or players_path is None:
            raise HTTPException(status_code=400, detail="Players and projections files are required")

        preview = None
        result = None
        error = None
        success = None

        try:
            records, report = merge_player_and_projection_files(
                players_path=players_path,
                projections_path=proj_path,
                site="FD",
                sport="NFL",
                players_mapping=parsed_players_mapping,
                projection_mapping=parsed_projection_mapping,
            )

            if submit_action == "preview":
                preview = report
                success = "Preview generated. Review report below."
            else:
                built_lineups = build_lineups(
                    records,
                    site="FD",
                    sport="NFL",
                    n_lineups=lineups,
                )
                lineups_payload = [
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
                    for lineup in built_lineups
                ]

                run_id = uuid4().hex
                store.save_run(
                    run_id=run_id,
                    site="FD",
                    sport="NFL",
                    request={"lineups": lineups},
                    report={
                        "total_players": report.total_players,
                        "matched_players": report.matched_players,
                        "players_missing_projection": report.players_missing_projection,
                        "unmatched_projection_rows": report.unmatched_projection_rows,
                    },
                    lineups=[lineup.model_dump() for lineup in lineups_payload],
                    players_mapping=parsed_players_mapping,
                    projection_mapping=parsed_projection_mapping,
                )

                success = f"Run saved with ID {run_id}"
                result = {
                    "run_id": run_id,
                    "lineup": lineups_payload[0],
                }

        except ValueError as exc:
            error = str(exc)
        finally:
            proj_path.unlink(missing_ok=True)
            if players_path:
                players_path.unlink(missing_ok=True)

        runs = store.list_runs(limit=20)
        content = _render_index_page(
            runs=runs,
            preview=preview,
            result=result,
            error=error,
            success=success,
            players_mapping_json=_json_pretty(parsed_players_mapping),
            projection_mapping_json=_json_pretty(parsed_projection_mapping),
            lineups_count=lineups,
        )
        return HTMLResponse(content)

    @app.get("/ui/runs/{run_id}", response_class=HTMLResponse)
    async def ui_run_detail(request: Request, run_id: str):
        run = _fetch_run_or_404(run_id)
        content = _render_run_detail_page(run)
        return HTMLResponse(content)


    return app
