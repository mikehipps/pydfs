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

PLAYERS_MAPPING_FIELDS = [
    ("player_id", "Player ID", "Unique identifier used to join players and projections."),
    ("name", "Player Name", "Full player name; can combine columns with |."),
    ("team", "Team", "Team abbreviation for roster rules."),
    ("position", "Positions", "Slash- or comma-separated positions."),
    ("salary", "Salary", "Player salary column."),
    ("projection", "Projection", "Baseline projection included with player pool."),
]

PROJECTION_MAPPING_FIELDS = [
    ("name", "Player Name", "Name column in the projections file."),
    ("team", "Team", "Team column in the projections file."),
    ("salary", "Salary", "Salary from projections to validate alignment."),
    ("projection", "Projection", "Projection value used by the optimizer."),
    ("ownership", "Ownership", "Optional ownership percentage column."),
]


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
        .mapping-section {{ border: 1px solid #e2e8f0; padding: 1rem; border-radius: 8px; background: #f8fafc; }}
        .mapping-section + .mapping-section {{ margin-top: 0.5rem; }}
        .mapping-grid {{ display: grid; gap: 1rem; margin-top: 0.75rem; }}
        .mapping-field label {{ display: block; font-weight: 600; margin-bottom: 0.25rem; }}
        .mapping-field select {{ width: 100%; padding: 0.5rem; border-radius: 6px; border: 1px solid #cbd5e1; background: #fff; }}
        .mapping-field .custom-value {{ width: 100%; padding: 0.5rem; border-radius: 6px; border: 1px solid #cbd5e1; margin-top: 0.4rem; }}
        .mapping-field small {{ display: block; color: #64748b; margin-top: 0.25rem; }}
        .hint {{ color: #475569; margin: 0; }}
        .mapping-preview {{ margin-top: 0.75rem; }}
        .mapping-preview summary {{ cursor: pointer; color: #2563eb; }}
        .mapping-preview pre {{ margin-top: 0.5rem; padding: 0.75rem; border-radius: 6px; background: #0f172a; color: #e2e8f0; overflow-x: auto; max-height: 240px; }}
        .form-actions {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
        .form-actions button {{ flex: 1 1 200px; }}
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
    players_mapping: dict[str, str],
    projection_mapping: dict[str, str],
    lineups_count: int,
) -> str:
    def _mapping_section(
        section_id: str,
        title: str,
        description: str,
        fields: list[tuple[str, str, str]],
        initial_mapping: dict[str, str],
        preview_element_id: str,
        hidden_input_id: str,
    ) -> str:
        rows_html = "".join(
            f"""
            <div class=\"mapping-field\" data-field=\"{field_key}\" data-initial=\"{escape(initial_mapping.get(field_key, ''))}\">
                <label>{escape(label)}</label>
                <select>
                    <option value=\"\">Select column</option>
                    <option value=\"__custom__\">Custom value…</option>
                </select>
                <input type=\"text\" class=\"custom-value\" placeholder=\"Custom column or pipeline (e.g. First Name|Last Name)\" style=\"display:none;\">
                <small>{escape(help_text)}</small>
            </div>
            """
            for field_key, label, help_text in fields
        )
        return f"""
        <section id=\"{section_id}\" class=\"mapping-section\">
            <h3>{escape(title)}</h3>
            <p class=\"hint\">{escape(description)}</p>
            <div class=\"mapping-grid\">{rows_html}</div>
            <details class=\"mapping-preview\">
                <summary>Show {escape(title.lower())} JSON</summary>
                <pre id=\"{preview_element_id}\"></pre>
            </details>
            <input type=\"hidden\" name=\"{hidden_input_id}\" id=\"{hidden_input_id}\">
        </section>
        """

    players_section = _mapping_section(
        section_id="players-mapping",
        title="Players Columns",
        description="Match each required field to a column in your players CSV.",
        fields=PLAYERS_MAPPING_FIELDS,
        initial_mapping=players_mapping,
        preview_element_id="players-mapping-preview",
        hidden_input_id="players_mapping",
    )

    projection_section = _mapping_section(
        section_id="projections-mapping",
        title="Projections Columns",
        description="Match each required field to a column in your projections CSV.",
        fields=PROJECTION_MAPPING_FIELDS,
        initial_mapping=projection_mapping,
        preview_element_id="projection-mapping-preview",
        hidden_input_id="projection_mapping",
    )

    form_html = f"""
    <form method=\"post\" action=\"/ui\" enctype=\"multipart/form-data\">
        <label>Players CSV</label>
        <input type=\"file\" id=\"players-file\" name=\"players\" required>

        <label>Projections CSV</label>
        <input type=\"file\" id=\"projections-file\" name=\"projections\" required>

        {players_section}
        {projection_section}

        <label>Number of lineups</label>
        <input type=\"number\" name=\"lineups\" min=\"1\" value=\"{lineups_count}\">

        <div class=\"form-actions\">
            <button type=\"submit\" name=\"submit_action\" value=\"preview\">Preview</button>
            <button type=\"submit\" name=\"submit_action\" value=\"lineups\" class=\"secondary\">Build Lineups</button>
        </div>
    </form>
    """

    def _safe_js_object(data: dict[str, str]) -> str:
        json_text = json.dumps(data)
        return json_text.replace("</", "<\\/")

    initial_players_mapping_json = _safe_js_object(players_mapping)
    initial_projection_mapping_json = _safe_js_object(projection_mapping)

    mapping_script = f"""
    <script>
    (() => {{
        const playersInitial = {initial_players_mapping_json};
        const projectionsInitial = {initial_projection_mapping_json};

        function createMappingController(sectionId, hiddenInputId, previewId, initialMapping) {{
            const section = document.getElementById(sectionId);
            const hiddenInput = document.getElementById(hiddenInputId);
            const preview = document.getElementById(previewId);
            if (!section || !hiddenInput) {{
                return {{
                    setHeaders() {{}},
                }};
            }}

            const rows = Array.from(section.querySelectorAll('.mapping-field')).map((row) => {{
                const select = row.querySelector('select');
                const customInput = row.querySelector('.custom-value');
                return {{
                    key: row.dataset.field,
                    row,
                    select,
                    customInput,
                }};
            }});

            const state = {{ headers: [] }};

            function rebuildOptions(select, headers) {{
                select.innerHTML = '';
                const placeholder = document.createElement('option');
                placeholder.value = '';
                placeholder.textContent = 'Select column';
                select.appendChild(placeholder);
                headers.forEach((header) => {{
                    const option = document.createElement('option');
                    option.value = header;
                    option.textContent = header;
                    select.appendChild(option);
                }});
                const customOption = document.createElement('option');
                customOption.value = '__custom__';
                customOption.textContent = 'Custom value…';
                select.appendChild(customOption);
            }}

            function setFieldValue(field, value) {{
                if (!value) {{
                    field.select.value = '';
                    field.customInput.value = '';
                    field.customInput.style.display = 'none';
                    return;
                }}
                if (state.headers.includes(value)) {{
                    field.select.value = value;
                    field.customInput.value = '';
                    field.customInput.style.display = 'none';
                    return;
                }}
                field.select.value = '__custom__';
                field.customInput.value = value;
                field.customInput.style.display = 'block';
            }}

            function getFieldValue(field) {{
                if (field.select.value === '__custom__') {{
                    return field.customInput.value.trim();
                }}
                return field.select.value.trim();
            }}

            function updateHidden() {{
                const mapping = {{}};
                rows.forEach((field) => {{
                    const value = getFieldValue(field);
                    if (value) {{
                        mapping[field.key] = value;
                    }}
                }});
                const jsonValue = JSON.stringify(mapping, null, 2);
                hiddenInput.value = jsonValue;
                if (preview) {{
                    preview.textContent = jsonValue;
                }}
            }}

            rows.forEach((field) => {{
                rebuildOptions(field.select, state.headers);
                const initialValue = initialMapping[field.key] ?? '';
                setFieldValue(field, initialValue);
                field.select.addEventListener('change', () => {{
                    if (field.select.value === '__custom__') {{
                        field.customInput.style.display = 'block';
                        if (!field.customInput.value) {{
                            field.customInput.focus();
                        }}
                    }} else {{
                        field.customInput.style.display = 'none';
                        field.customInput.value = '';
                    }}
                    updateHidden();
                }});
                field.customInput.addEventListener('input', updateHidden);
            }});

            updateHidden();

            return {{
                setHeaders(headers) {{
                    state.headers = headers;
                    rows.forEach((field) => {{
                        const currentValue = getFieldValue(field) || (initialMapping[field.key] ?? '');
                        rebuildOptions(field.select, headers);
                        setFieldValue(field, currentValue);
                    }});
                    updateHidden();
                }},
            }};
        }}

        function splitCSV(line) {{
            const result = [];
            let current = '';
            let inQuotes = false;
            for (let i = 0; i < line.length; i += 1) {{
                const char = line[i];
                if (char === '"') {{
                    if (inQuotes && line[i + 1] === '"') {{
                        current += '"';
                        i += 1;
                    }} else {{
                        inQuotes = !inQuotes;
                    }}
                }} else if (char === ',' && !inQuotes) {{
                    result.push(current);
                    current = '';
                }} else {{
                    current += char;
                }}
            }}
            result.push(current);
            return result;
        }}

        function extractHeaders(contents) {{
            if (!contents) {{
                return [];
            }}
            const lines = contents.replace(/\\r\\n/g, '\\n').split('\\n');
            const firstLine = lines.find((line) => line.trim().length > 0);
            if (!firstLine) {{
                return [];
            }}
            return splitCSV(firstLine).map((header) => header.trim()).filter((header) => header.length > 0);
        }}

        function readHeadersFromInput(input, callback) {{
            if (!input || !input.files || input.files.length === 0) {{
                callback([]);
                return;
            }}
            const file = input.files[0];
            const reader = new FileReader();
            reader.onload = (event) => {{
                const readerTarget = event && event.target ? event.target : {{}};
                const contents = typeof readerTarget.result === 'string' ? readerTarget.result : '';
                const headers = extractHeaders(contents);
                callback(headers);
            }};
            reader.onerror = () => callback([]);
            reader.readAsText(file);
        }}

        const playersController = createMappingController(
            'players-mapping',
            'players_mapping',
            'players-mapping-preview',
            playersInitial,
        );
        const projectionsController = createMappingController(
            'projections-mapping',
            'projection_mapping',
            'projection-mapping-preview',
            projectionsInitial,
        );

        const playersInput = document.getElementById('players-file');
        const projectionsInput = document.getElementById('projections-file');

        if (playersInput) {{
            readHeadersFromInput(playersInput, (headers) => playersController.setHeaders(headers));
            playersInput.addEventListener('change', () => {{
                readHeadersFromInput(playersInput, (headers) => playersController.setHeaders(headers));
            }});
        }}
        if (projectionsInput) {{
            readHeadersFromInput(projectionsInput, (headers) => projectionsController.setHeaders(headers));
            projectionsInput.addEventListener('change', () => {{
                readHeadersFromInput(projectionsInput, (headers) => projectionsController.setHeaders(headers));
            }});
        }}
    }})();
    </script>
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

    body = flash_html + form_html + preview_html + result_html + runs_section + mapping_script
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
    default_players_mapping = DEFAULT_PLAYERS_MAPPING.copy()
    default_projection_mapping = DEFAULT_PROJECTION_MAPPING.copy()

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
            players_mapping=default_players_mapping.copy(),
            projection_mapping=default_projection_mapping.copy(),
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
        parsed_players_mapping = _parse_mapping(players_mapping) or DEFAULT_PLAYERS_MAPPING.copy()
        parsed_projection_mapping = _parse_mapping(projection_mapping) or DEFAULT_PROJECTION_MAPPING.copy()

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
            players_mapping=parsed_players_mapping,
            projection_mapping=parsed_projection_mapping,
            lineups_count=lineups,
        )
        return HTMLResponse(content)

    @app.get("/ui/runs/{run_id}", response_class=HTMLResponse)
    async def ui_run_detail(request: Request, run_id: str):
        run = _fetch_run_or_404(run_id)
        content = _render_run_detail_page(run)
        return HTMLResponse(content)


    return app
