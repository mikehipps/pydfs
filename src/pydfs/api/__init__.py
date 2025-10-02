"""REST API for the pydfs optimizer."""

from __future__ import annotations

import csv
import json
import tempfile
import statistics
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from html import escape
from io import StringIO
from pathlib import Path
from typing import Any, Iterable, Mapping, cast
from uuid import uuid4

import urllib.parse

from fastapi import FastAPI, File, Form, HTTPException, UploadFile, Request, Query
from fastapi.responses import HTMLResponse, Response, RedirectResponse

from dataclasses import asdict

from pydfs.api.schemas import (
    LineupBatchResponse,
    LineupPlayerResponse,
    LineupRequest,
    LineupResponse,
    MappingPreviewResponse,
    PlayerUsageResponse,
)
from pydfs.ingest import merge_player_and_projection_files
from pydfs.ingest.projections import MergeReport
from pydfs.models import PlayerRecord
from pydfs.optimizer import LineupGenerationPartial, build_lineups
from pydfs.persistence import RunJob, RunRecord, RunStore, SlateRecord


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

SITE_CHOICES: list[tuple[str, str]] = [
    ("FD", "FanDuel Classic"),
    ("FD_SINGLE", "FanDuel Single Game"),
    ("DK", "DraftKings Classic"),
    ("DK_CAPTAIN", "DraftKings Showdown"),
    ("YAHOO", "Yahoo"),
]

SPORT_CHOICES: list[tuple[str, str]] = [
    ("NFL", "NFL"),
    ("NBA", "NBA"),
    ("MLB", "MLB"),
    ("NHL", "NHL"),
    ("WNBA", "WNBA"),
]

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


def _calculate_player_usage(lineups: list[LineupResponse]) -> list[PlayerUsageResponse]:
    total_lineups = len(lineups)
    if total_lineups == 0:
        return []

    usage: dict[str, dict[str, Any]] = {}
    for lineup in lineups:
        for player in lineup.players:
            entry = usage.setdefault(
                player.player_id,
                {
                    "name": player.name,
                    "team": player.team,
                    "positions": tuple(player.positions),
                    "count": 0,
                },
            )
            entry["count"] = int(entry["count"]) + 1

    sorted_usage = sorted(
        usage.items(),
        key=lambda item: (-int(item[1]["count"]), str(item[1]["name"])),
    )
    return [
        PlayerUsageResponse(
            player_id=player_id,
            name=str(data["name"]),
            team=str(data["team"]),
            positions=list(data["positions"]),
            count=int(data["count"]),
            exposure=int(data["count"]) / total_lineups,
        )
        for player_id, data in sorted_usage
    ]


def _report_to_mapping(report: MappingPreviewResponse | MergeReport | dict) -> MappingPreviewResponse:
    if isinstance(report, MappingPreviewResponse):
        return report
    if isinstance(report, MergeReport):
        return MappingPreviewResponse(
            total_players=report.total_players,
            matched_players=report.matched_players,
            players_missing_projection=report.players_missing_projection,
            unmatched_projection_rows=report.unmatched_projection_rows,
        )
    if isinstance(report, dict):
        return MappingPreviewResponse.model_validate(report)
    raise TypeError("Unsupported report type")


def _analyze_lineups(
    lineups: list[LineupResponse],
    *,
    baseline_overrides: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    if baseline_overrides:
        lineups = [_apply_baseline_overrides(lineup, baseline_overrides) for lineup in lineups]
    usage = _calculate_player_usage(lineups)
    usage_lookup = {item.player_id: item.exposure for item in usage}

    lineup_groups: dict[tuple[str, ...], dict[str, Any]] = {}
    lineup_metrics: dict[tuple[str, ...], dict[str, float]] = {}
    baseline_scores: list[float] = []
    usage_sums: list[float] = []
    uniqueness_scores: list[float] = []
    unique_players: set[str] = set()

    for lineup in lineups:
        baseline_scores.append(lineup.baseline_projection)
        unique_players.update(player.player_id for player in lineup.players)
        signature = tuple(sorted(player.player_id for player in lineup.players))
        exposures = [usage_lookup.get(player.player_id, 0.0) for player in lineup.players]
        usage_sum = sum(exposures) * 100
        product = 1.0
        for exposure in exposures:
            product *= max(exposure, 1e-6)
        uniqueness = 1.0 / product
        usage_sums.append(usage_sum)
        uniqueness_scores.append(uniqueness)
        lineup_metrics[signature] = {
            "usage_sum": usage_sum,
            "uniqueness": uniqueness,
            "baseline": lineup.baseline_projection,
        }
        bucket = lineup_groups.setdefault(signature, {"lineup": lineup, "count": 0})
        bucket["count"] = int(bucket["count"]) + 1

    baseline_sorted = sorted(baseline_scores)
    usage_sorted = sorted(usage_sums)
    uniqueness_sorted = sorted(uniqueness_scores)

    for signature, data in lineup_metrics.items():
        data["baseline_percentile"] = _percentile(data["baseline"], baseline_sorted) if baseline_sorted else 0.0
        data["usage_percentile"] = _percentile(data["usage_sum"], usage_sorted, higher_is_better=False) if usage_sorted else 0.0
        data["uniqueness_percentile"] = _percentile(data["uniqueness"], uniqueness_sorted) if uniqueness_sorted else 0.0

    return {
        "usage": usage,
        "usage_lookup": usage_lookup,
        "lineup_groups": lineup_groups,
        "lineup_metrics": lineup_metrics,
        "baseline_scores": baseline_scores,
        "baseline_sorted": baseline_sorted,
        "usage_sums": usage_sums,
        "usage_sorted": usage_sorted,
        "uniqueness_scores": uniqueness_scores,
        "uniqueness_sorted": uniqueness_sorted,
        "unique_players": unique_players,
        "lineups": lineups,
    }


def _lineup_signature(players: Iterable[LineupPlayerResponse]) -> tuple[str, ...]:
    return tuple(sorted(player.player_id for player in players))


def _apply_baseline_overrides(
    lineup: LineupResponse,
    overrides: Mapping[str, float],
) -> LineupResponse:
    updated_players: list[LineupPlayerResponse] = []
    changed = False
    for player in lineup.players:
        new_baseline = float(overrides.get(player.player_id, 0.0))
        if abs(new_baseline - player.baseline_projection) > 1e-9:
            changed = True
            updated_players.append(
                player.model_copy(update={"baseline_projection": new_baseline})
            )
        else:
            updated_players.append(player)
    if not changed:
        return lineup
    new_baseline_total = sum(player.baseline_projection for player in updated_players)
    return lineup.model_copy(update={"players": updated_players, "baseline_projection": new_baseline_total})


def _normalize_lineup_dict(lineup_data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(lineup_data)
    normalized.setdefault("baseline_projection", normalized.get("projection", 0.0))
    players = []
    for player in normalized.get("players", []):
        player_copy = dict(player)
        player_copy.setdefault("baseline_projection", player_copy.get("projection", 0.0))
        players.append(player_copy)
    normalized["players"] = players
    return normalized


def _write_temp_from_bytes(contents: bytes) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        tmp.write(contents)
        tmp.flush()
    finally:
        tmp.close()
    return Path(tmp.name)


def _render_top_lineups(
    lineup_groups: dict[tuple[str, ...], dict[str, Any]],
    lineup_metrics: dict[tuple[str, ...], dict[str, float]],
    usage_lookup: dict[str, float],
    *,
    top_n: int = 100,
    descriptor: str = "baseline projection",
) -> tuple[str, int]:
    sorted_lineups = sorted(
        lineup_groups.values(),
        key=lambda data: (
            -cast(LineupResponse, data["lineup"]).baseline_projection,
            -int(data["count"]),
        ),
    )
    top_entries = sorted_lineups[:top_n]
    html_parts: list[str] = []
    for entry in top_entries:
        lineup = cast(LineupResponse, entry["lineup"])
        count = int(entry["count"])
        signature = _lineup_signature(lineup.players)
        metrics = lineup_metrics.get(
            signature,
            {
                "usage_sum": 0.0,
                "uniqueness": 0.0,
                "baseline": lineup.baseline_projection,
                "baseline_percentile": 0.0,
                "usage_percentile": 0.0,
                "uniqueness_percentile": 0.0,
            },
        )
        rows = "".join(
            f"<tr><td>{'/'.join(player.positions)}</td><td>{escape(player.name)}</td><td>{escape(player.team)}</td>"
            f"<td>{player.salary}</td><td>{player.baseline_projection:.2f}</td><td>{player.projection:.2f}</td><td>{usage_lookup.get(player.player_id, 0.0) * 100:.1f}%</td></tr>"
            for player in lineup.players
        )
        html_parts.append(
            f"<h3>{lineup.lineup_id} – Salary {lineup.salary} – Baseline {lineup.baseline_projection:.2f} ({metrics['baseline_percentile']:.0f}%) "
            f"(perturbed {lineup.projection:.2f}, x{count}) – Usage Sum {metrics['usage_sum']:.1f}% ({metrics['usage_percentile']:.0f}%) "
            f"– Uniqueness {_format_large(metrics['uniqueness'])} ({metrics['uniqueness_percentile']:.0f}%)</h3>"
            f"<table><thead><tr><th>Position</th><th>Player</th><th>Team</th><th>Salary</th><th>Baseline Projection</th><th>Perturbed Projection</th><th>Usage</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    lineups_html = "".join(html_parts)
    total_unique = len(sorted_lineups)
    if total_unique > len(top_entries):
        lineups_html += f"<p>Showing top {len(top_entries)} of {total_unique} unique lineups by {descriptor}.</p>"
    return lineups_html, total_unique


def job_to_dict(job: RunJob) -> dict:
    return {
        "run_id": job.run_id,
        "state": job.state,
        "site": job.site,
        "sport": job.sport,
        "message": job.message,
        "created_at": job.created_at.isoformat(),
        "updated_at": job.updated_at.isoformat(),
        "cancel_requested_at": job.cancel_requested_at.isoformat() if job.cancel_requested_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }


def run_record_to_dict(run: RunRecord, job: RunJob | None = None) -> dict:
    lineups = [LineupResponse.model_validate(_normalize_lineup_dict(lineup)) for lineup in run.lineups]
    usage = _calculate_player_usage(lineups)
    payload = {
        "run_id": run.run_id,
        "created_at": run.created_at.isoformat(),
        "site": run.site,
        "sport": run.sport,
        "request": run.request,
        "report": run.report,
        "lineups": [lineup.model_dump() for lineup in lineups],
        "player_usage": [item.model_dump() for item in usage],
        "players_mapping": run.players_mapping,
        "projection_mapping": run.projection_mapping,
    }
    if job:
        payload["state"] = job.state
        payload["job"] = job_to_dict(job)
    else:
        payload["state"] = "completed"
        payload["job"] = None
    return payload


def _format_large(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value:,.1f}"
    return f"{value:.1f}"


def _percentile(value: float, sorted_values: list[float], *, higher_is_better: bool = True) -> float:
    if not sorted_values:
        return 0.0
    n = len(sorted_values)
    if higher_is_better:
        idx = bisect_right(sorted_values, value)
        return 100.0 * idx / n
    idx = bisect_left(sorted_values, value)
    return 100.0 * (n - idx) / n


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
        .pool-filter {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-top: 0.75rem; }}
        .pool-filter label {{ display: flex; flex-direction: column; font-weight: 600; }}
        .pool-filter select, .pool-filter input {{ margin-top: 0.35rem; padding: 0.4rem; border-radius: 6px; border: 1px solid #cbd5e1; }}
        .pool-filter label.checkbox {{ flex-direction: row; align-items: center; gap: 0.5rem; font-weight: 600; }}
        .pool-filter label.checkbox input {{ margin-top: 0; }}
        .slate-info {{ margin-bottom: 1.5rem; padding: 1rem; border: 1px solid #e2e8f0; border-radius: 8px; background: #f8fafc; }}
        .slate-info ul {{ list-style: none; padding: 0; margin: 0.5rem 0 0; }}
        .slate-info li {{ margin-bottom: 0.25rem; }}
        .slate-info form {{ margin-top: 0.75rem; display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: center; }}
        .slate-info form label {{ font-weight: 600; }}
        .slate-info form input[type=\"file\"] {{ flex: 1 1 260px; }}
        .notice {{ margin: 0.5rem 0; padding: 0.75rem 1rem; border-radius: 6px; }}
        .notice.success {{ background: #ecfdf5; color: #065f46; border: 1px solid #a7f3d0; }}
        .notice.error {{ background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }}
    </style>
</head>
<body>
    <nav><a href=\"/ui\">Home</a></nav>
    <main>{body}</main>
</body>
</html>"""


def _coerce_percentage_value(value: float | None) -> float | None:
    if value is None:
        return None
    if value < 0:
        return 0.0
    return value * 100.0 if value <= 1.0 else value


def _apply_bias_to_player_records(records: list[PlayerRecord], bias_map: Mapping[str, float]) -> list[PlayerRecord]:
    if not bias_map:
        return list(records)
    biased: list[PlayerRecord] = []
    for record in records:
        factor = float(bias_map.get(record.player_id, 1.0))
        factor = max(0.0, factor)
        new_projection = max(0.0, record.projection * factor)
        metadata = dict(record.metadata)
        metadata.setdefault("baseline_projection", float(metadata.get("baseline_projection", record.projection)))
        metadata["bias_factor"] = factor
        metadata["biased_projection"] = new_projection
        biased.append(record.model_copy(update={"projection": new_projection, "metadata": metadata}))
    return biased


def _render_index_page(
    runs: list[RunRecord],
    preview,
    result,
    error: str | None,
    success: str | None,
    players_mapping: dict[str, str],
    projection_mapping: dict[str, str],
    lineups_count: int,
    parallel_jobs: int,
    perturbation_p25_value: float,
    perturbation_p75_value: float,
    exposure_bias_value: float,
    exposure_bias_target_value: float,
    max_exposure_value: float,
    lineups_per_job_value: int | None,
    max_repeating_players_value: int | None,
    site_value: str,
    sport_value: str,
    min_salary_value: int | None,
    slates: list[dict[str, Any]],
    selected_slate_id: str | None,
    slate_name_value: str | None,
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

    site_options_html = "".join(
        f'<option value="{escape(site)}"{" selected" if site == site_value else ""}>{escape(label)}</option>'
        for site, label in SITE_CHOICES
    )
    sport_options_html = "".join(
        f'<option value="{escape(sport)}"{" selected" if sport == sport_value else ""}>{escape(label)}</option>'
        for sport, label in SPORT_CHOICES
    )

    current_slate = slates[0] if slates else None
    slate_options_html = "<option value=\"\">Upload new slate</option>" + "".join(
        f"<option value=\"{escape(str(slate['slate_id']))}\"{' selected' if str(slate['slate_id']) == (selected_slate_id or '') else ''}>{escape(slate['display'])}</option>"
        for slate in slates
    )

    form_html = f"""
    <form method=\"post\" action=\"/ui\" enctype=\"multipart/form-data\">
        <label>Stored Slate (optional)</label>
        <select name=\"slate_id\">{slate_options_html}</select>
        <small>Select a saved slate or leave blank to upload new files.</small>

        <label>Slate name</label>
        <input type=\"text\" name=\"slate_name\" value=\"{escape(slate_name_value or '')}\" placeholder=\"e.g. FD NFL Main\">

        <label>Players CSV</label>
        <input type=\"file\" id=\"players-file\" name=\"players\" accept=\".csv,text/csv\">
        <small>Optional when using a stored slate.</small>

        <label>Projections CSV</label>
        <input type=\"file\" id=\"projections-file\" name=\"projections\" accept=\".csv,text/csv\">
        <small>Optional when using a stored slate.</small>

        {players_section}
        {projection_section}

        <label>Number of lineups</label>
        <input type=\"number\" name=\"lineups\" min=\"1\" value=\"{lineups_count}\">

        <label>Site</label>
        <select name=\"site\">{site_options_html}</select>

        <label>Sport</label>
        <select name=\"sport\">{sport_options_html}</select>

        <label>Parallel workers</label>
        <input type=\"number\" name=\"parallel_jobs\" min=\"1\" max=\"16\" value=\"{parallel_jobs}\">

        <label>Perturbation at 25th percentile (%)</label>
        <input type=\"number\" name=\"perturbation_p25\" min=\"0\" max=\"100\" step=\"1\" value=\"{perturbation_p25_value:.1f}\">

        <label>Perturbation at 75th percentile (%)</label>
        <input type=\"number\" name=\"perturbation_p75\" min=\"0\" max=\"100\" step=\"1\" value=\"{perturbation_p75_value:.1f}\">

        <label>Exposure bias (%)</label>
        <input type=\"number\" name=\"exposure_bias\" min=\"0\" max=\"100\" step=\"1\" value=\"{exposure_bias_value:.1f}\">

        <label>Bias target exposure (%)</label>
        <input type=\"number\" name=\"exposure_bias_target\" min=\"0\" max=\"100\" step=\"1\" value=\"{exposure_bias_target_value:.1f}\">

        <label>Max player exposure (0-1)</label>
        <input type=\"number\" name=\"max_exposure\" min=\"0\" max=\"1\" step=\"0.05\" value=\"{max_exposure_value:.2f}\">

        <label>Lineups per job (blank for auto)</label>
        <input type=\"number\" name=\"lineups_per_job\" min=\"1\" max=\"1000\" value=\"{'' if lineups_per_job_value is None else lineups_per_job_value}\">

        <label>Max repeating players (blank for default)</label>
        <input type=\"number\" name=\"max_repeating_players\" min=\"0\" max=\"8\" value=\"{'' if max_repeating_players_value is None else max_repeating_players_value}\">

        <label>Minimum salary (blank to use site default)</label>
        <input type=\"number\" name=\"min_salary\" min=\"0\" step=\"100\" value=\"{'' if min_salary_value is None else min_salary_value}\">

        <div class=\"form-actions\">
            <button type=\"submit\" name=\"submit_action\" value=\"preview\">Preview</button>
            <button type=\"submit\" name=\"submit_action\" value=\"lineups\" class=\"secondary\">Build Lineups</button>
        </div>
    </form>
    """

    slate_info_html = ""
    if current_slate:
        slate_info_html = f"""
        <section class=\"slate-info\">
            <h2>Current Slate</h2>
            <p><strong>{escape(current_slate['display'])}</strong></p>
            <ul>
                <li>Players file: {escape(current_slate['players_filename'])}</li>
                <li>Projections file: {escape(current_slate['projections_filename'])}</li>
                <li>Uploaded: {current_slate['created_at'].astimezone().strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>
        </section>
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
        top_lineup = result["lineup"]
        min_salary_display = result.get("min_salary")
        min_salary_text = "" if not min_salary_display else f" – Min Salary {min_salary_display}"
        usage_lookup = result.get("usage_lookup") or {
            item.player_id: item.exposure for item in _calculate_player_usage([top_lineup])
        }
        lineup_usage_sum = sum(usage_lookup.get(player.player_id, 0.0) for player in top_lineup.players) * 100
        usage_product = 1.0
        for player in top_lineup.players:
            usage_product *= max(usage_lookup.get(player.player_id, 0.0), 1e-6)
        uniqueness_score = 1.0 / usage_product
        uniqueness_text = _format_large(uniqueness_score)
        lineup_rows = "".join(
            f"<tr><td>{'/'.join(player.positions)}</td>"
            f"<td>{escape(player.name)}</td>"
            f"<td>{escape(player.team)}</td>"
            f"<td>{player.salary}</td>"
            f"<td>{player.baseline_projection:.2f}</td>"
            f"<td>{player.projection:.2f}</td>"
            f"<td>{usage_lookup.get(player.player_id, 0.0) * 100:.1f}%</td></tr>"
            for player in top_lineup.players
        )
        result_html = f"""
        <section>
            <h2>Run Saved</h2>
            <p>Run ID: <a href=\"/ui/runs/{result['run_id']}\">{result['run_id']}</a></p>
            <h3>Top Lineup – Baseline {top_lineup.baseline_projection:.2f} (perturbed {top_lineup.projection:.2f}){min_salary_text} – Usage Sum {lineup_usage_sum:.1f}% – Uniqueness {uniqueness_text}</h3>
            <table>
                <thead><tr><th>Position</th><th>Player</th><th>Team</th><th>Salary</th><th>Baseline Projection</th><th>Perturbed Projection</th><th>Usage</th></tr></thead>
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
        <p><a href=\"/ui/pool\">View combined lineup pool</a></p>
        <ul>{runs_html}</ul>
    </section>
    """

    body = flash_html + slate_info_html + form_html + preview_html + result_html + runs_section + mapping_script
    return _render_page(body)


def _run_to_csv(run: RunRecord) -> str:
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "run_id", "lineup_id", "slot", "player_id", "name", "team", "positions", "salary", "projection", "baseline_projection", "ownership"
    ])
    for lineup_data in run.lineups:
        lineup = LineupResponse.model_validate(_normalize_lineup_dict(lineup_data))
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
                f"{player.baseline_projection:.4f}",
                "" if player.ownership is None else f"{player.ownership:.2f}",
            ])
    return buffer.getvalue()



def _render_run_detail_page(run: RunRecord) -> str:
    report = run.report
    lineups = [LineupResponse.model_validate(_normalize_lineup_dict(lineup_data)) for lineup_data in run.lineups]
    analysis = _analyze_lineups(lineups)
    usage = analysis["usage"]
    usage_lookup = analysis["usage_lookup"]
    lineup_metrics = analysis["lineup_metrics"]
    lineup_groups = analysis["lineup_groups"]
    baseline_scores = analysis["baseline_scores"]
    lineup_usage_sums = analysis["usage_sums"]
    lineup_uniqueness_scores = analysis["uniqueness_scores"]
    unique_players_used = analysis["unique_players"]

    if baseline_scores:
        mean_score = statistics.fmean(baseline_scores)
        median_score = statistics.median(baseline_scores)
        std_score = statistics.pstdev(baseline_scores) if len(baseline_scores) > 1 else 0.0
        sorted_scores = sorted(baseline_scores, reverse=True)
        top10_count = max(1, int(len(sorted_scores) * 0.10))
        top1_count = max(1, int(len(sorted_scores) * 0.01))
        top10_avg = sum(sorted_scores[:top10_count]) / top10_count
        top1_avg = sum(sorted_scores[:top1_count]) / top1_count
    else:
        mean_score = median_score = std_score = top10_avg = top1_avg = 0.0
        top10_count = top1_count = 0

    if lineup_usage_sums:
        usage_mean = statistics.fmean(lineup_usage_sums)
        usage_median = statistics.median(lineup_usage_sums)
        usage_std = statistics.pstdev(lineup_usage_sums) if len(lineup_usage_sums) > 1 else 0.0
    else:
        usage_mean = usage_median = usage_std = 0.0

    if lineup_uniqueness_scores:
        uniqueness_mean = statistics.fmean(lineup_uniqueness_scores)
        uniqueness_median = statistics.median(lineup_uniqueness_scores)
        uniqueness_std = statistics.pstdev(lineup_uniqueness_scores) if len(lineup_uniqueness_scores) > 1 else 0.0
    else:
        uniqueness_mean = uniqueness_median = uniqueness_std = 0.0

    def _lineup_signature(players: list[LineupPlayerResponse]) -> tuple[str, ...]:
        return tuple(sorted(player.player_id for player in players))

    lineups_html, total_unique = _render_top_lineups(
        lineup_groups,
        lineup_metrics,
        usage_lookup,
        top_n=100,
        descriptor="baseline projection",
    )

    if baseline_scores:
        stats_html = f"""
        <section>
            <h2>Lineup Summary</h2>
            <table>
                <tr><th>Total lineups</th><td>{len(lineups)}</td></tr>
                <tr><th>Unique players used</th><td>{len(unique_players_used)}</td></tr>
                <tr><th>Mean baseline projection</th><td>{mean_score:.2f}</td></tr>
                <tr><th>Median baseline projection</th><td>{median_score:.2f}</td></tr>
                <tr><th>Std. dev.</th><td>{std_score:.2f}</td></tr>
                <tr><th>Top 10% average ({top10_count} lineups)</th><td>{top10_avg:.2f}</td></tr>
                <tr><th>Top 1% average ({max(top1_count, 1)} lineup{'s' if top1_count != 1 else ''})</th><td>{top1_avg:.2f}</td></tr>
                <tr><th>Mean usage sum</th><td>{usage_mean:.1f}%</td></tr>
                <tr><th>Median usage sum</th><td>{usage_median:.1f}%</td></tr>
                <tr><th>Usage std. dev.</th><td>{usage_std:.1f}</td></tr>
                <tr><th>Mean uniqueness</th><td>{_format_large(uniqueness_mean)}</td></tr>
                <tr><th>Median uniqueness</th><td>{_format_large(uniqueness_median)}</td></tr>
                <tr><th>Uniqueness std. dev.</th><td>{_format_large(uniqueness_std)}</td></tr>
            </table>
        </section>
        """
    else:
        stats_html = ""

    usage_rows = "".join(
        f"<tr><td>{escape(item.name)}</td><td>{escape(item.team)}</td><td>{'/'.join(item.positions)}</td>"
        f"<td>{item.count}</td><td>{item.exposure * 100:.1f}%</td></tr>"
        for item in usage
    ) or "<tr><td colspan=5>No lineups generated.</td></tr>"
    usage_table = f"""
    <section>
        <h2>Player Usage</h2>
        <table>
            <thead><tr><th>Player</th><th>Team</th><th>Positions</th><th>Lineups</th><th>Usage</th></tr></thead>
            <tbody>{usage_rows}</tbody>
        </table>
    </section>
    """

    max_exposure_display = run.request.get("max_exposure")
    max_repeating_display = run.request.get("max_repeating_players")
    min_salary_display = run.request.get("min_salary")
    if isinstance(max_exposure_display, (int, float)):
        max_exposure_display = f"{max_exposure_display:.2f}"
    else:
        max_exposure_display = "-"
    max_repeating_display = "-" if max_repeating_display is None else str(max_repeating_display)
    min_salary_display = "-" if min_salary_display in (None, "") else str(min_salary_display)

    bias_summary_data = run.request.get("bias_summary") if isinstance(run.request, dict) else None
    bias_section = ""
    slate_id_for_reset = run.request.get("slate_id") if isinstance(run.request, dict) else None
    if bias_summary_data:
        try:
            min_factor = float(bias_summary_data.get("min_factor", 1.0))
            max_factor = float(bias_summary_data.get("max_factor", 1.0))
            target_percent = bias_summary_data.get("target_percent")
            strength_percent = bias_summary_data.get("strength_percent")
            lineups_tracked = bias_summary_data.get("lineups_tracked", 0)
            factor_count = len(bias_summary_data.get("factors", {}))
        except Exception:  # pragma: no cover - defensive parsing
            min_factor = max_factor = 1.0
            target_percent = strength_percent = None
            lineups_tracked = factor_count = 0
        reset_form = ""
        if slate_id_for_reset:
            reset_form = f"""
            <form method=\"post\" action=\"/slates/{escape(slate_id_for_reset)}/reset-bias\" style=\"margin-top:0.75rem;\">
                <input type=\"hidden\" name=\"redirect\" value=\"/ui/runs/{run.run_id}\">
                <button type=\"submit\" class=\"secondary\">Remove accumulated bias</button>
            </form>
            """
        target_display = "-" if target_percent is None else f"{target_percent:.1f}"
        strength_display = "-" if strength_percent is None else f"{strength_percent:.1f}"
        bias_section = f"""
        <section>
            <h2>Bias Summary</h2>
            <table>
                <tr><th>Min factor</th><td>{min_factor:.3f}</td></tr>
                <tr><th>Max factor</th><td>{max_factor:.3f}</td></tr>
                <tr><th>Target exposure (%)</th><td>{target_display}</td></tr>
                <tr><th>Bias strength (%)</th><td>{strength_display}</td></tr>
                <tr><th>Lineups tracked</th><td>{lineups_tracked}</td></tr>
                <tr><th>Players tracked</th><td>{factor_count}</td></tr>
            </table>
            {reset_form}
        </section>
        """

    body = f"""
    <section>
        <h1>Run {run.run_id}</h1>
        <p><strong>Created:</strong> {run.created_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Site/Sport:</strong> {run.site} {run.sport}</p>
        <p><strong>Max Exposure:</strong> {max_exposure_display}</p>
        <p><strong>Max Repeating Players:</strong> {max_repeating_display}</p>
        <p><strong>Min Salary:</strong> {min_salary_display}</p>

        <h2>Merge Report</h2>
        <table>
            <tr><th>Total Players</th><td>{report['total_players']}</td></tr>
            <tr><th>Matched Players</th><td>{report['matched_players']}</td></tr>
            <tr><th>Missing Projections</th><td>{len(report['players_missing_projection'])}</td></tr>
            <tr><th>Unmatched Projections</th><td>{len(report['unmatched_projection_rows'])}</td></tr>
        </table>
    </section>
    {stats_html}
    {bias_section}
    {usage_table}
    <section>
        <h2>Lineups</h2>
        {lineups_html}
    </section>
    <p><a href=\"/ui\">Back to runs</a></p>
    """
    return _render_page(body)


def _render_lineup_pool_page(
    runs: list[RunRecord],
    *,
    slates: list[SlateRecord],
    selected_slate: SlateRecord | None,
    slate_filter: str | None,
    site_filter: str | None,
    sport_filter: str | None,
    limit: int,
    all_dates: bool,
    today: Any,
    notice: str | None,
    error: str | None,
) -> str:
    selected_slate_id = selected_slate.slate_id if selected_slate else (slate_filter or None)

    if selected_slate and not site_filter:
        site_filter = selected_slate.site
    if selected_slate and not sport_filter:
        sport_filter = selected_slate.sport

    filtered_runs: list[RunRecord] = []
    for run in runs:
        run_slate_id = run.request.get("slate_id") if isinstance(run.request, dict) else None
        if selected_slate_id:
            if run_slate_id != selected_slate_id:
                continue
        else:
            if site_filter is not None and run.site != site_filter:
                continue
            if sport_filter is not None and run.sport != sport_filter:
                continue
            run_date = run.created_at.astimezone().date()
            if not all_dates and run_date != today:
                continue
        if selected_slate_id and not all_dates:
            run_date = run.created_at.astimezone().date()
            if run_date != today:
                continue
        filtered_runs.append(run)
    filtered_runs = filtered_runs[:limit]

    total_runs = len(filtered_runs)
    recent_label = filtered_runs[0].created_at.astimezone().strftime('%Y-%m-%d %H:%M:%S') if filtered_runs else "-"
    latest_run_id = filtered_runs[0].run_id if filtered_runs else "-"

    all_lineups: list[LineupResponse] = []
    baseline_lookup: dict[str, float] = {}
    if selected_slate:
        for record in selected_slate.records:
            player_id = record.get("player_id")
            if not player_id:
                continue
            baseline_lookup[player_id] = float(record.get("projection", 0.0))
    elif filtered_runs:
        latest_run = filtered_runs[0]
        latest_lineups = [
            LineupResponse.model_validate(_normalize_lineup_dict(lineup_data)) for lineup_data in latest_run.lineups
        ]
        for lineup in latest_lineups:
            for player in lineup.players:
                baseline_lookup[player.player_id] = player.baseline_projection

    for run in filtered_runs:
        for lineup_data in run.lineups:
            all_lineups.append(LineupResponse.model_validate(_normalize_lineup_dict(lineup_data)))

    analysis = _analyze_lineups(all_lineups, baseline_overrides=baseline_lookup)
    usage = analysis["usage"]
    usage_lookup = analysis["usage_lookup"]
    lineup_groups = analysis["lineup_groups"]
    lineup_metrics = analysis["lineup_metrics"]
    baseline_scores = analysis["baseline_scores"]
    lineup_usage_sums = analysis["usage_sums"]
    lineup_uniqueness_scores = analysis["uniqueness_scores"]
    unique_players_used = analysis["unique_players"]

    total_lineups = len(all_lineups)
    unique_lineups = len(lineup_groups)

    if baseline_scores:
        mean_score = statistics.fmean(baseline_scores)
        median_score = statistics.median(baseline_scores)
        std_score = statistics.pstdev(baseline_scores) if len(baseline_scores) > 1 else 0.0
        sorted_scores = sorted(baseline_scores, reverse=True)
        top10_count = max(1, int(len(sorted_scores) * 0.10))
        top1_count = max(1, int(len(sorted_scores) * 0.01))
        top10_avg = sum(sorted_scores[:top10_count]) / top10_count
        top1_avg = sum(sorted_scores[:top1_count]) / top1_count
    else:
        mean_score = median_score = std_score = top10_avg = top1_avg = 0.0
        top10_count = top1_count = 0

    if lineup_usage_sums:
        usage_mean = statistics.fmean(lineup_usage_sums)
        usage_median = statistics.median(lineup_usage_sums)
        usage_std = statistics.pstdev(lineup_usage_sums) if len(lineup_usage_sums) > 1 else 0.0
    else:
        usage_mean = usage_median = usage_std = 0.0

    if lineup_uniqueness_scores:
        uniqueness_mean = statistics.fmean(lineup_uniqueness_scores)
        uniqueness_median = statistics.median(lineup_uniqueness_scores)
        uniqueness_std = statistics.pstdev(lineup_uniqueness_scores) if len(lineup_uniqueness_scores) > 1 else 0.0
    else:
        uniqueness_mean = uniqueness_median = uniqueness_std = 0.0

    lineups_html, _ = _render_top_lineups(
        lineup_groups,
        lineup_metrics,
        usage_lookup,
        top_n=200,
        descriptor="baseline projection",
    )

    usage_rows = "".join(
        f"<tr><td>{escape(item.name)}</td><td>{escape(item.team)}</td><td>{'/'.join(item.positions)}</td><td>{item.count}</td><td>{item.exposure * 100:.1f}%</td></tr>"
        for item in usage
    ) or "<tr><td colspan=5>No lineups available.</td></tr>"

    usage_table = f"""
    <section>
        <h2>Player Usage (All Runs)</h2>
        <table>
            <thead><tr><th>Player</th><th>Team</th><th>Positions</th><th>Lineups</th><th>Usage</th></tr></thead>
            <tbody>{usage_rows}</tbody>
        </table>
    </section>
    """

    display_top1_count = max(top1_count, 1)
    summary_rows = "".join(
        [
            f"<tr><th>Runs included</th><td>{total_runs}</td></tr>",
            f"<tr><th>Total lineups</th><td>{total_lineups}</td></tr>",
            f"<tr><th>Unique lineups</th><td>{unique_lineups}</td></tr>",
            f"<tr><th>Unique players</th><td>{len(unique_players_used)}</td></tr>",
            f"<tr><th>Mean baseline projection</th><td>{mean_score:.2f}</td></tr>",
            f"<tr><th>Median baseline projection</th><td>{median_score:.2f}</td></tr>",
            f"<tr><th>Baseline std. dev.</th><td>{std_score:.2f}</td></tr>",
            f"<tr><th>Top 10% average ({top10_count} lineups)</th><td>{top10_avg:.2f}</td></tr>",
            f"<tr><th>Top 1% average ({display_top1_count} lineup{'s' if display_top1_count != 1 else ''})</th><td>{top1_avg:.2f}</td></tr>",
            f"<tr><th>Mean usage sum</th><td>{usage_mean:.1f}%</td></tr>",
            f"<tr><th>Median usage sum</th><td>{usage_median:.1f}%</td></tr>",
            f"<tr><th>Usage std. dev.</th><td>{usage_std:.1f}%</td></tr>",
            f"<tr><th>Mean uniqueness</th><td>{_format_large(uniqueness_mean)}</td></tr>",
            f"<tr><th>Median uniqueness</th><td>{_format_large(uniqueness_median)}</td></tr>",
            f"<tr><th>Uniqueness std. dev.</th><td>{_format_large(uniqueness_std)}</td></tr>",
        ]
    )

    range_note = "All dates" if all_dates else "Today"
    if selected_slate:
        range_note += f" • Slate: {escape(selected_slate.name or 'Unnamed')}"
    summary_section = f"""
    <section>
        <h2>Lineup Pool Summary</h2>
        <p>Range: {range_note}</p>
        <table>
            {summary_rows}
        </table>
    </section>
    """

    runs_list_items = []
    for run in filtered_runs:
        run_slate_name = run.request.get("slate_name") if isinstance(run.request, dict) else None
        run_slate_id = run.request.get("slate_id") if isinstance(run.request, dict) else None
        slate_descriptor = ""
        if run_slate_name:
            slate_descriptor = f" – {escape(str(run_slate_name))}"
        elif run_slate_id:
            slate_descriptor = f" – {escape(str(run_slate_id))}"
        runs_list_items.append(
            f"<li><a href=\"/ui/runs/{run.run_id}\">{run.created_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}</a> – {run.run_id} ({run.site} {run.sport}, {len(run.lineups)} lineups{slate_descriptor})</li>"
        )
    runs_list = "".join(runs_list_items) or "<p>No runs match the current filters.</p>"

    runs_section = f"""
    <section>
        <h2>Runs in Pool</h2>
        <ol>{runs_list}</ol>
    </section>
    """

    slate_options_html = ["<option value=\"\">All slates</option>"]
    for slate in slates:
        selected_attr = " selected" if slate.slate_id == selected_slate_id else ""
        label = f"{slate.site} {slate.sport} – {slate.name or 'Unnamed'}"
        slate_options_html.append(f"<option value=\"{escape(slate.slate_id)}\"{selected_attr}>{escape(label)}</option>")
    slate_options = "".join(slate_options_html)

    site_options = "".join(
        f"<option value=\"{code}\"{' selected' if site_filter == code else ''}>{label}</option>"
        for code, label in SITE_CHOICES
    )
    site_options = "<option value=\"\">All sites</option>" + site_options

    sport_options = "".join(
        f"<option value=\"{code}\"{' selected' if sport_filter == code else ''}>{label}</option>"
        for code, label in SPORT_CHOICES
    )
    sport_options = "<option value=\"\">All sports</option>" + sport_options

    all_dates_checked = " checked" if all_dates else ""
    site_disabled = " disabled" if selected_slate_id else ""
    sport_disabled = " disabled" if selected_slate_id else ""
    hidden_site = (
        f"<input type=\"hidden\" name=\"site\" value=\"{escape(site_filter)}\">" if selected_slate_id and site_filter else ""
    )
    hidden_sport = (
        f"<input type=\"hidden\" name=\"sport\" value=\"{escape(sport_filter)}\">" if selected_slate_id and sport_filter else ""
    )

    notice_html = f"<p class=\"notice success\">{escape(notice)}</p>" if notice else ""
    error_html = f"<p class=\"notice error\">{escape(error)}</p>" if error else ""

    filter_form = f"""
    <section>
        <h1>Lineup Pool</h1>
        <p>Latest run: {recent_label} (ID {latest_run_id})</p>
        {notice_html}{error_html}
        <form method=\"get\" class=\"pool-filter\">
            <label>Slate<select name=\"slate_id\">{slate_options}</select></label>
            <label>Site<select name=\"site\"{site_disabled}>{site_options}</select></label>
            <label>Sport<select name=\"sport\"{sport_disabled}>{sport_options}</select></label>
            <label>Run history depth<input type=\"number\" name=\"limit\" min=\"1\" max=\"500\" value=\"{limit}\"></label>
            <label class=\"checkbox\"><input type=\"checkbox\" name=\"all_dates\" value=\"true\"{all_dates_checked}>Include previous days</label>
            <button type=\"submit\">Apply</button>
            {hidden_site}
            {hidden_sport}
        </form>
    </section>
    """

    slate_info_html = ""
    if selected_slate:
        slate_info_html = f"""
        <section class=\"slate-info\">
            <h2>Current Slate</h2>
            <ul>
                <li><strong>Name:</strong> {escape(selected_slate.name or 'Unnamed')}</li>
                <li><strong>Site/Sport:</strong> {escape(selected_slate.site)} {escape(selected_slate.sport)}</li>
                <li><strong>Players CSV:</strong> {escape(selected_slate.players_filename)}</li>
                <li><strong>Projections CSV:</strong> {escape(selected_slate.projections_filename)}</li>
                <li><strong>Last updated:</strong> {selected_slate.updated_at.astimezone().strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>
            <form method=\"post\" action=\"/ui/pool/{escape(selected_slate.slate_id)}/update\" enctype=\"multipart/form-data\">
                <label>Replace projections CSV<input type=\"file\" name=\"projections\" accept=\".csv,text/csv\" required></label>
                <button type=\"submit\">Update projections</button>
            </form>
        </section>
        """

    if total_lineups == 0:
        body = filter_form + slate_info_html + "<p>No lineups have been generated yet for the selected filters.</p>" + runs_section
        return _render_page(body)

    body = filter_form + slate_info_html + summary_section + usage_table + runs_section + f"<section><h2>Top Lineups</h2>{lineups_html}</section>"
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


async def _write_temp(upload: UploadFile | None) -> tuple[Path | None, bytes | None]:
    if upload is None:
        return None, None
    contents = await upload.read()
    if not contents:
        return None, None
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        tmp.write(contents)
        tmp.flush()
    finally:
        tmp.close()
    return Path(tmp.name), contents


def create_app() -> FastAPI:
    app = FastAPI(title="pydfs optimizer")
    store = RunStore(Path(__file__).resolve().parent.parent / "pydfs.sqlite")
    app.state.run_store = store
    default_players_mapping = DEFAULT_PLAYERS_MAPPING.copy()
    default_projection_mapping = DEFAULT_PROJECTION_MAPPING.copy()

    def slate_to_summary(slate: SlateRecord) -> dict[str, Any]:
        return {
            "slate_id": slate.slate_id,
            "site": slate.site,
            "sport": slate.sport,
            "name": slate.name or f"{slate.site} {slate.sport}",
            "players_filename": slate.players_filename,
            "projections_filename": slate.projections_filename,
            "created_at": slate.created_at,
            "updated_at": slate.updated_at,
            "display": f"{slate.site} {slate.sport} – {slate.name or 'Unnamed'} ({slate.created_at.astimezone().strftime('%Y-%m-%d %H:%M')})",
        }

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/preview", response_model=MappingPreviewResponse)
    async def preview(
        projections: UploadFile = File(...),
        players: UploadFile | None = File(None),
        projection_mapping: str | None = Form(None),
        players_mapping: str | None = Form(None),
        site: str = Form("FD"),
        sport: str = Form("NFL"),
    ) -> MappingPreviewResponse:
        proj_path, projections_bytes = await _write_temp(projections)
        players_path, players_bytes = await _write_temp(players)
        if proj_path is None:
            raise HTTPException(status_code=400, detail="projections file is empty")

        try:
            if players_path:
                _, report = merge_player_and_projection_files(
                    players_path=players_path,
                    projections_path=proj_path,
                    site=site,
                    sport=sport,
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=_parse_mapping(projection_mapping) or None,
                )
            else:
                _, report = merge_player_and_projection_files(
                    players_path=proj_path,
                    projections_path=None,
                    site=site,
                    sport=sport,
                    players_mapping=_parse_mapping(players_mapping) or None,
                    projection_mapping=None,
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            proj_path.unlink(missing_ok=True)
            if players_path:
                players_path.unlink(missing_ok=True)

        if 'lineups_payload' in locals():
            store.save_run(
                run_id=run_id,
                site=site,
                sport=sport,
                request={
                    "lineups": lineups,
                    "max_repeating_players": max_repeating_players,
                    "max_exposure": max_exposure,
                    "lineups_per_job": lineups_per_job,
                    "site": site,
                    "sport": sport,
                    "min_salary": min_salary,
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
            if partial_message:
                error = partial_message
                success = None

        return MappingPreviewResponse(
            total_players=report.total_players,
            matched_players=report.matched_players,
            players_missing_projection=report.players_missing_projection,
            unmatched_projection_rows=report.unmatched_projection_rows,
        )

    @app.post("/lineups", response_model=LineupBatchResponse)
    async def build(
        projections: UploadFile | None = File(None),
        players: UploadFile | None = File(None),
        lineup_request: str = Form("{}"),
        projection_mapping: str | None = Form(None),
        players_mapping: str | None = Form(None),
        parallel_jobs: int = Form(1),
        perturbation: float = Form(0.0),
        perturbation_p25_form: float | None = Form(None, alias="perturbation_p25"),
        perturbation_p75_form: float | None = Form(None, alias="perturbation_p75"),
        exposure_bias_form: float | None = Form(None, alias="exposure_bias"),
        exposure_bias_target_form: float | None = Form(None, alias="exposure_bias_target"),
        max_exposure: float = Form(0.5),
        lineups_per_job: int | None = Form(None),
        max_repeating_players_form: int | None = Form(None),
        site_form: str = Form("FD"),
        sport_form: str = Form("NFL"),
        min_salary_form: int | None = Form(None),
        slate_id_form: str | None = Form(None, alias="slate_id"),
        slate_name_form: str | None = Form(None, alias="slate_name"),
    ) -> LineupBatchResponse:
        try:
            raw_request = json.loads(lineup_request or "{}")
            request = LineupRequest.model_validate(raw_request)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid lineup_request JSON: {exc}") from exc

        proj_path, projections_bytes = await _write_temp(projections)
        players_path, players_bytes = await _write_temp(players)

        parsed_players_mapping = _parse_mapping(players_mapping) or {}
        parsed_projection_mapping = _parse_mapping(projection_mapping) or {}
        if request.parallel_jobs is not None:
            parallel_jobs = request.parallel_jobs

        base_perturb_input = request.perturbation if request.perturbation is not None else perturbation
        if base_perturb_input is None:
            base_pct = 0.0
        else:
            base_pct = base_perturb_input * 100.0 if base_perturb_input <= 1.0 else base_perturb_input

        p25_source = request.perturbation_p25 if request.perturbation_p25 is not None else perturbation_p25_form
        if p25_source is None:
            perturbation_p25 = max(0.0, base_pct)
        else:
            perturbation_p25 = max(0.0, p25_source * 100.0 if p25_source <= 1.0 else p25_source)

        p75_source = request.perturbation_p75 if request.perturbation_p75 is not None else perturbation_p75_form
        if p75_source is None:
            perturbation_p75 = max(0.0, perturbation_p25)
        else:
            perturbation_p75 = max(0.0, p75_source * 100.0 if p75_source <= 1.0 else p75_source)
        if request.max_exposure is not None:
            max_exposure = request.max_exposure
        exposure_bias_input = request.exposure_bias if request.exposure_bias is not None else exposure_bias_form
        exposure_bias = _coerce_percentage_value(exposure_bias_input)
        if exposure_bias is None:
            exposure_bias = 0.0
        exposure_bias_target_input = request.exposure_bias_target if request.exposure_bias_target is not None else exposure_bias_target_form
        exposure_bias_target = _coerce_percentage_value(exposure_bias_target_input)
        site = raw_request.get("site") or site_form
        sport = raw_request.get("sport") or sport_form
        min_salary = raw_request.get("min_salary") if raw_request.get("min_salary") is not None else min_salary_form
        slate_id = slate_id_form.strip() if slate_id_form else None
        slate_name = slate_name_form.strip() if slate_name_form else None

        slate_inputs = _prepare_slate_inputs(
            site=site,
            sport=sport,
            players_path=players_path,
            players_bytes=players_bytes,
            projections_path=proj_path,
            projections_bytes=projections_bytes,
            players_mapping=parsed_players_mapping,
            projection_mapping=parsed_projection_mapping,
            slate_id=slate_id,
        )

        site = slate_inputs["resolved_site"]
        sport = slate_inputs["resolved_sport"]
        request = request.model_copy(update={"site": site, "sport": sport, "min_salary": min_salary})
        site = request.site
        sport = request.sport
        max_repeating_players = request.max_repeating_players
        if max_repeating_players is None:
            max_repeating_players = max_repeating_players_form
        lineups_per_job = request.lineups_per_job if request.lineups_per_job is not None else lineups_per_job
        parallel_jobs = max(1, parallel_jobs)
        perturbation_p25 = max(0.0, perturbation_p25)
        perturbation_p75 = max(0.0, perturbation_p75)
        max_exposure = max(0.0, min(1.0, max_exposure))
        if lineups_per_job is not None:
            lineups_per_job = max(1, min(1000, lineups_per_job))
        if max_repeating_players is not None:
            max_repeating_players = max(0, max_repeating_players)
        if exposure_bias_target is None and max_exposure is not None:
            exposure_bias_target = max_exposure * 100.0

        records: list[PlayerRecord] = slate_inputs["records"]
        raw_records: list[PlayerRecord] = slate_inputs.get("raw_records", records)
        raw_records: list[PlayerRecord] = slate_inputs.get("raw_records", records)
        mapping_report: MappingPreviewResponse = slate_inputs["report"]
        slate = slate_inputs["slate"]
        effective_players_mapping = slate_inputs["effective_players_mapping"]
        effective_projection_mapping = slate_inputs["effective_projection_mapping"]
        cleanup_paths: list[Path] = slate_inputs["cleanup_paths"]
        players_path = slate_inputs["players_path"]
        proj_path = slate_inputs["projections_path"]
        new_players_uploaded = slate_inputs["new_players_uploaded"]
        new_projections_uploaded = slate_inputs["new_projections_uploaded"]

        run_id = uuid4().hex
        store.create_job(
            run_id=run_id,
            site=site,
            sport=sport,
            state="running",
        )

        partial_message: str | None = None
        try:
            build_output = build_lineups(
                records,
                site=site,
                sport=sport,
                n_lineups=request.lineups,
                lock_player_ids=request.lock_player_ids,
                exclude_player_ids=request.exclude_player_ids,
                max_repeating_players=max_repeating_players,
                max_from_one_team=request.max_from_one_team,
                parallel_jobs=parallel_jobs,
                perturbation_p25=perturbation_p25,
                perturbation_p75=perturbation_p75,
                lineups_per_job=lineups_per_job,
                max_exposure=max_exposure,
                min_salary=min_salary,
                exposure_bias=exposure_bias,
                exposure_bias_target=exposure_bias_target,
            )
            lineups = build_output.lineups
            bias_summary = build_output.bias_summary or {}
        except ValueError as exc:
            store.update_job_state(run_id, state="failed", message=str(exc))
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except LineupGenerationPartial as exc:
            partial_message = exc.message
            lineups = exc.lineups
            bias_summary = exc.bias_summary or {}
        except Exception as exc:  # pragma: no cover
            store.update_job_state(run_id, state="failed", message=str(exc))
            raise
        finally:
            paths_to_cleanup = {path for path in cleanup_paths if path}
            if proj_path:
                paths_to_cleanup.add(proj_path)
            if players_path:
                paths_to_cleanup.add(players_path)
            for path in paths_to_cleanup:
                path.unlink(missing_ok=True)

        lineups_payload = [
            LineupResponse(
                lineup_id=lineup.lineup_id,
                salary=lineup.salary,
                projection=lineup.projection,
                baseline_projection=lineup.baseline_projection,
                players=[
                    LineupPlayerResponse(
                        player_id=player.player_id,
                        name=player.name,
                        team=player.team,
                        positions=list(player.positions),
                        salary=player.salary,
                        projection=player.projection,
                        ownership=player.ownership,
                        baseline_projection=player.baseline_projection,
                    )
                    for player in lineup.players
                ],
            )
            for lineup in lineups
        ]
        player_usage = _calculate_player_usage(lineups_payload)

        new_slate = None
        if new_players_uploaded or new_projections_uploaded:
            players_csv_text = (
                players_bytes.decode("utf-8")
                if new_players_uploaded and players_bytes is not None
                else (slate.players_csv if slate else "")
            )
            projections_csv_text = (
                projections_bytes.decode("utf-8")
                if new_projections_uploaded and projections_bytes is not None
                else (slate.projections_csv if slate else "")
            )
            slate_name_to_store = slate_name or (slate.name if slate else f"{site} {sport} Slate")
            players_filename_to_store = (
                players.filename if players and players.filename else (slate.players_filename if slate else "players.csv")
            )
            projections_filename_to_store = (
                projections.filename if projections and projections.filename else (slate.projections_filename if slate else "projections.csv")
            )
            new_slate = store.save_slate(
                site=site,
                sport=sport,
                name=slate_name_to_store,
                players_filename=players_filename_to_store,
                projections_filename=projections_filename_to_store,
                players_csv=players_csv_text,
                projections_csv=projections_csv_text,
                records=[record.model_dump() for record in records],
                report=mapping_report.model_dump(),
                players_mapping=effective_players_mapping,
                projection_mapping=effective_projection_mapping,
            )

        slate_used = new_slate or slate

        store.save_run(
            run_id=run_id,
            site=site,
            sport=sport,
            request={
                "lineups": request.lineups,
                "lock_player_ids": request.lock_player_ids,
                "exclude_player_ids": request.exclude_player_ids,
                "max_repeating_players": max_repeating_players,
                "max_from_one_team": request.max_from_one_team,
                "max_exposure": max_exposure,
                "lineups_per_job": lineups_per_job,
                "site": site,
                "sport": sport,
                "min_salary": min_salary,
                "perturbation_p25": perturbation_p25,
                "perturbation_p75": perturbation_p75,
                "exposure_bias": exposure_bias,
                "exposure_bias_target": exposure_bias_target,
                "bias_summary": bias_summary,
                "slate_id": slate_used.slate_id if slate_used else slate_id,
                "slate_name": slate_used.name if slate_used else (slate_name or None),
                "slate_players_filename": slate_used.players_filename if slate_used else (players.filename if players else None),
                "slate_projections_filename": slate_used.projections_filename if slate_used else (projections.filename if projections else None),
            },
            report=mapping_report.model_dump(),
            lineups=[lineup.model_dump() for lineup in lineups_payload],
            players_mapping=effective_players_mapping,
            projection_mapping=effective_projection_mapping,
        )

        if partial_message:
            store.update_job_state(run_id, state="completed", message=partial_message)

        if slate_used and bias_summary is not None:
            store.update_slate_bias(
                slate_used.slate_id,
                bias_factors=bias_summary.get("factors"),
                bias_summary=bias_summary,
            )

        response = LineupBatchResponse(
            run_id=run_id,
            report=mapping_report,
            lineups=lineups_payload,
            player_usage=player_usage,
            message=partial_message,
            slate_id=slate_used.slate_id if slate_used else None,
            bias_summary=bias_summary or None,
        )
        return response

    @app.get("/runs")
    async def list_runs(limit: int = 50):
        jobs = store.list_jobs(limit=limit)
        summaries: list[dict[str, Any]] = []
        for job in jobs:
            run = store.get_run(job.run_id)
            created_at = run.created_at if run else job.created_at
            summaries.append(
                {
                    "run_id": job.run_id,
                    "created_at": created_at.isoformat(),
                    "site": job.site,
                    "sport": job.sport,
                    "state": job.state,
                    "message": job.message,
                    "updated_at": job.updated_at.isoformat(),
                    "cancel_requested_at": job.cancel_requested_at.isoformat() if job.cancel_requested_at else None,
                    "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                    "has_results": run is not None,
                }
            )
        if len(summaries) < limit:
            existing_ids = {item["run_id"] for item in summaries}
            for run in store.list_runs(limit=limit):
                if run.run_id in existing_ids:
                    continue
                summaries.append(
                    {
                        "run_id": run.run_id,
                        "created_at": run.created_at.isoformat(),
                        "site": run.site,
                        "sport": run.sport,
                        "state": "completed",
                        "message": None,
                        "updated_at": run.created_at.isoformat(),
                        "cancel_requested_at": None,
                        "completed_at": run.created_at.isoformat(),
                        "has_results": True,
                    }
                )
        summaries.sort(key=lambda item: item["created_at"], reverse=True)
        return summaries[:limit]

    def _fetch_run_or_404(run_id: str):
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return run

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str):
        run = _fetch_run_or_404(run_id)
        job = store.get_job(run_id)
        return run_record_to_dict(run, job)

    @app.post("/runs/{run_id}/rerun", response_model=LineupBatchResponse)
    async def rerun(run_id: str):
        run = _fetch_run_or_404(run_id)
        lineups = [LineupResponse.model_validate(lineup) for lineup in run.lineups]
        return LineupBatchResponse(
            run_id=run.run_id,
            report=MappingPreviewResponse(
                total_players=run.report["total_players"],
                matched_players=run.report["matched_players"],
                players_missing_projection=run.report["players_missing_projection"],
                unmatched_projection_rows=run.report["unmatched_projection_rows"],
            ),
            lineups=lineups,
            player_usage=_calculate_player_usage(lineups),
            bias_summary=run.request.get("bias_summary") if isinstance(run.request, dict) else None,
        )

    @app.post("/runs/{run_id}/cancel")
    async def cancel_run(run_id: str):
        job = store.get_job(run_id)
        if job is None:
            run = store.get_run(run_id)
            if run is None:
                raise HTTPException(status_code=404, detail="Run not found")
            return {
                "run_id": run.run_id,
                "state": "completed",
                "site": run.site,
                "sport": run.sport,
                "message": None,
                "created_at": run.created_at.isoformat(),
                "updated_at": run.created_at.isoformat(),
                "cancel_requested_at": None,
                "completed_at": run.created_at.isoformat(),
            }
        if job.state in {"completed", "failed", "canceled"}:
            return job_to_dict(job)
        cancel_message = job.message or "Cancellation requested"
        updated = store.mark_job_cancel_requested(run_id, message=cancel_message)
        return job_to_dict(updated)

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
        slates_data = [slate_to_summary(slate) for slate in store.list_slates(limit=20)]
        content = _render_index_page(
            runs=runs,
            preview=None,
            result=None,
            error=None,
            success=None,
            players_mapping=default_players_mapping.copy(),
            projection_mapping=default_projection_mapping.copy(),
            lineups_count=20,
            parallel_jobs=1,
            perturbation_p25_value=40.0,
            perturbation_p75_value=10.0,
            exposure_bias_value=0.0,
            exposure_bias_target_value=50.0,
            max_exposure_value=0.5,
            lineups_per_job_value=None,
            max_repeating_players_value=None,
            site_value="FD",
            sport_value="NFL",
            min_salary_value=None,
            slates=slates_data,
            selected_slate_id=None,
            slate_name_value="",
        )
        return HTMLResponse(content)

    def _render_pool_page(
        *,
        site_filter: str | None,
        sport_filter: str | None,
        slate_filter: str | None,
        limit: int,
        all_dates: bool,
        notice: str | None,
        error: str | None,
    ) -> str:
        limit = max(1, min(500, limit))
        fetch_limit = limit if all_dates else max(limit * 3, limit)
        if slate_filter:
            fetch_limit = max(fetch_limit, limit * 5)
        runs = store.list_runs(limit=fetch_limit)

        selected_slate: SlateRecord | None = store.get_slate(slate_filter) if slate_filter else None
        if selected_slate is None and site_filter and sport_filter:
            selected_slate = store.get_latest_slate(site=site_filter, sport=sport_filter)
        if selected_slate is None and sport_filter:
            selected_slate = store.get_latest_slate(sport=sport_filter)
        if selected_slate is None and site_filter:
            selected_slate = store.get_latest_slate(site=site_filter)

        base_slates = store.list_slates(limit=50)
        if selected_slate is None and base_slates:
            selected_slate = base_slates[0]
        if selected_slate is not None:
            slate_filter = selected_slate.slate_id

        seen_slates: set[str] = set()
        ordered_slates: list[SlateRecord] = []

        def _extend(collection: Iterable[SlateRecord]) -> None:
            for slate in collection:
                if slate.slate_id in seen_slates:
                    continue
                seen_slates.add(slate.slate_id)
                ordered_slates.append(slate)

        if selected_slate is not None:
            _extend([selected_slate])
        if sport_filter:
            _extend(store.list_slates(sport=sport_filter, limit=50))
        if site_filter and sport_filter:
            _extend(store.list_slates(site=site_filter, sport=sport_filter, limit=50))
        elif site_filter:
            _extend(store.list_slates(site=site_filter, limit=50))
        _extend(base_slates)

        today = datetime.now(timezone.utc).astimezone().date()
        return _render_lineup_pool_page(
            runs,
            slates=ordered_slates,
            selected_slate=selected_slate,
            slate_filter=slate_filter,
            site_filter=site_filter,
            sport_filter=sport_filter,
            limit=limit,
            all_dates=all_dates,
            today=today,
            notice=notice,
            error=error,
        )

    @app.post("/ui/pool/{slate_id}/update")
    async def ui_pool_update_projections(
        slate_id: str,
        projections: UploadFile = File(...),
    ):
        slate = store.get_slate(slate_id)
        if slate is None:
            raise HTTPException(status_code=404, detail="Slate not found")

        proj_path, projections_bytes = await _write_temp(projections)
        if proj_path is None or projections_bytes is None:
            raise HTTPException(status_code=400, detail="Projections file is empty")

        players_path = _write_temp_from_bytes(slate.players_csv.encode("utf-8"))
        try:
            records, merge_report = merge_player_and_projection_files(
                players_path=players_path,
                projections_path=proj_path,
                site=slate.site,
                sport=slate.sport,
                players_mapping=slate.players_mapping or None,
                projection_mapping=slate.projection_mapping or None,
            )
            mapping_report = _report_to_mapping(merge_report)
            store.update_slate(
                slate_id,
                projections_filename=projections.filename or slate.projections_filename,
                projections_csv=projections_bytes.decode("utf-8"),
                records=[record.model_dump() for record in records],
                report=mapping_report.model_dump(),
            )
        except ValueError as exc:
            message = urllib.parse.quote_plus(str(exc))
            return RedirectResponse(f"/ui/pool?slate_id={slate_id}&error={message}", status_code=303)
        finally:
            players_path.unlink(missing_ok=True)
            if proj_path:
                proj_path.unlink(missing_ok=True)

        notice = urllib.parse.quote_plus("Projections updated")
        return RedirectResponse(f"/ui/pool?slate_id={slate_id}&notice={notice}", status_code=303)

    def _prepare_slate_inputs(
        *,
        site: str,
        sport: str,
        players_path: Path | None,
        players_bytes: bytes | None,
        projections_path: Path | None,
        projections_bytes: bytes | None,
        players_mapping: dict[str, str],
        projection_mapping: dict[str, str],
        slate_id: str | None,
    ) -> dict[str, Any]:
        new_players_uploaded = players_bytes is not None
        new_projections_uploaded = projections_bytes is not None

        slate: SlateRecord | None = None
        if slate_id:
            slate = store.get_slate(slate_id)
            if slate is None:
                raise HTTPException(status_code=400, detail="Selected slate not found")
        if slate is None and (players_path is None or projections_path is None):
            slate = store.get_latest_slate(site=site, sport=sport)

        if slate is None and (players_path is None or projections_path is None):
            raise HTTPException(status_code=400, detail="Players and projections files are required when no stored slate is available")

        resolved_site = slate.site if slate and not new_players_uploaded and players_path is None else site
        resolved_sport = slate.sport if slate and not new_players_uploaded and players_path is None else sport

        effective_players_mapping = dict(players_mapping) if players_mapping else (dict(slate.players_mapping) if slate else {})
        effective_projection_mapping = dict(projection_mapping) if projection_mapping else (dict(slate.projection_mapping) if slate else {})

        cleanup_paths: list[Path] = []
        if players_path is None:
            if slate is None:
                raise HTTPException(status_code=400, detail="Players file is required")
            players_path = _write_temp_from_bytes(slate.players_csv.encode("utf-8"))
            cleanup_paths.append(players_path)
            if not effective_players_mapping:
                effective_players_mapping = dict(slate.players_mapping)
        if projections_path is None:
            if slate is None:
                raise HTTPException(status_code=400, detail="Projections file is required")
            projections_path = _write_temp_from_bytes(slate.projections_csv.encode("utf-8"))
            cleanup_paths.append(projections_path)
            if not effective_projection_mapping:
                effective_projection_mapping = dict(slate.projection_mapping)

        use_cached_records = slate is not None and players_bytes is None and projections_bytes is None and new_players_uploaded is False and new_projections_uploaded is False and players_path in cleanup_paths and projections_path in cleanup_paths

        if use_cached_records and slate is not None:
            records = [PlayerRecord.model_validate(item) for item in slate.records]
            report = _report_to_mapping(slate.report)
        else:
            records, merge_report = merge_player_and_projection_files(
                players_path=players_path,
                projections_path=projections_path,
                site=resolved_site,
                sport=resolved_sport,
                players_mapping=effective_players_mapping or None,
                projection_mapping=effective_projection_mapping or None,
            )
            report = _report_to_mapping(merge_report)

        raw_records = list(records)
        if slate and slate.bias_factors:
            records = _apply_bias_to_player_records(raw_records, slate.bias_factors)
        else:
            records = list(records)

        return {
            "records": records,
            "raw_records": raw_records,
            "report": report,
            "slate": slate,
            "effective_players_mapping": effective_players_mapping,
            "effective_projection_mapping": effective_projection_mapping,
            "cleanup_paths": cleanup_paths,
            "players_path": players_path,
            "projections_path": projections_path,
            "new_players_uploaded": new_players_uploaded,
            "new_projections_uploaded": new_projections_uploaded,
            "resolved_site": resolved_site,
            "resolved_sport": resolved_sport,
        }

    @app.get("/ui/pool", response_class=HTMLResponse)
    async def ui_pool(
        request: Request,
        site: str | None = None,
        sport: str | None = None,
        slate_id: str | None = None,
        limit: int = 50,
        all_dates: bool = Query(False),
    ):
        notice_msg = request.query_params.get("notice")
        error_msg = request.query_params.get("error")
        content = _render_pool_page(
            site_filter=(site or None) and (site or None).upper(),
            sport_filter=(sport or None) and (sport or None).upper(),
            slate_filter=slate_id,
            limit=limit,
            all_dates=all_dates,
            notice=notice_msg,
            error=error_msg,
        )
        return HTMLResponse(content)

    @app.get("/ui/pool/{sport}", response_class=HTMLResponse)
    async def ui_pool_sport_only(
        request: Request,
        sport: str,
        limit: int = 50,
        all_dates: bool = Query(False),
    ):
        notice_msg = request.query_params.get("notice")
        error_msg = request.query_params.get("error")
        content = _render_pool_page(
            site_filter=None,
            sport_filter=sport.upper(),
            slate_filter=request.query_params.get("slate_id"),
            limit=limit,
            all_dates=all_dates,
            notice=notice_msg,
            error=error_msg,
        )
        return HTMLResponse(content)

    @app.get("/ui/pool/{sport}/{site}", response_class=HTMLResponse)
    async def ui_pool_shortcut(
        request: Request,
        sport: str,
        site: str,
        limit: int = 50,
        all_dates: bool = Query(False),
    ):
        notice_msg = request.query_params.get("notice")
        error_msg = request.query_params.get("error")
        content = _render_pool_page(
            site_filter=site.upper(),
            sport_filter=sport.upper(),
            slate_filter=request.query_params.get("slate_id"),
            limit=limit,
            all_dates=all_dates,
            notice=notice_msg,
            error=error_msg,
        )
        return HTMLResponse(content)

    @app.post("/slates/{slate_id}/reset-bias")
    async def reset_slate_bias(slate_id: str, redirect: str | None = Form(None)):
        target = redirect or "/ui"
        try:
            store.update_slate_bias(slate_id, bias_factors={}, bias_summary={})
        except KeyError:
            raise HTTPException(status_code=404, detail="Slate not found")
        return RedirectResponse(url=target, status_code=303)

    @app.post("/ui", response_class=HTMLResponse)
    async def ui_handle(
        request: Request,
        submit_action: str = Form(...),
        players: UploadFile | None = File(None),
        projections: UploadFile | None = File(None),
        players_mapping: str = Form(""),
        projection_mapping: str = Form(""),
        lineups: int = Form(20),
        max_repeating_players: int | None = Form(None),
        parallel_jobs: int = Form(1),
        perturbation: float = Form(0.0),
        perturbation_p25_form: float | None = Form(None, alias="perturbation_p25"),
        perturbation_p75_form: float | None = Form(None, alias="perturbation_p75"),
        exposure_bias_form: float | None = Form(None, alias="exposure_bias"),
        exposure_bias_target_form: float | None = Form(None, alias="exposure_bias_target"),
        max_exposure: float = Form(0.5),
        lineups_per_job: int | None = Form(None),
        site: str = Form("FD"),
        sport: str = Form("NFL"),
        min_salary: int | None = Form(None),
        slate_id: str | None = Form(None),
        slate_name: str | None = Form(None),
    ):
        parsed_players_mapping = _parse_mapping(players_mapping) or DEFAULT_PLAYERS_MAPPING.copy()
        parsed_projection_mapping = _parse_mapping(projection_mapping) or DEFAULT_PROJECTION_MAPPING.copy()

        base_pct_value = _coerce_percentage_value(perturbation)
        p25_normalized = _coerce_percentage_value(perturbation_p25_form)
        if p25_normalized is None:
            perturbation_p25 = max(0.0, base_pct_value or 0.0)
        else:
            perturbation_p25 = max(0.0, p25_normalized)

        p75_normalized = _coerce_percentage_value(perturbation_p75_form)
        if p75_normalized is None:
            fallback = perturbation_p25 if perturbation_p25 > 0.0 else (base_pct_value or 0.0)
            perturbation_p75 = max(0.0, fallback)
        else:
            perturbation_p75 = max(0.0, p75_normalized)

        exposure_bias = _coerce_percentage_value(exposure_bias_form)
        if exposure_bias is None:
            exposure_bias = 0.0
        exposure_bias_target = _coerce_percentage_value(exposure_bias_target_form)

        parallel_jobs = max(1, parallel_jobs)
        max_exposure = max(0.0, min(1.0, max_exposure))
        if lineups_per_job is not None:
            lineups_per_job = max(1, min(1000, lineups_per_job))
        if max_repeating_players is not None:
            max_repeating_players = max(0, max_repeating_players)
        if exposure_bias_target is None and max_exposure is not None:
            exposure_bias_target = max_exposure * 100.0

        proj_path, projections_bytes = await _write_temp(projections)
        players_path, players_bytes = await _write_temp(players)

        slate_inputs = _prepare_slate_inputs(
            site=site,
            sport=sport,
            players_path=players_path,
            players_bytes=players_bytes,
            projections_path=proj_path,
            projections_bytes=projections_bytes,
            players_mapping=parsed_players_mapping,
            projection_mapping=parsed_projection_mapping,
            slate_id=slate_id.strip() if slate_id else None,
        )

        site = slate_inputs["resolved_site"]
        sport = slate_inputs["resolved_sport"]
        records: list[PlayerRecord] = slate_inputs["records"]
        raw_records: list[PlayerRecord] = slate_inputs["raw_records"]
        mapping_report: MappingPreviewResponse = slate_inputs["report"]
        slate_obj = slate_inputs["slate"]
        effective_players_mapping = slate_inputs["effective_players_mapping"]
        effective_projection_mapping = slate_inputs["effective_projection_mapping"]
        cleanup_paths: list[Path] = slate_inputs["cleanup_paths"]
        players_path = slate_inputs["players_path"]
        proj_path = slate_inputs["projections_path"]
        new_players_uploaded = slate_inputs["new_players_uploaded"]
        new_projections_uploaded = slate_inputs["new_projections_uploaded"]

        preview = None
        result = None
        error = None
        success = None
        redirect_url: str | None = None

        try:
            if submit_action == "preview":
                preview = mapping_report
                success = "Preview generated. Review report below."
            else:
                run_id = uuid4().hex
                store.create_job(
                    run_id=run_id,
                    site=site,
                    sport=sport,
                    state="running",
                )
                partial_message = None
                bias_summary: dict | None = None
                try:
                    build_output = build_lineups(
                        records,
                        site=site,
                        sport=sport,
                        n_lineups=lineups,
                        max_repeating_players=max_repeating_players,
                        max_from_one_team=None,
                        lock_player_ids=None,
                        exclude_player_ids=None,
                        parallel_jobs=parallel_jobs,
                        perturbation_p25=perturbation_p25,
                        perturbation_p75=perturbation_p75,
                        lineups_per_job=lineups_per_job,
                        max_exposure=max_exposure,
                        min_salary=min_salary,
                        exposure_bias=exposure_bias,
                        exposure_bias_target=exposure_bias_target,
                    )
                    built_lineups = build_output.lineups
                    bias_summary = build_output.bias_summary or {}
                except LineupGenerationPartial as exc:
                    partial_message = exc.message
                    built_lineups = exc.lineups
                    bias_summary = exc.bias_summary or {}
                    store.update_job_state(run_id, state="completed", message=exc.message)
                except Exception as exc:
                    store.update_job_state(run_id, state="failed", message=str(exc))
                    raise
                else:
                    store.update_job_state(run_id, state="completed")

                lineups_payload = [
                    LineupResponse(
                        lineup_id=lineup.lineup_id,
                        salary=lineup.salary,
                        projection=lineup.projection,
                        baseline_projection=lineup.baseline_projection,
                        players=[
                            LineupPlayerResponse(
                                player_id=player.player_id,
                                name=player.name,
                                team=player.team,
                                positions=list(player.positions),
                                salary=player.salary,
                                projection=player.projection,
                                ownership=player.ownership,
                                baseline_projection=player.baseline_projection,
                            )
                            for player in lineup.players
                        ],
                    )
                    for lineup in built_lineups
                ]
                player_usage = _calculate_player_usage(lineups_payload)
                usage_lookup_mapping = {item.player_id: item.exposure for item in player_usage}

                new_slate = None
                if new_players_uploaded or new_projections_uploaded:
                    players_csv_text = (
                        players_bytes.decode("utf-8")
                        if new_players_uploaded and players_bytes is not None
                        else (slate_obj.players_csv if slate_obj else "")
                    )
                    projections_csv_text = (
                        projections_bytes.decode("utf-8")
                        if new_projections_uploaded and projections_bytes is not None
                        else (slate_obj.projections_csv if slate_obj else "")
                    )
                    slate_name_to_store = slate_name or (slate_obj.name if slate_obj else f"{site} {sport} Slate")
                    players_filename_to_store = (
                        players.filename if players and players.filename else (slate_obj.players_filename if slate_obj else "players.csv")
                    )
                    projections_filename_to_store = (
                        projections.filename if projections and projections.filename else (slate_obj.projections_filename if slate_obj else "projections.csv")
                    )
                    new_slate = store.save_slate(
                        site=site,
                        sport=sport,
                        name=slate_name_to_store,
                        players_filename=players_filename_to_store,
                        projections_filename=projections_filename_to_store,
                        players_csv=players_csv_text,
                        projections_csv=projections_csv_text,
                        records=[record.model_dump() for record in raw_records],
                        report=mapping_report.model_dump(),
                        players_mapping=effective_players_mapping,
                        projection_mapping=effective_projection_mapping,
                        bias_factors=bias_summary.get("factors") if bias_summary else None,
                        bias_summary=bias_summary,
                    )

                slate_used = new_slate or slate_obj

                store.save_run(
                    run_id=run_id,
                    site=site,
                    sport=sport,
                    request={
                        "lineups": lineups,
                        "max_repeating_players": max_repeating_players,
                        "max_exposure": max_exposure,
                        "lineups_per_job": lineups_per_job,
                        "site": site,
                        "sport": sport,
                        "min_salary": min_salary,
                        "perturbation_p25": perturbation_p25,
                        "perturbation_p75": perturbation_p75,
                        "exposure_bias": exposure_bias,
                        "exposure_bias_target": exposure_bias_target,
                        "bias_summary": bias_summary,
                        "slate_id": slate_used.slate_id if slate_used else (slate_id.strip() if slate_id else None),
                        "slate_name": slate_used.name if slate_used else (slate_name or None),
                        "slate_players_filename": slate_used.players_filename if slate_used else (players.filename if players else None),
                        "slate_projections_filename": slate_used.projections_filename if slate_used else (projections.filename if projections else None),
                    },
                    report=mapping_report.model_dump(),
                    lineups=[lineup.model_dump() for lineup in lineups_payload],
                    players_mapping=effective_players_mapping,
                    projection_mapping=effective_projection_mapping,
                )

                if slate_used and bias_summary is not None:
                    store.update_slate_bias(
                        slate_used.slate_id,
                        bias_factors=bias_summary.get("factors"),
                        bias_summary=bias_summary,
                    )

                success = f"Run saved with ID {run_id}"
                if partial_message:
                    success = f"Partial run saved with ID {run_id}"
                if lineups_payload:
                    result = {
                        "run_id": run_id,
                        "lineup": lineups_payload[0],
                        "usage_lookup": usage_lookup_mapping,
                        "min_salary": min_salary,
                        "slate_id": slate_used.slate_id if slate_used else None,
                        "bias_summary": bias_summary,
                    }
                redirect_url = f"/ui/runs/{run_id}"
        except ValueError as exc:
            error = str(exc)
        finally:
            paths_to_cleanup = {path for path in cleanup_paths if path}
            if proj_path:
                paths_to_cleanup.add(proj_path)
            if players_path:
                paths_to_cleanup.add(players_path)
            for path in paths_to_cleanup:
                path.unlink(missing_ok=True)

        if redirect_url:
            return RedirectResponse(url=redirect_url, status_code=303)

        runs = store.list_runs(limit=20)
        slates_data = [slate_to_summary(slate) for slate in store.list_slates(limit=20)]
        content = _render_index_page(
            runs=runs,
            preview=preview,
            result=result,
            error=error,
            success=success,
            players_mapping=parsed_players_mapping,
            projection_mapping=parsed_projection_mapping,
            lineups_count=lineups,
            parallel_jobs=parallel_jobs,
            perturbation_p25_value=perturbation_p25,
            perturbation_p75_value=perturbation_p75,
            exposure_bias_value=exposure_bias,
            exposure_bias_target_value=exposure_bias_target if exposure_bias_target is not None else (max_exposure * 100.0 if max_exposure is not None else 0.0),
            max_exposure_value=max_exposure,
            lineups_per_job_value=lineups_per_job,
            max_repeating_players_value=max_repeating_players,
            site_value=site,
            sport_value=sport,
            min_salary_value=min_salary,
            slates=slates_data,
            selected_slate_id=slate_id.strip() if slate_id else None,
            slate_name_value=slate_name,
        )
        return HTMLResponse(content)

    @app.get("/ui/runs/{run_id}", response_class=HTMLResponse)
    async def ui_run_detail(request: Request, run_id: str):
        run = _fetch_run_or_404(run_id)
        content = _render_run_detail_page(run)
        return HTMLResponse(content)


    return app
