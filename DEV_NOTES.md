# pydfs Dev Notes

Running summary of major decisions and implementation details so future sessions can quickly regain context.

## Environment
- Python dependencies managed via editable install (`pip install --break-system-packages -e .`) to keep CLI/API available system-wide. Consider moving to a project-local virtualenv (`python3 -m venv .venv && source .venv/bin/activate`) for cleaner isolation.

## Key Features Implemented (2025-02-08)
- Canonical player model with projection ingestion supporting flexible column mappings.
- CLI that merges players + projections, reports mismatches, exports lineups (with names/teams/positions/ownership) and optional JSON report. Profiles (`--save-profile`/`--load-profile`) persist column mappings.
- Optimizer service wraps `pydfs-lineup-optimizer` with exposure controls.
- FastAPI skeleton with `/health`, `/preview`, and `/lineups` endpoints (file uploads + JSON payload support); tests use sample CSVs to validate flow.

## Testing
- `python3 -m pytest` covers ingestion, optimizer, and API routes.

## Next Ideas
- Finish API integration tests (current run hits timeout due to hanging pytest after API tests start).
- Persist merge stats automatically alongside lineup outputs.
- Build simple frontend or REST client for upload + run workflow.

Keep this file updated after each significant change set.
