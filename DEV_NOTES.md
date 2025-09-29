# pydfs Dev Notes

Running summary of major decisions and implementation details so future sessions can quickly regain context.

## Environment
- Python dependencies managed via editable install (`pip install --break-system-packages -e .`) to keep CLI/API available system-wide. Consider moving to a project-local virtualenv (`python3 -m venv .venv && source .venv/bin/activate`) for cleaner isolation.

## Key Features Implemented (2025-02-08)
- Canonical player model with projection ingestion supporting flexible column mappings.
- CLI that merges players + projections, reports mismatches, exports lineups (with names/teams/positions/ownership) and optional JSON report. Profiles (`--save-profile`/`--load-profile`) persist column mappings.
- Optimizer service wraps `pydfs-lineup-optimizer` with exposure controls.
- FastAPI API (`/health`, `/preview`, `/lineups`) accepts file uploads + JSON payloads, supports ownership mapping, and returns merge stats alongside lineup payloads. Tests use sample CSVs to validate flow.
- API tests run via `httpx.AsyncClient` with AnyIO; previously observed TestClient hang resolved.
- HTTP client helper at `scripts/api_client.py` drives preview/lineups from the command line.

## Testing
- `python3 -m pytest` covers ingestion, optimizer, and API routes.

## Next Ideas
- Add persistence hooks (e.g., store merge reports or lineups to a database / filesystem for later retrieval).
- Build a simple UI (web form or dashboard) that walks users through upload → preview → lineup generation.
- Harden API error responses and validation (e.g., better messaging for infeasible solves, profile management endpoints).

Keep this file updated after each significant change set.
