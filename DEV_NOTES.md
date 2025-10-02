# pydfs Dev Notes

Running summary of major decisions and implementation details so future sessions can quickly regain context.

## Environment
- Python dependencies managed via editable install (`pip install --break-system-packages -e .`) to keep CLI/API available system-wide. Consider moving to a project-local virtualenv (`python3 -m venv .venv && source .venv/bin/activate`) for cleaner isolation.

## Key Features Implemented (2025-02-08)
- Canonical player model with projection ingestion supporting flexible column mappings.
- CLI that merges players + projections, reports mismatches, exports lineups (with names/teams/positions/ownership) and optional JSON report. Profiles (`--save-profile`/`--load-profile`) persist column mappings.
- Optimizer service wraps `pydfs-lineup-optimizer` with exposure controls.
- FastAPI API (`/health`, `/preview`, `/lineups`, `/runs`, `/runs/{id}`, `/runs/{id}/rerun`) accepts file uploads + JSON payloads, supports ownership mapping, persists run records, and exposes history/rerun functionality backed by SQLite (`RunStore`). `/ui` endpoints render lightweight HTML pages for upload/preview/run history without requiring external templating packages.
- API tests run via `httpx.AsyncClient` with AnyIO; previously observed TestClient hang resolved.
- HTTP client helper at `scripts/api_client.py` drives preview/lineups from the command line. Supports run listing, retrieval, and CSV export.

## Testing
- `python3 -m pytest` covers ingestion, optimizer, and API routes.

## Next Ideas
- Add persistence hooks (e.g., store merge reports or lineups to a database / filesystem for later retrieval).
- Build a simple UI (web form or dashboard) that walks users through upload → preview → lineup generation.
- Harden API error responses and validation (e.g., better messaging for infeasible solves, profile management endpoints).

Keep this file updated after each significant change set.

## Session Summary (2025-02-09)
- **Performance enhancements**: Added batch logging with per-batch elapsed times, configurable `lineups_per_job`, max exposure, and max repeating players across UI/API/CLI. Default solver gap now `gapRel=0.001` (override with `PYDFS_SOLVER_GAP`).
- **UI/UX**: Form includes knobs for exposure/overlap/batch size; run detail pages only show top 100 most frequent lineups (with duplicate counts) and display configured run parameters.
- **Player usage / uniqueness**: Backend tracks unique lineup counts per batch and surfaces player usage tables on run detail view. API responses include `player_usage`.
- **Ingestion guardrails**: Negative projection values are now clamped to zero during CSV parsing to prevent validation errors from fallback FPPG columns.
- **Randomness weighting**: Perturbation now scales with projection rank so elite players receive tighter noise while deep-value plays see larger swings (sport-agnostic).
- **Roster hygiene**: Projection merges now drop players without projections, respect injury indicators (skip `OUT/IL` statuses), and—when sport is MLB—exclude non-probable pitchers if the flag is present.
- **UI controls**: Added site/sport selectors and a minimum salary override so different slates can reuse the same flow without hard-coded NFL defaults.
- **Partial runs & cancellation groundwork**: Optimizer raises `LineupGenerationPartial` with the lineups collected so far; API/CLI persist partial batches, surface a friendly message, and keep results instead of failing outright. Batch logs now include full position counts and salary caps for easier troubleshooting.
- **Lineup analytics**: Run detail pages display usage-based metrics (usage sum, uniqueness) with percentile context and human-friendly formatting (M/B abbreviations). Summary table reports the same stats across the pool.
- **Current workflow**:
  - Typical run command: `PYDFS_SOLVER=cbc uvicorn pydfs.api:create_app --host 0.0.0.0 --port 8000`
  - Preferred run settings during testing: `parallel_jobs≈10`, `lineups_per_job` tuned between 25–40, `max_exposure=0.5`, `max_repeating_players` optionally set for diversity.
  - Batch logs now show total elapsed and per-batch time to help dial in parameters (watch for ~0.3s savings across ~200 batches ⇒ ~1 min overall).
- **Open questions / next steps**:
  1. Benchmark different `lineups_per_job` values (20/30/40/50) against uniqueness + wall time; record results for future reference.
  2. Explore background job architecture or cancellation flag to support “stop & save” runs; currently synchronous HTTP flow prevents interruptions.
  3. Investigate heuristic/“fast-pass” lineup generator for cheap diversity before full optimization.
  4. Plan simulation pipeline integration once lineup generation parameters are finalized.
  5. Design interactive “hand builder” tool: filter the 10k lineup pool by locked picks, update player usage counts in real time, and suggest completions (optionally calling the solver for fresh options).
  6. Investigate a background “lineup pool maintainer”: keep solving for additional uniques once a slate is uploaded, and on projection updates rescore the existing pool and continue the search (handles late injury news).
  7. Finish cancellation UX: wire UI button to the new `/runs/{id}/cancel` endpoint, ensure background workers respect cancel flags, and move generation into an interruptible flow.

## Session Summary (2025-02-10)
- **Job state tracking**: Added `run_jobs` table with helper methods so runs now carry lifecycle state (`running`, `cancel_requested`, `completed`, `failed`). `RunStore.save_run` finalises the job automatically.
- **API surface**: `/runs` and `/runs/{id}` now include job metadata, `create_app` exposes the store via `app.state`, and the new `/runs/{id}/cancel` endpoint marks jobs as cancel-requested while returning their status.
- **Tests**: Expanded API tests to cover job state responses and cancellation flow.
- **Operational notes**: If uvicorn hangs, `pkill -9 -f "uvicorn pydfs.api:create_app"` followed by killing lingering multiprocessing helpers (`ps -fC python3`) clears the port. Restart command above reloads config. Partial results are saved per batch via `RunStore`; consider future enhancement to persist after each batch for safer cancellation.

## Session Summary (2025-02-11)
- **Lineup rescoring**: Pooled and run-detail views now normalise legacy records and override every player's baseline projection with the most recent projections (missing players fall back to `0`). Top lineup lists sort strictly by current baselines.
- **Combined pool UX**: Added `/ui/pool` dashboard defaulting to today's runs (limit 50) with optional "include previous days" toggle, plus quick routes like `/ui/pool/nfl/fd`. Summary table shows range context, run list, and uses the rescored data.
- **Testing**: Added coverage for the new pool endpoints and verified the default range + shortcut rendering via HTTPX tests.
- **Slate persistence**: Introduced a `slates` store capturing raw player/projection CSVs, mappings, and parsed records. API/UI runs can now reuse the latest (or selected) slate without re-uploading files, and responses include the associated `slate_id` for easy follow-up runs.
- **Perturbation controls**: Replaced the single noise input with percentile anchors (P25/P75). API, CLI, and UI accept fractional or percent values, optimizer interpolates a smooth variance curve (bottom 25% up to 1.5× the P25 window, studs taper to 0.5× the P75 cap), and new tests lock the behaviour down.
- **Slate-centric pool view**: Lineup pool filters now pivot on stored slates (with friendly names), shows slate metadata, and exposes a projections-replacement flow that re-merges the slate and rescales the pool without generating new lineups.
- **Pool defaults**: `/ui/pool`, `/ui/pool/<sport>`, and `/ui/pool/<sport>/<site>` automatically load the most recent slate for their scope (overall, sport, sport+site) while keeping a dropdown to switch slates; tests cover the new defaults.
- **Usage-aware randomness**: Optimizer now biases projections on the fly based on cumulative usage—players trending under target exposure get positive boosts while over-used cores get tapered. Bias strength/target are configurable via API/CLI/UI, metadata is persisted with each run, and tests cover the new helpers.

## Immediate Focus: Lineup Pool Filtering, Export, and Hand Builder

The current docs highlight rich pool summaries (usage metrics, rescoring, slate awareness) but we still lack tooling to curate
and ship lineups once they're generated. Prioritise the following track so the workflow progresses from "generate" to "deploy":

1. **Baseline data audit**
   - Confirm the `RunStore`/`SlateStore` payloads already persisted for pool views (usage tables, rescored projections, slate
     metadata). Surface any missing attributes required for filtering (e.g., cumulative ownership, salary span, stack tags) so we
     know whether to extend the solver metadata or derive them post-hoc.
   - Catalogue the contest CSV schemas we care about first (e.g., FD single-entry vs. MME) and note any fields not present in the
     stored lineup representation.

2. **Server-side filtering contract**
   - Add API endpoints (or extend `/ui/pool` handlers) that accept filter parameters: projection/ceiling ranges, ownership caps,
     overlap limits, team/game filters, salary buckets, min/max player exposures, etc.
   - Reuse the existing rescored lineup payload so filters act on up-to-date projections; ensure responses keep both summary stats
     and the filtered lineup list to feed UI + exports.
   - Introduce a lightweight query object (or pydantic model) to keep validation consistent across API/UI and future CLI usage.

3. **UI workflow**
   - Expand the pool page with a filtering sidebar (form submission first; progressive enhancement with HTMX/JS later). Display
     result counts, aggregate stats (mean projection, ownership sums), and allow multi-select for export.
   - Provide quick presets (e.g., "Highest projection", "Balanced ownership", "Bring-backs") powered by saved filter configs.
   - Surface per-lineup affordances for the hand builder: lock players, clone to manual editor, mark favorites.

4. **Export pipeline**
   - Implement contest-specific CSV serializers that map our lineup model to required columns (player IDs, captain flag, flex
     slots). Include validations so users can't export mis-sized rosters.
   - Wire exports to both UI (download button) and CLI/script flows. Logging should reference the slate, run ID, filters applied,
     and timestamp for auditability.

5. **Hand builder foundation**
   - Back the manual builder with the same slate data (player projections, ownership, salary) and expose helper endpoints to
     suggest completions or validate partial rosters.
   - Allow users to lock players, see remaining salary/slots, and optionally request "fill recommendations" using the optimizer
     with locks enforced. Persist manual lineups alongside generated ones for export parity.

6. **Follow-up hygiene**
   - Document contest templates and filter presets in `README` once implemented.
   - Add end-to-end tests (API + UI) that cover a filter → export cycle to prevent regressions.

## Session Summary (2025-02-12)
- **Filtering UX**: `/ui/pool` now exposes baseline/projection/salary/usage/uniqueness filters with player/team include/exclude
  controls, surfaces filtered summaries, and wires a CSV export button for contest-ready payloads.
- **API endpoints**: Added `POST /pool/filter` for programmatic filtering plus `GET /pool/export.csv` for FanDuel Classic exports
  powered by the new `pydfs.pool.filtering` and `pydfs.pool.export` helpers.
- **Regression coverage**: New integration test exercises the filter → export flow to keep the API + UI contract locked down.
