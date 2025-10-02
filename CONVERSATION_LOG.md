# Conversation Log – 2025-02-11

## Highlights
- Confirmed pool rescoring now uses the most recent projections; missing players drop to zero so stale lineups fall out of the top 100.
- Added default filters for `/ui/pool`, new shortcut routes (`/ui/pool/{sport}/{site}`), and today-only run selection with an optional "include previous days" flag.
- Planned future enhancements around advanced filtering (median thresholds, player locks) once the combined pool view is in place.
- Delivered percentile-based perturbation controls (P25/P75 knobs), updated UI/CLI/API wiring, and documented/validated the new randomness curve.
- Lineup pool now centers on named slates, displays current projection metadata, and lets you refresh projections in place so the pool rescales without a fresh solve.
- Pool routes now auto-select the freshest slate (global/sport/site-specific) so `/ui/pool`, `/ui/pool/{sport}`, and `/ui/pool/{sport}/{site}` always open on the latest relevant context while still allowing manual slate selection.
- Added usage-aware randomness: successive optimizer jobs bias projections toward underused players (and away from overused ones) with configurable strength/targets; state is persisted with runs and surfaced through the API/CLI/UI inputs.
- Delivered percentile-based perturbation controls (P25/P75 knobs), updated UI/CLI/API wiring, and documented/validated the new randomness curve.

## Recent Conversation Snapshot
- **User**: Verified the pooled page works but noted some lineups still showed outdated projections; requested that only the most recent projections be used.
- **Assistant**: Enforced overrides so every lineup player baseline comes from the newest projections (missing players → 0) and re-ran tests.
- **User**: Confirmed the fix and asked for default filters plus shortcut routes (`/ui/pool/sport/site`).
- **Assistant**: Implemented today-only filtering, added "include previous days", and created shortcut routes; updated tests accordingly.
- **User**: Confirmed success and requested pause with commits + notes.
- **User**: Requested persistence for player/projection uploads and slate reuse; approved implementation plan.
- **Assistant**: Added slate storage in SQLite, updated API/UI to reuse or update slates, exposed slate IDs in responses, and expanded tests.
- **User**: Asked for wider-but-smarter randomness controls with soft caps tied to projection percentiles.
- **Assistant**: Added P25/P75 knobs across the stack, reworked optimizer perturbation logic with quantile-aware windows, and backfilled tests + notes.
- **User**: Wanted the pool view to key off friendly slate names and support quick projection refreshes without re-running lineups.
- **Assistant**: Reworked the pool filters around slates, surfaced slate metadata on the dashboard, and added a projections-upload pathway that updates the slate + rescored pool in one step.

Refer to `DEV_NOTES.md` for the technical summary of today's changes.
