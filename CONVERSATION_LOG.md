# Conversation Log – 2025-02-11

## Highlights
- Confirmed pool rescoring now uses the most recent projections; missing players drop to zero so stale lineups fall out of the top 100.
- Added default filters for `/ui/pool`, new shortcut routes (`/ui/pool/{sport}/{site}`), and today-only run selection with an optional "include previous days" flag.
- Planned future enhancements around advanced filtering (median thresholds, player locks) once the combined pool view is in place.

## Recent Conversation Snapshot
- **User**: Verified the pooled page works but noted some lineups still showed outdated projections; requested that only the most recent projections be used.
- **Assistant**: Enforced overrides so every lineup player baseline comes from the newest projections (missing players → 0) and re-ran tests.
- **User**: Confirmed the fix and asked for default filters plus shortcut routes (`/ui/pool/sport/site`).
- **Assistant**: Implemented today-only filtering, added "include previous days", and created shortcut routes; updated tests accordingly.
- **User**: Confirmed success and requested pause with commits + notes.

Refer to `DEV_NOTES.md` for the technical summary of today's changes.
