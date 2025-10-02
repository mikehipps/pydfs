# pydfs

Fresh DFS optimizer toolkit built around [`pydfs-lineup-optimizer`](https://github.com/DimaKudosh/pydfs-lineup-optimizer).

## Repo layout

- `src/pydfs/` – application code (projection ingestion, optimizer wrappers, API layers).
- `tests/` – unit/integration tests mirroring the `src` layout.
- `pyproject.toml` – dependency + tooling configuration (pytest, ruff, mypy).

## Local setup

```bash
# optional: create a dedicated virtualenv
python3 -m venv .venv
source .venv/bin/activate

pip install -U pip
pip install -e .[dev]
```

## Next steps

1. Build the hand-builder workflow: surface lock/exclude tooling against the filtered pool and persist manual lineups for
   export alongside optimizer results.
2. Solidify core models (`players`, `slates`, `constraints`) under `src/pydfs/models/` and ensure they're reused by the solver,
   persistence, and export layers.
3. Expand ingestion adapters beyond the current sport/site defaults while keeping projections + injury data normalised across
   slates.
4. Continue layering persistence / API / UI improvements once the optimizer contract and export pipeline are stable.

## Lineup pool filtering & export

- The `/ui/pool` view now includes projection, salary, usage, uniqueness, player, and team filters plus contest-ready CSV
  export for the filtered selection.
- Programmatic access is available via `POST /pool/filter` (returns filtered lineups + summary metrics) and
  `GET /pool/export.csv` (downloads the filtered lineups in FanDuel Classic format).
- Filtering logic lives under `pydfs.pool.filtering` and the contest serializers under `pydfs.pool.export`.

Tracking work in issues / ADRs up front will help keep multi-sport support consistent as we grow.
