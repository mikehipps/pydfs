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

1. Productise the lineup pool: expose robust filtering, selection, and export flows (UI + API) so generated runs can ship
   directly to contest CSVs.
2. Solidify core models (`players`, `slates`, `constraints`) under `src/pydfs/models/` and ensure they're reused by the solver,
   persistence, and export layers.
3. Expand ingestion adapters beyond the current sport/site defaults while keeping projections + injury data normalised across
   slates.
4. Continue layering persistence / API / UI improvements once the optimizer contract and export pipeline are stable.

Tracking work in issues / ADRs up front will help keep multi-sport support consistent as we grow.
