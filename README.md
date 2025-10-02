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

1. Flesh out the canonical domain models (players, slates, constraints) under `src/pydfs/models/`.
2. Wrap `pydfs-lineup-optimizer` behind a service module to keep business logic isolated from the third-party API.
3. Port MLB ingestion, then expand to NFL/NBA via adapter modules that emit the shared schema.
4. Layer on persistence / API / UI pieces once the core optimizer contract is stable.

Tracking work in issues / ADRs up front will help keep multi-sport support consistent as we grow.

_Temporary sync test note – safe to remove once verified._
