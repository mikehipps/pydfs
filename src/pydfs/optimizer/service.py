"""Wrapper utilities for running pydfs-lineup-optimizer."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import multiprocessing as mp
import os
import random
import time
from collections import defaultdict
from typing import Iterable, List, Optional, Sequence, Tuple, Mapping

from pydfs_lineup_optimizer import Site, Sport, get_optimizer
from pydfs_lineup_optimizer.lineup import Lineup
from pydfs_lineup_optimizer.exceptions import LineupOptimizerException

from pydfs.models import PlayerRecord


logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)

_SOLVER_CONFIGURED = False
_SOLVER_ENV = "PYDFS_SOLVER"
_SOLVER_GAP_ENV = "PYDFS_SOLVER_GAP"
_PLAYER_RETAIN_ENV = "PYDFS_PLAYER_RETAIN"
_PLAYER_MIN_PER_POS_ENV = "PYDFS_PLAYER_MIN_PER_POS"

_PLAYER_RETAIN_DEFAULT = 0.75
_PLAYER_MIN_PER_POS_DEFAULT = 8


def _env_float(name: str, default: float, *, clamp_min: float | None = None, clamp_max: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid float for %s: %s; using default %.2f", name, raw, default)
        return default
    if clamp_min is not None:
        value = max(clamp_min, value)
    if clamp_max is not None:
        value = min(clamp_max, value)
    return value


def _env_int(name: str, default: int, *, min_value: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid int for %s: %s; using default %d", name, raw, default)
        return default
    if min_value is not None:
        value = max(min_value, value)
    return value


def _player_retain_ratio() -> float:
    return _env_float(_PLAYER_RETAIN_ENV, _PLAYER_RETAIN_DEFAULT, clamp_min=0.0, clamp_max=1.0)


def _player_min_per_pos() -> int:
    return _env_int(_PLAYER_MIN_PER_POS_ENV, _PLAYER_MIN_PER_POS_DEFAULT, min_value=1)


class ParallelLineupJobResult:
    def __init__(self, job_id: int, lineups: list[LineupResult], seed: int, error: str | None = None):
        self.job_id = job_id
        self.lineups = lineups
        self.seed = seed
        self.error = error


class ParallelLineupJobConfig:
    def __init__(self, job_id: int, seed: int, records: list[PlayerRecord],
                 site: str, sport: str, n_lineups: int, perturbation: float,
                 max_repeating_players: Optional[int], max_from_one_team: Optional[int],
                 lock_player_ids: Optional[Iterable[str]], exclude_player_ids: Optional[Iterable[str]],
                 max_exposure: Optional[float], min_salary: Optional[int]):
        self.job_id = job_id
        self.seed = seed
        self.records = records
        self.site = site
        self.sport = sport
        self.n_lineups = n_lineups
        self.perturbation = perturbation
        self.max_repeating_players = max_repeating_players
        self.max_from_one_team = max_from_one_team
        self.lock_player_ids = set(lock_player_ids or []) or None
        self.exclude_player_ids = set(exclude_player_ids or []) or None
        self.max_exposure = max_exposure
        self.min_salary = min_salary


class LineupGenerationPartial(Exception):
    def __init__(self, lineups: list[LineupResult], message: str):
        super().__init__(message)
        self.lineups = lineups
        self.message = message


def _configure_solver() -> None:
    global _SOLVER_CONFIGURED
    if _SOLVER_CONFIGURED:
        return

    solver_choice = os.getenv(_SOLVER_ENV, "ortools").lower()
    gap_kwargs: dict[str, float] = {}
    gap_raw = os.getenv(_SOLVER_GAP_ENV)
    if gap_raw:
        try:
            gap_value = float(gap_raw)
            if gap_value > 0:
                gap_kwargs["gapRel"] = gap_value
        except ValueError:
            logger.warning("Invalid solver gap value %s; ignoring", gap_raw)
    else:
        # Default to a small relative gap to allow faster "good enough" solutions
        gap_kwargs["gapRel"] = 0.001

    try:
        from pulp import PULP_CBC_CMD
    except ImportError as exc:  # pragma: no cover - pulp must be installed
        raise RuntimeError("PuLP is required for lineup optimization") from exc

    chosen = None
    solver_label = "CBC"

    if solver_choice in {"ortools", "or-tools", "or_cbc", "pulp_or_cbc"}:
        try:
            from pulp import PULP_OR_CBC_CMD  # type: ignore
            candidate = PULP_OR_CBC_CMD(msg=False, **gap_kwargs)  # type: ignore
            if getattr(candidate, "available", lambda: True)():
                chosen = candidate
                solver_label = "OR-Tools"
            else:
                logger.warning("OR-Tools solver is not available on this system; trying HiGHS")
        except ImportError:
            logger.warning(
                "OR-Tools solver requested but pulp does not expose PULP_OR_CBC_CMD; trying HiGHS",
            )

    if chosen is None and solver_choice in {"highs", "ortools", "hi_gs"}:
        try:
            from pulp.apis.highs_api import HiGHS_CMD
            candidate = HiGHS_CMD(msg=False, **gap_kwargs)
            if getattr(candidate, "available", lambda: True)():
                chosen = candidate
                solver_label = "HiGHS"
            else:
                logger.warning("HiGHS solver unavailable (missing binary); falling back to CBC")
        except ImportError:
            if solver_choice in {"highs", "hi_gs"}:
                logger.warning("HiGHS solver package not available; falling back to CBC")

    if chosen is None:
        chosen = PULP_CBC_CMD(msg=False, **gap_kwargs)
        solver_label = "CBC"

    if "gapRel" in gap_kwargs:
        extra = f" (gapRel={gap_kwargs['gapRel']})"
    else:
        extra = ""

    logger.info("Using %s solver backend%s", solver_label, extra)

    from pydfs_lineup_optimizer.solvers import PuLPSolver

    PuLPSolver.LP_SOLVER = chosen
    _SOLVER_CONFIGURED = True


@dataclass(frozen=True)
class LineupPlayer:
    player_id: str
    name: str
    team: str
    positions: Tuple[str, ...]
    salary: int
    projection: float
    ownership: Optional[float] = None
    baseline_projection: float = 0.0


@dataclass(frozen=True)
class LineupResult:
    lineup_id: str
    players: Tuple[LineupPlayer, ...]
    salary: int
    projection: float
    baseline_projection: float


_SITE_ALIASES = {
    "FD": Site.FANDUEL,
    "FD_SINGLE": Site.FANDUEL_SINGLE_GAME,
    "DK": Site.DRAFTKINGS,
    "DK_CAPTAIN": Site.DRAFTKINGS_CAPTAIN_MODE,
    "YAHOO": Site.YAHOO,
}

_SPORT_ALIASES = {
    "MLB": Sport.BASEBALL,
    "NBA": Sport.BASKETBALL,
    "NFL": Sport.FOOTBALL,
    "NHL": Sport.HOCKEY,
    "WNBA": Sport.WNBA,
}


def _resolve_site(site: str) -> str:
    key = site.upper()
    if key not in _SITE_ALIASES:
        raise KeyError(f"Unsupported site {site!r}")
    return _SITE_ALIASES[key]


def _resolve_sport(sport: str) -> str:
    key = sport.upper()
    if key not in _SPORT_ALIASES:
        raise KeyError(f"Unsupported sport {sport!r}")
    return _SPORT_ALIASES[key]


def _split_name(full_name: str) -> Tuple[str, str]:
    parts = full_name.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _to_pydfs_players(records: Sequence[PlayerRecord]):
    from pydfs_lineup_optimizer.player import Player as PydfsPlayer

    dfs_players: List[PydfsPlayer] = []
    for record in records:
        first, last = _split_name(record.name)
        dfs_players.append(
            PydfsPlayer(
                player_id=record.player_id,
                first_name=first,
                last_name=last,
                positions=list(record.positions) or [""],
                team=record.team,
                salary=float(record.salary),
                fppg=float(record.projection),
                projected_ownership=record.metadata.get("projected_ownership"),
                fppg_floor=record.metadata.get("projection_floor"),
                fppg_ceil=record.metadata.get("projection_ceil"),
            )
        )
    return dfs_players


def _filter_player_pool(
    records: Sequence[PlayerRecord],
    mandatory_ids: Optional[Iterable[str]] = None,
) -> List[PlayerRecord]:
    retain_ratio = _player_retain_ratio()
    min_per_pos = _player_min_per_pos()
    if not records or retain_ratio >= 1.0:
        return list(records)

    mandatory_set = {pid for pid in (mandatory_ids or []) if pid}

    by_position: dict[str, list[PlayerRecord]] = defaultdict(list)
    for record in records:
        if not record.positions:
            continue
        for pos in record.positions:
            by_position[pos].append(record)

    keep_ids: set[str] = set(mandatory_set)
    for pos, players in by_position.items():
        sorted_players = sorted(players, key=lambda r: r.projection, reverse=True)
        keep_count = int(len(sorted_players) * retain_ratio)
        keep_count = max(min_per_pos, keep_count)
        keep_count = min(len(sorted_players), keep_count)
        for record in sorted_players[:keep_count]:
            keep_ids.add(record.player_id)

    if not keep_ids:
        return list(records)

    filtered = [record for record in records if record.player_id in keep_ids or not record.positions]
    if len(filtered) != len(records):
        logger.info(
            "Player pool trimmed from %s to %s (retain %.0f%%, min %s per position, mandatory %s)",
            len(records),
            len(filtered),
            retain_ratio * 100,
            min_per_pos,
            len(mandatory_set),
        )
    else:
        logger.info("Player pool retained full size (%s players)", len(records))
    return filtered


def _perturb_projections(records: Sequence[PlayerRecord], *, seed: int, magnitude: float) -> list[PlayerRecord]:
    """Return a new list of records with projections randomly nudged up/down."""
    if magnitude <= 0:
        return list(records)
    rng = random.Random(seed)
    players = list(records)
    if not players:
        return []
    # Sort descending so higher projection players get smaller perturbation windows.
    players.sort(key=lambda rec: rec.projection, reverse=True)
    highest = players[0].projection if players else 0.0
    lowest = players[-1].projection if players else 0.0
    spread = max(highest - lowest, 1e-6)

    cloned: list[PlayerRecord] = []
    for idx, player in enumerate(players):
        # Normalized rank (0 = highest projection, 1 = lowest).
        relative_rank = idx / max(len(players) - 1, 1)
        # Invert so high projections get smaller magnitude, low projections get larger.
        weight = 1.0 - 0.5 * (1.0 - relative_rank)
        weighted_magnitude = magnitude * weight
        # Symmetric perturbation around the original value.
        offset = rng.uniform(-weighted_magnitude, weighted_magnitude)
        new_projection = max(0.0, player.projection * (1.0 + offset))
        cloned.append(player.model_copy(update={"projection": new_projection}))
    return cloned


def _lineup_signature(lineup: LineupResult) -> tuple[str, ...]:
    return tuple(sorted(player.player_id for player in lineup.players))


def _run_parallel_job(config: "ParallelLineupJobConfig") -> ParallelLineupJobResult:
    perturbed = _perturb_projections(config.records, seed=config.seed, magnitude=config.perturbation)
    try:
        lineups = _build_lineups_serial(
            perturbed,
            site=config.site,
            sport=config.sport,
            n_lineups=config.n_lineups,
            max_repeating_players=config.max_repeating_players,
            max_from_one_team=config.max_from_one_team,
            lock_player_ids=config.lock_player_ids,
            exclude_player_ids=config.exclude_player_ids,
            max_exposure=config.max_exposure,
            min_salary=config.min_salary,
        )
        return ParallelLineupJobResult(config.job_id, lineups, config.seed)
    except LineupGenerationPartial as exc:
        return ParallelLineupJobResult(config.job_id, exc.lineups, config.seed, error=exc.message)


def _parallel_worker(config: "ParallelLineupJobConfig", queue: mp.Queue) -> None:
    try:
        queue.put(_run_parallel_job(config))
    except Exception as exc:  # pragma: no cover - worker errors bubble to parent
        queue.put(exc)


def generate_lineups_parallel(
    *,
    records: Sequence[PlayerRecord],
    site: str,
    sport: str,
    total_lineups: int,
    max_repeating_players: Optional[int] = None,
    max_from_one_team: Optional[int] = None,
    lock_player_ids: Optional[Iterable[str]] = None,
    exclude_player_ids: Optional[Iterable[str]] = None,
    workers: int = 2,
    lineups_per_job: Optional[int] = None,
    perturbation: float = 0.02,
    max_exposure: Optional[float] = None,
    min_salary: Optional[int] = None,
) -> List[LineupResult]:
    """Generate lineups across multiple processes with slight projection perturbations."""

    total_lineups = max(0, total_lineups)
    if total_lineups == 0:
        return []

    workers = max(1, workers)
    records_list = list(records)
    per_job = lineups_per_job or min(50, total_lineups)
    per_job = max(1, per_job)

    results: list[LineupResult] = []
    seen_signatures: set[tuple[str, ...]] = set()
    pool_size = len(records_list)
    position_counts: dict[str, int] = defaultdict(int)
    for record in records_list:
        for pos in record.positions:
            position_counts[pos] += 1
    if position_counts:
        sorted_positions = sorted(position_counts.items(), key=lambda item: item[1])
        most_constrained_pos, constrained_count = sorted_positions[0]
        pos_log = ", ".join(f"{pos}:{count}" for pos, count in sorted(position_counts.items()))
    else:
        most_constrained_pos, constrained_count = ("-", 0)
        pos_log = "-"

    run_start = time.perf_counter()

    logger.info(
        "Starting lineup generation – total=%s, workers=%s, per_job=%s, perturbation=%.3f, max_exposure=%s",
        total_lineups,
        workers,
        per_job,
        perturbation,
        "auto" if max_exposure is None else f"{max_exposure:.3f}",
    )

    def remaining_lineups() -> int:
        return total_lineups - len(results)

    def build_config(job_id: int, batch: int) -> ParallelLineupJobConfig:
        seed = random.randint(1, 2 ** 31 - 1)
        return ParallelLineupJobConfig(
            job_id=job_id,
            seed=seed,
            records=records_list,
            site=site,
            sport=sport,
            n_lineups=batch,
            perturbation=perturbation,
            max_repeating_players=max_repeating_players,
            max_from_one_team=max_from_one_team,
            lock_player_ids=lock_player_ids,
            exclude_player_ids=exclude_player_ids,
            max_exposure=max_exposure,
            min_salary=min_salary,
        )

    def apply_outcome(outcome: ParallelLineupJobResult, batch_start: float) -> tuple[int, int, float]:
        appended = 0
        new_unique = 0
        for lineup in outcome.lineups:
            if remaining_lineups() <= 0:
                break
            results.append(lineup)
            appended += 1
            signature = _lineup_signature(lineup)
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                new_unique += 1
        return appended, new_unique, time.perf_counter() - batch_start

    if workers == 1:
        next_job_id = 0
        partial_message: str | None = None
        while remaining_lineups() > 0:
            batch = min(per_job, remaining_lineups())
            config = build_config(next_job_id, batch)
            logger.info(
                "Sequential batch %s – requesting %s lineups (seed=%s, total %.2fs, pool=%s, positions=%s)",
                config.job_id,
                batch,
                config.seed,
                time.perf_counter() - run_start,
                pool_size,
                pos_log,
            )
            before = len(results)
            batch_start = time.perf_counter()
            outcome = _run_parallel_job(config)
            added, new_unique, batch_elapsed = apply_outcome(outcome, batch_start)
            next_job_id += 1
            logger.info(
                "Sequential batch %s completed – added %s lineups (%s new); total %s/%s (unique %s, total %.2fs, batch %.2fs, pool=%s, positions=%s)",
                outcome.job_id,
                added,
                new_unique,
                len(results),
                total_lineups,
                len(seen_signatures),
                time.perf_counter() - run_start,
                batch_elapsed,
                pool_size,
                pos_log,
            )
            if outcome.error:
                logger.warning("Sequential batch %s stopped early: %s", outcome.job_id, outcome.error)
                partial_message = outcome.error
                break
            if len(results) == before:
                logger.info(
                    "Sequential batch produced no new lineups (job %s, seed %s); stopping early",
                    outcome.job_id,
                    outcome.seed,
                )
                partial_message = "No additional feasible lineups"
                break
        if len(results) < total_lineups:
            raise LineupGenerationPartial(results, partial_message or "Unable to build lineup")
        return results[:total_lineups]

    ctx = mp.get_context('spawn')
    queue: mp.Queue = ctx.Queue()

    processes: dict[int, mp.Process] = {}
    next_job_id = 0

    def start_job(batch: int) -> None:
        nonlocal next_job_id
        if batch <= 0:
            return
        if remaining_lineups() <= 0:
            return
        config = build_config(next_job_id, batch)
        logger.info(
            "Dispatching batch %s – requesting %s lineups (seed=%s, total %.2fs, pool=%s, positions=%s)",
            config.job_id,
            batch,
            config.seed,
            time.perf_counter() - run_start,
            pool_size,
            pos_log,
        )
        proc = ctx.Process(target=_parallel_worker, args=(config, queue))
        proc.start()
        processes[config.job_id] = proc
        next_job_id += 1

    partial_error: str | None = None
    try:
        while len(processes) < workers and remaining_lineups() > 0:
            start_job(min(per_job, remaining_lineups()))

        stop_requested = False
        while processes:
            outcome = queue.get()
            if isinstance(outcome, Exception):
                raise outcome

            proc = processes.pop(outcome.job_id, None)
            if proc is not None:
                proc.join()

            batch_start = time.perf_counter()
            added, new_unique, inner_elapsed = apply_outcome(outcome, batch_start)
            logger.info(
                "Batch %s completed – added %s lineups (%s new); total %s/%s (unique %s, seed=%s, total %.2fs, batch %.2fs, pool=%s, positions=%s)",
                outcome.job_id,
                added,
                new_unique,
                len(results),
                total_lineups,
                len(seen_signatures),
                outcome.seed,
                time.perf_counter() - run_start,
                inner_elapsed,
                pool_size,
                pos_log,
            )

            if outcome.error and partial_error is None:
                logger.warning("Batch %s stopped early: %s", outcome.job_id, outcome.error)
                partial_error = outcome.error
                stop_requested = True
                break

            if remaining_lineups() <= 0:
                stop_requested = True
                break

            while len(processes) < workers and remaining_lineups() > 0:
                start_job(min(per_job, remaining_lineups()))
        if stop_requested:
            for proc in processes.values():
                if proc.is_alive():
                    proc.terminate()
                proc.join()
        else:
            for proc in processes.values():
                proc.join()
    finally:
        for proc in processes.values():
            if proc.is_alive():
                proc.terminate()

    if partial_error or remaining_lineups() > 0:
        raise LineupGenerationPartial(results, partial_error or "Unable to build lineup")
    return results[:total_lineups]


def _lineup_to_result(lineup: Lineup, idx: int, baseline_lookup: Mapping[str, float]) -> LineupResult:
    players = tuple(
        LineupPlayer(
            player_id=p.id,
            name=f"{p.first_name} {p.last_name}".strip(),
            team=p.team,
            positions=tuple(p.positions),
            salary=int(p.salary),
            projection=float(p.fppg),
            ownership=getattr(p, "projected_ownership", None),
            baseline_projection=baseline_lookup.get(p.id, float(p.fppg)),
        )
        for p in lineup.players
    )
    baseline_projection = sum(player.baseline_projection for player in players)
    return LineupResult(
        lineup_id=f"L{idx + 1:03}",
        players=players,
        salary=int(lineup.salary_costs),
        projection=float(lineup.fantasy_points_projection),
        baseline_projection=float(baseline_projection),
    )


def _build_lineups_serial(
    records: Sequence[PlayerRecord],
    *,
    site: str,
    sport: str,
    n_lineups: int = 20,
    max_repeating_players: Optional[int] = None,
    max_from_one_team: Optional[int] = None,
    lock_player_ids: Optional[Iterable[str]] = None,
    exclude_player_ids: Optional[Iterable[str]] = None,
    max_exposure: Optional[float] = None,
    min_salary: Optional[int] = None,
) -> List[LineupResult]:
    """Generate lineups from the supplied player pool."""

    _configure_solver()

    active_records = _filter_player_pool(list(records), mandatory_ids=lock_player_ids)
    optimizer = get_optimizer(_resolve_site(site), _resolve_sport(sport))
    if min_salary is not None and hasattr(optimizer, "min_salary_cap"):
        optimizer.min_salary_cap = min_salary
    logger.info(
        "Optimizer salary caps – max=%s min=%s",
        getattr(optimizer, "max_salary", None),
        getattr(optimizer, "min_salary_cap", None),
    )

    dfs_players = _to_pydfs_players(active_records)
    optimizer.player_pool.load_players(dfs_players)

    pool = optimizer.player_pool

    if lock_player_ids:
        for pid in lock_player_ids:
            player = pool.get_player_by_id(pid)
            if player is not None:
                pool.lock_player(player)

    if exclude_player_ids:
        for pid in exclude_player_ids:
            player = pool.get_player_by_id(pid)
            if player is not None:
                pool.remove_player(player)

    if max_repeating_players is not None:
        optimizer.max_repeating_players = max_repeating_players

    if max_from_one_team is not None:
        optimizer.set_players_from_one_team(max_from_one_team)

    baseline_lookup = {
        record.player_id: float(record.metadata.get("baseline_projection", record.projection))
        for record in active_records
    }

    results: List[LineupResult] = []
    start_time = time.perf_counter()
    try:
        for idx, lineup in enumerate(optimizer.optimize(n_lineups, max_exposure=max_exposure)):
            result = _lineup_to_result(lineup, idx, baseline_lookup)
            results.append(result)
            elapsed = time.perf_counter() - start_time
            avg = elapsed / (idx + 1)
            logger.info(
                "Built lineup %s/%s – projection %.2f, salary %s (elapsed %.2fs, avg %.2fs)",
                idx + 1,
                n_lineups,
                result.projection,
                result.salary,
                elapsed,
                avg,
            )
    except LineupOptimizerException as exc:
        if results:
            logger.warning("Lineup optimization stopped early after %s/%s lineups: %s", len(results), n_lineups, exc)
            raise LineupGenerationPartial(results, str(exc)) from exc
        raise

    total_elapsed = time.perf_counter() - start_time
    if results:
        logger.info(
            "Completed %s lineups in %.2fs (avg %.2fs per lineup)",
            len(results),
            total_elapsed,
            total_elapsed / len(results),
        )
    return results


def build_lineups(
    records: Sequence[PlayerRecord],
    *,
    site: str,
    sport: str,
    n_lineups: int = 20,
    max_repeating_players: Optional[int] = None,
    max_from_one_team: Optional[int] = None,
    lock_player_ids: Optional[Iterable[str]] = None,
    exclude_player_ids: Optional[Iterable[str]] = None,
    parallel_jobs: int = 1,
    perturbation: float = 0.0,
    lineups_per_job: int | None = None,
    max_exposure: Optional[float] = 0.5,
    min_salary: Optional[int] = None,
) -> List[LineupResult]:
    """Generate lineups, optionally distributing work across processes."""

    workers = max(1, parallel_jobs)
    per_job = lineups_per_job
    if per_job is None and n_lineups > 50:
        per_job = min(50, n_lineups)

    use_parallel = (
        workers > 1
        or (per_job is not None and per_job < n_lineups)
        or perturbation > 0.0
    )

    try:
        if not use_parallel:
            return _build_lineups_serial(
                records,
                site=site,
                sport=sport,
                n_lineups=n_lineups,
                max_repeating_players=max_repeating_players,
                max_from_one_team=max_from_one_team,
                lock_player_ids=lock_player_ids,
                exclude_player_ids=exclude_player_ids,
                max_exposure=max_exposure,
                min_salary=min_salary,
            )

        return generate_lineups_parallel(
            records=records,
            site=site,
            sport=sport,
            total_lineups=n_lineups,
            max_repeating_players=max_repeating_players,
            max_from_one_team=max_from_one_team,
            lock_player_ids=lock_player_ids,
            exclude_player_ids=exclude_player_ids,
            workers=workers,
            lineups_per_job=per_job,
            perturbation=perturbation,
            max_exposure=max_exposure,
            min_salary=min_salary,
        )
    except LineupGenerationPartial as exc:
        exc.lineups = exc.lineups[:n_lineups]
        raise
