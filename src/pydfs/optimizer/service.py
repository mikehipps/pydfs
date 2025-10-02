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
_EXPOSURE_BIAS_DEFAULT_TARGET = 0.4
_EXPOSURE_BIAS_WARMUP_LINEUPS = 25


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
                 site: str, sport: str, n_lineups: int, perturbation_p25: float, perturbation_p75: float,
                 max_repeating_players: Optional[int], max_from_one_team: Optional[int],
                 lock_player_ids: Optional[Iterable[str]], exclude_player_ids: Optional[Iterable[str]],
                 max_exposure: Optional[float], min_salary: Optional[int], bias_factors: Optional[dict[str, float]] = None):
        self.job_id = job_id
        self.seed = seed
        self.records = records
        self.site = site
        self.sport = sport
        self.n_lineups = n_lineups
        self.perturbation_p25 = max(0.0, perturbation_p25)
        self.perturbation_p75 = max(0.0, perturbation_p75)
        self.max_repeating_players = max_repeating_players
        self.max_from_one_team = max_from_one_team
        self.lock_player_ids = set(lock_player_ids or []) or None
        self.exclude_player_ids = set(exclude_player_ids or []) or None
        self.max_exposure = max_exposure
        self.min_salary = min_salary
        self.bias_factors = bias_factors or {}


class LineupGenerationPartial(Exception):
    def __init__(self, lineups: list[LineupResult], message: str, bias_summary: dict | None = None):
        super().__init__(message)
        self.lineups = lineups
        self.message = message
        self.bias_summary = bias_summary or {}


@dataclass
class BuildOutput:
    lineups: List[LineupResult]
    bias_summary: dict | None = None


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


def _apply_bias_to_records(
    records: Sequence[PlayerRecord],
    bias_factors: Mapping[str, float],
) -> list[PlayerRecord]:
    if not bias_factors:
        return list(records)

    biased_records: list[PlayerRecord] = []
    for record in records:
        bias = float(bias_factors.get(record.player_id, 1.0))
        bias = max(0.0, bias)
        new_projection = max(0.0, record.projection * bias)
        metadata = dict(record.metadata)
        metadata.setdefault("baseline_projection", float(metadata.get("baseline_projection", record.projection)))
        metadata["bias_factor"] = bias
        metadata["biased_projection"] = new_projection
        biased_records.append(
            record.model_copy(update={"projection": new_projection, "metadata": metadata})
        )
    return biased_records


def _perturbation_window(percentile: float, pct25: float, pct75: float) -> float:
    """Return the max fractional perturbation for a given percentile."""
    if percentile <= 0.25:
        t = percentile / 0.25 if 0.25 else 0.0
        return pct25 * (1.5 - 0.5 * t)
    if percentile >= 0.75:
        t = (percentile - 0.75) / 0.25 if 0.25 else 0.0
        return pct75 * (1.0 - 0.5 * t)
    t = (percentile - 0.25) / 0.5 if 0.5 else 0.0
    return pct25 + (pct75 - pct25) * t


def _perturb_projections(
    records: Sequence[PlayerRecord],
    *,
    seed: int,
    percentile_25: float,
    percentile_75: float,
) -> list[PlayerRecord]:
    """Return a new list of records with projections randomly nudged up/down."""

    pct25 = max(0.0, percentile_25 / 100.0)
    pct75 = max(0.0, percentile_75 / 100.0)
    if max(pct25, pct75) <= 0:
        return list(records)

    rng = random.Random(seed)
    players = list(records)
    if not players:
        return []

    indexed_players = list(enumerate(players))
    sorted_pairs = sorted(indexed_players, key=lambda item: item[1].projection)
    max_rank = max(len(players) - 1, 1)

    perturbation_windows: dict[int, float] = {}
    for rank, (original_index, player) in enumerate(sorted_pairs):
        percentile = rank / max_rank
        perturbation_windows[original_index] = max(0.0, _perturbation_window(percentile, pct25, pct75))

    cloned: list[PlayerRecord] = []
    for idx, player in enumerate(players):
        magnitude = perturbation_windows.get(idx, 0.0)
        if magnitude <= 0.0:
            cloned.append(player.model_copy())
            continue
        offset = rng.uniform(-magnitude, magnitude)
        offset = max(-0.99, min(0.99, offset))
        new_projection = max(0.0, player.projection * (1.0 + offset))
        cloned.append(player.model_copy(update={"projection": new_projection}))
    return cloned


def _lineup_signature(lineup: LineupResult) -> tuple[str, ...]:
    return tuple(sorted(player.player_id for player in lineup.players))


def _run_parallel_job(config: "ParallelLineupJobConfig") -> ParallelLineupJobResult:
    perturbed = _perturb_projections(config.records, seed=config.seed, percentile_25=config.perturbation_p25, percentile_75=config.perturbation_p75)
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


def _normalize_percentage(value: float | None) -> float | None:
    if value is None:
        return None
    if value < 0:
        return 0.0
    return value if value <= 1.0 else value / 100.0


def _summarize_bias(
    bias_map: Mapping[str, float],
    *,
    bias_target: float,
    bias_strength: float,
    lineups_tracked: int,
) -> dict | None:
    if not bias_map:
        return None
    factors = list(bias_map.values())
    if not factors:
        return None
    return {
        "min_factor": min(factors),
        "max_factor": max(factors),
        "target_percent": bias_target,
        "strength_percent": bias_strength * 100.0,
        "lineups_tracked": lineups_tracked,
        "factors": dict(bias_map),
    }


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
    perturbation: float | None = None,
    perturbation_p25: float | None = None,
    perturbation_p75: float | None = None,
    max_exposure: Optional[float] = None,
    min_salary: Optional[int] = None,
    exposure_bias: float | None = None,
    exposure_bias_target: float | None = None,
    ) -> BuildOutput:
    """Generate lineups across multiple processes with slight projection perturbations."""

    total_lineups = max(0, total_lineups)
    if total_lineups == 0:
        return []

    # Backwards compatibility: single perturbation value acts as both percentile bounds.
    base = perturbation if perturbation is not None else 0.0
    base = max(0.0, base)
    if base <= 1.0:
        base_pct = base * 100.0
    else:
        base_pct = base
    p25_value = perturbation_p25 if perturbation_p25 is not None else base_pct
    if p25_value <= 1.0:
        p25_value *= 100.0
    p75_value = perturbation_p75 if perturbation_p75 is not None else p25_value
    if p75_value <= 1.0:
        p75_value *= 100.0

    workers = max(1, workers)
    records_list = list(records)
    per_job = lineups_per_job or min(50, total_lineups)
    per_job = max(1, per_job)

    results: list[LineupResult] = []
    seen_signatures: set[tuple[str, ...]] = set()
    pool_size = len(records_list)
    bias_strength = _normalize_percentage(exposure_bias) or 0.0
    bias_strength = min(max(bias_strength, 0.0), 0.9)
    bias_target_input = _normalize_percentage(exposure_bias_target)
    if bias_target_input is not None:
        bias_target = max(0.01, min(1.0, bias_target_input))
    elif max_exposure is not None:
        bias_target = max(0.01, min(1.0, max_exposure))
    else:
        bias_target = _EXPOSURE_BIAS_DEFAULT_TARGET
    usage_counts: defaultdict[str, int] = defaultdict(int)
    last_bias_snapshot: dict[str, float] = {}

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
        "Starting lineup generation – total=%s, workers=%s, per_job=%s, perturbation_p25=%.1f%%, perturbation_p75=%.1f%%, max_exposure=%s",
        total_lineups,
        workers,
        per_job,
        p25_value,
        p75_value,
        "auto" if max_exposure is None else f"{max_exposure:.3f}",
    )

    def remaining_lineups() -> int:
        return total_lineups - len(results)

    def compute_bias_map() -> dict[str, float]:
        if bias_strength <= 0.0:
            return {}
        total = len(results)
        if total <= 0:
            return {}
        target = bias_target
        if target <= 0:
            return {}
        clamp_min = max(0.0, 1.0 - bias_strength)
        clamp_max = 1.0 + bias_strength
        warmup_scale = min(1.0, total / _EXPOSURE_BIAS_WARMUP_LINEUPS)
        bias_map: dict[str, float] = {}
        for record in records_list:
            exposure = usage_counts.get(record.player_id, 0) / total
            delta = target - exposure
            adjust_ratio = delta / target
            factor = 1.0 + adjust_ratio * bias_strength * warmup_scale
            if factor < clamp_min:
                factor = clamp_min
            elif factor > clamp_max:
                factor = clamp_max
            bias_map[record.player_id] = factor
        return bias_map

    def build_config(job_id: int, batch: int, records_for_job: Optional[Sequence[PlayerRecord]] = None, bias_map: Optional[dict[str, float]] = None) -> ParallelLineupJobConfig:
        seed = random.randint(1, 2 ** 31 - 1)
        return ParallelLineupJobConfig(
            job_id=job_id,
            seed=seed,
            records=list(records_for_job) if records_for_job is not None else list(records_list),
            site=site,
            sport=sport,
            n_lineups=batch,
            perturbation_p25=p25_value,
            perturbation_p75=p75_value,
            max_repeating_players=max_repeating_players,
            max_from_one_team=max_from_one_team,
            lock_player_ids=lock_player_ids,
            exclude_player_ids=exclude_player_ids,
            max_exposure=max_exposure,
            min_salary=min_salary,
            bias_factors=bias_map,
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
            if bias_strength > 0.0:
                for player in lineup.players:
                    usage_counts[player.player_id] += 1
        return appended, new_unique, time.perf_counter() - batch_start

    bias_summary: dict | None = None

    if workers == 1:
        next_job_id = 0
        partial_message: str | None = None
        while remaining_lineups() > 0:
            batch = min(per_job, remaining_lineups())
            bias_map = compute_bias_map() if bias_strength > 0.0 else {}
            records_override = None
            if bias_map:
                records_override = _apply_bias_to_records(records_list, bias_map)
                last_bias_snapshot = bias_map
            config = build_config(next_job_id, batch, records_override, bias_map if bias_map else None)
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
        bias_summary = _summarize_bias(
            last_bias_snapshot,
            bias_target=bias_target,
            bias_strength=bias_strength,
            lineups_tracked=len(results),
        ) if bias_strength > 0.0 else None
        if len(results) < total_lineups:
            raise LineupGenerationPartial(results, partial_message or "Unable to build lineup", bias_summary)
        if bias_summary:
            logger.info(
                "Final bias factors range %.3f – %.3f (target %.2f)",
                bias_summary["min_factor"],
                bias_summary["max_factor"],
                bias_summary["target_percent"],
            )
        return BuildOutput(results[:total_lineups], bias_summary)

    ctx = mp.get_context('spawn')
    queue: mp.Queue = ctx.Queue()

    processes: dict[int, mp.Process] = {}
    next_job_id = 0

    def start_job(batch: int) -> None:
        nonlocal next_job_id, last_bias_snapshot
        if batch <= 0:
            return
        if remaining_lineups() <= 0:
            return
        bias_map = compute_bias_map() if bias_strength > 0.0 else {}
        records_override = None
        if bias_map:
            records_override = _apply_bias_to_records(records_list, bias_map)
            last_bias_snapshot = bias_map
        config = build_config(next_job_id, batch, records_override, bias_map if bias_map else None)
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

    bias_summary = _summarize_bias(
        last_bias_snapshot,
        bias_target=bias_target,
        bias_strength=bias_strength,
        lineups_tracked=len(results),
    ) if bias_strength > 0.0 else None
    if partial_error or remaining_lineups() > 0:
        raise LineupGenerationPartial(results, partial_error or "Unable to build lineup", bias_summary)
    if bias_summary:
        logger.info(
            "Final bias factors range %.3f – %.3f (target %.2f)",
            bias_summary["min_factor"],
            bias_summary["max_factor"],
            bias_summary["target_percent"],
        )
    return BuildOutput(results[:total_lineups], bias_summary)


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
    perturbation: float | None = None,
    perturbation_p25: float | None = None,
    perturbation_p75: float | None = None,
    lineups_per_job: int | None = None,
    max_exposure: Optional[float] = 0.5,
    min_salary: Optional[int] = None,
    exposure_bias: float | None = None,
    exposure_bias_target: float | None = None,
) -> BuildOutput:
    """Generate lineups, optionally distributing work across processes."""

    workers = max(1, parallel_jobs)
    per_job = lineups_per_job
    if per_job is None and n_lineups > 50:
        per_job = min(50, n_lineups)

    base = perturbation if perturbation is not None else 0.0
    base_pct = base * 100.0 if base <= 1.0 else base
    p25_value = perturbation_p25 if perturbation_p25 is not None else base_pct
    p25_value = p25_value * 100.0 if p25_value <= 1.0 else p25_value
    p75_value = perturbation_p75 if perturbation_p75 is not None else p25_value
    p75_value = p75_value * 100.0 if p75_value <= 1.0 else p75_value

    bias_strength = _normalize_percentage(exposure_bias) or 0.0

    use_parallel = (
        workers > 1
        or (per_job is not None and per_job < n_lineups)
        or p25_value > 0.0
        or p75_value > 0.0
        or bias_strength > 0.0
    )

    try:
        if not use_parallel:
            lineups = _build_lineups_serial(
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
            return BuildOutput(lineups, None)

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
            perturbation_p25=p25_value,
            perturbation_p75=p75_value,
            max_exposure=max_exposure,
            min_salary=min_salary,
            exposure_bias=exposure_bias,
            exposure_bias_target=exposure_bias_target,
        )
    except LineupGenerationPartial as exc:
        exc.lineups = exc.lineups[:n_lineups]
        raise
