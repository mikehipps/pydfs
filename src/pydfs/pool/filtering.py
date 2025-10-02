"""Helpers for slicing lineup pools by common metrics."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from statistics import fmean, median, pstdev
from typing import Iterable, Literal, Mapping, Sequence

from pydfs.api.schemas.lineup import LineupResponse


@dataclass(frozen=True)
class LineupCandidate:
    """Single unique lineup with aggregated metrics."""

    signature: tuple[str, ...]
    lineup: LineupResponse
    count: int
    run_ids: tuple[str, ...]
    salary: int
    projection: float
    baseline: float
    usage_sum: float
    uniqueness: float
    baseline_percentile: float
    usage_percentile: float
    uniqueness_percentile: float


@dataclass(frozen=True)
class FilterCriteria:
    """Filtering configuration for lineup pools."""

    min_baseline: float | None = None
    max_baseline: float | None = None
    min_projection: float | None = None
    max_projection: float | None = None
    min_salary: int | None = None
    max_salary: int | None = None
    min_usage_sum: float | None = None
    max_usage_sum: float | None = None
    min_uniqueness: float | None = None
    max_uniqueness: float | None = None
    include_player_ids: tuple[str, ...] = ()
    exclude_player_ids: tuple[str, ...] = ()
    include_team_codes: tuple[str, ...] = ()
    exclude_team_codes: tuple[str, ...] = ()
    limit: int | None = None
    sort_by: Literal["baseline", "projection", "salary", "usage", "uniqueness"] = "baseline"
    sort_direction: Literal["asc", "desc"] = "desc"
    max_player_exposure: float | None = None
    player_exposure_caps: tuple[tuple[str, float], ...] = ()


@dataclass(frozen=True)
class FilteredLineup:
    """Lineup returned from a filter operation."""

    candidate: LineupCandidate
    rank: int


@dataclass(frozen=True)
class FilterSummary:
    """Aggregate stats for a filtered lineup selection."""

    available_lineups: int
    selected_lineups: int
    total_instances: int
    baseline_mean: float | None
    baseline_median: float | None
    baseline_std: float | None
    projection_mean: float | None
    usage_mean: float | None
    uniqueness_mean: float | None


@dataclass(frozen=True)
class FilterResult:
    """Container for filtered lineups and summary statistics."""

    lineups: list[FilteredLineup]
    summary: FilterSummary
    pool_summary: FilterSummary


def _passes_criteria(candidate: LineupCandidate, criteria: FilterCriteria) -> bool:
    lineup = candidate.lineup
    if criteria.min_baseline is not None and candidate.baseline < criteria.min_baseline:
        return False
    if criteria.max_baseline is not None and candidate.baseline > criteria.max_baseline:
        return False
    if criteria.min_projection is not None and candidate.projection < criteria.min_projection:
        return False
    if criteria.max_projection is not None and candidate.projection > criteria.max_projection:
        return False
    if criteria.min_salary is not None and lineup.salary < criteria.min_salary:
        return False
    if criteria.max_salary is not None and lineup.salary > criteria.max_salary:
        return False
    if criteria.min_usage_sum is not None and candidate.usage_sum < criteria.min_usage_sum:
        return False
    if criteria.max_usage_sum is not None and candidate.usage_sum > criteria.max_usage_sum:
        return False
    if criteria.min_uniqueness is not None and candidate.uniqueness < criteria.min_uniqueness:
        return False
    if criteria.max_uniqueness is not None and candidate.uniqueness > criteria.max_uniqueness:
        return False

    player_ids = {player.player_id for player in lineup.players}
    if criteria.include_player_ids and not set(criteria.include_player_ids).issubset(player_ids):
        return False
    if criteria.exclude_player_ids and player_ids.intersection(criteria.exclude_player_ids):
        return False

    team_codes = {player.team for player in lineup.players}
    if criteria.include_team_codes and not set(criteria.include_team_codes).issubset(team_codes):
        return False
    if criteria.exclude_team_codes and team_codes.intersection(criteria.exclude_team_codes):
        return False

    return True


def _sort_key(candidate: LineupCandidate, criteria: FilterCriteria) -> float:
    if criteria.sort_by == "projection":
        return candidate.projection
    if criteria.sort_by == "salary":
        return float(candidate.salary)
    if criteria.sort_by == "usage":
        return candidate.usage_sum
    if criteria.sort_by == "uniqueness":
        return candidate.uniqueness
    # Default to baseline projection
    return candidate.baseline


def _build_summary(
    *,
    available: Sequence[LineupCandidate],
    selected: Sequence[LineupCandidate],
) -> FilterSummary:
    def _safe_stats(values: Iterable[float]) -> tuple[float | None, float | None, float | None]:
        values = list(values)
        if not values:
            return None, None, None
        mean = fmean(values)
        med = median(values)
        std = pstdev(values) if len(values) > 1 else 0.0
        return mean, med, std

    baselines = [candidate.baseline for candidate in selected]
    projections = [candidate.projection for candidate in selected]
    usage_values = [candidate.usage_sum for candidate in selected]
    uniqueness_values = [candidate.uniqueness for candidate in selected]

    baseline_mean, baseline_median, baseline_std = _safe_stats(baselines)
    projection_mean, _, _ = _safe_stats(projections)
    usage_mean, _, _ = _safe_stats(usage_values)
    uniqueness_mean, _, _ = _safe_stats(uniqueness_values)

    return FilterSummary(
        available_lineups=len(available),
        selected_lineups=len(selected),
        total_instances=sum(candidate.count for candidate in selected),
        baseline_mean=baseline_mean,
        baseline_median=baseline_median,
        baseline_std=baseline_std,
        projection_mean=projection_mean,
        usage_mean=usage_mean,
        uniqueness_mean=uniqueness_mean,
    )


def filter_lineups(
    candidates: Sequence[LineupCandidate],
    criteria: FilterCriteria,
) -> FilterResult:
    """Filter lineups and return ordered selections with summary statistics."""

    candidate_list = list(candidates)
    filtered_candidates = [
        candidate for candidate in candidate_list if _passes_criteria(candidate, criteria)
    ]

    reverse = criteria.sort_direction != "asc"
    filtered_candidates.sort(
        key=lambda c: (_sort_key(c, criteria), c.lineup.lineup_id), reverse=reverse
    )

    selected_candidates = filtered_candidates
    limit = criteria.limit if criteria.limit is not None and criteria.limit > 0 else None

    global_cap = criteria.max_player_exposure
    player_caps = dict(criteria.player_exposure_caps)

    if global_cap is not None or player_caps:
        exposure_counts: Counter[str] = Counter()
        target_total = limit or len(filtered_candidates) or 0
        target_total = max(target_total, 1)
        selected_candidates = []
        for candidate in filtered_candidates:
            if limit is not None and len(selected_candidates) >= limit:
                break

            if _violates_cap_limit(
                candidate,
                exposure_counts,
                global_cap,
                player_caps,
                target_total,
            ):
                continue

            selected_candidates.append(candidate)
            for player in candidate.lineup.players:
                exposure_counts[player.player_id] += 1

        selected_candidates = _enforce_final_caps(
            selected_candidates,
            global_cap,
            player_caps,
        )
    elif limit is not None:
        selected_candidates = filtered_candidates[:limit]

    ranked: list[FilteredLineup] = [
        FilteredLineup(candidate=candidate, rank=index)
        for index, candidate in enumerate(selected_candidates, start=1)
    ]

    filtered_summary = _build_summary(
        available=filtered_candidates,
        selected=selected_candidates,
    )
    pool_summary = _build_summary(
        available=candidate_list,
        selected=candidate_list,
    )

    return FilterResult(lineups=ranked, summary=filtered_summary, pool_summary=pool_summary)


def _violates_cap_limit(
    candidate: LineupCandidate,
    counts: Counter[str],
    global_cap: float | None,
    player_caps: Mapping[str, float],
    target_total: int,
) -> bool:
    if target_total <= 0:
        return False

    for player in candidate.lineup.players:
        cap = player_caps.get(player.player_id, global_cap)
        if cap is None:
            continue
        allowed = cap * target_total
        if counts[player.player_id] + 1 > allowed + 1e-9:
            return True
    return False


def _enforce_final_caps(
    selected: list[LineupCandidate],
    global_cap: float | None,
    player_caps: Mapping[str, float],
) -> list[LineupCandidate]:
    if not selected:
        return selected

    while selected:
        counts: Counter[str] = Counter()
        for candidate in selected:
            for player in candidate.lineup.players:
                counts[player.player_id] += 1

        total = len(selected)
        violation_player: str | None = None
        for player_id, count in counts.items():
            cap = player_caps.get(player_id, global_cap)
            if cap is None:
                continue
            allowed = cap * total
            if count > allowed + 1e-9:
                violation_player = player_id
                break

        if violation_player is None:
            break

        removed = False
        for idx in range(len(selected) - 1, -1, -1):
            lineup = selected[idx]
            if any(player.player_id == violation_player for player in lineup.lineup.players):
                selected.pop(idx)
                removed = True
                break

        if not removed:
            break

    return selected


__all__ = [
    "FilterCriteria",
    "FilteredLineup",
    "FilterSummary",
    "FilterResult",
    "LineupCandidate",
    "filter_lineups",
]
