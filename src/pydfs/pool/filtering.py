"""Helpers for slicing lineup pools by common metrics."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import fmean, median, pstdev
from typing import Iterable, Literal, Sequence

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


def filter_lineups(
    candidates: Sequence[LineupCandidate],
    criteria: FilterCriteria,
) -> FilterResult:
    """Filter lineups and return ordered selections with summary statistics."""

    filtered = [candidate for candidate in candidates if _passes_criteria(candidate, criteria)]
    available = len(filtered)

    reverse = criteria.sort_direction != "asc"
    filtered.sort(key=lambda c: (_sort_key(c, criteria), c.lineup.lineup_id), reverse=reverse)

    if criteria.limit is not None and criteria.limit > 0:
        filtered = filtered[: criteria.limit]

    ranked: list[FilteredLineup] = [
        FilteredLineup(candidate=candidate, rank=index)
        for index, candidate in enumerate(filtered, start=1)
    ]

    total_instances = sum(item.candidate.count for item in ranked)

    baselines = [item.candidate.baseline for item in ranked]
    projections = [item.candidate.projection for item in ranked]
    usage_values = [item.candidate.usage_sum for item in ranked]
    uniqueness_values = [item.candidate.uniqueness for item in ranked]

    def _safe_stats(values: Iterable[float]) -> tuple[float | None, float | None, float | None]:
        values = list(values)
        if not values:
            return None, None, None
        mean = fmean(values)
        med = median(values)
        std = pstdev(values) if len(values) > 1 else 0.0
        return mean, med, std

    baseline_mean, baseline_median, baseline_std = _safe_stats(baselines)
    projection_mean, _, _ = _safe_stats(projections)
    usage_mean, _, _ = _safe_stats(usage_values)
    uniqueness_mean, _, _ = _safe_stats(uniqueness_values)

    summary = FilterSummary(
        available_lineups=available,
        selected_lineups=len(ranked),
        total_instances=total_instances,
        baseline_mean=baseline_mean,
        baseline_median=baseline_median,
        baseline_std=baseline_std,
        projection_mean=projection_mean,
        usage_mean=usage_mean,
        uniqueness_mean=uniqueness_mean,
    )

    return FilterResult(lineups=ranked, summary=summary)


__all__ = [
    "FilterCriteria",
    "FilteredLineup",
    "FilterSummary",
    "FilterResult",
    "LineupCandidate",
    "filter_lineups",
]
