import random

import pytest

from pydfs.models import PlayerRecord
from pydfs.optimizer import build_lineups
from pydfs.optimizer.service import _perturb_projections, _perturbation_window, _apply_bias_to_records


def _sample_pool() -> list[PlayerRecord]:
    return [
        PlayerRecord(player_id="p1", name="Pitcher One", team="NYM", positions=["P"], salary=9000, projection=20.0),
        PlayerRecord(player_id="p2", name="Pitcher Two", team="ATL", positions=["P"], salary=8800, projection=18.5),
        PlayerRecord(player_id="c1", name="Catcher One", team="LAD", positions=["C"], salary=2800, projection=9.2),
        PlayerRecord(player_id="c2", name="Catcher Two", team="ATL", positions=["C"], salary=2700, projection=7.0),
        PlayerRecord(player_id="b21", name="Second One", team="ATL", positions=["2B"], salary=3100, projection=9.0),
        PlayerRecord(player_id="b22", name="Second Two", team="BOS", positions=["2B"], salary=2900, projection=8.2),
        PlayerRecord(player_id="b31", name="Third One", team="BOS", positions=["3B"], salary=3200, projection=9.3),
        PlayerRecord(player_id="b32", name="Third Two", team="NYM", positions=["3B"], salary=3100, projection=8.1),
        PlayerRecord(player_id="ss1", name="Short One", team="NYM", positions=["SS"], salary=3300, projection=9.0),
        PlayerRecord(player_id="ss2", name="Short Two", team="ATL", positions=["SS"], salary=3400, projection=8.4),
        PlayerRecord(player_id="of1", name="Out One", team="ATL", positions=["OF"], salary=3600, projection=10.1),
        PlayerRecord(player_id="of2", name="Out Two", team="NYM", positions=["OF"], salary=3500, projection=9.6),
        PlayerRecord(player_id="of3", name="Out Three", team="LAD", positions=["OF"], salary=3400, projection=9.3),
        PlayerRecord(player_id="of4", name="Out Four", team="BOS", positions=["OF"], salary=3300, projection=8.9),
        PlayerRecord(player_id="ut1", name="Utility One", team="BOS", positions=["1B"], salary=3000, projection=9.1),
        PlayerRecord(player_id="ut2", name="Utility Two", team="ATL", positions=["OF"], salary=3100, projection=8.5),
        PlayerRecord(player_id="ut3", name="Utility Three", team="LAD", positions=["C/1B"], salary=2950, projection=8.3),
    ]


def test_build_lineups_generates_lineup():
    output = build_lineups(
        _sample_pool(),
        site="FD",
        sport="MLB",
        n_lineups=1,
    )
    lineups = output.lineups
    assert len(lineups) == 1
    lineup = lineups[0]
    assert lineup.lineup_id == "L001"
    assert len(lineup.players) == 9
    total_salary = sum(player.salary for player in lineup.players)
    assert total_salary <= 35_000
    assert output.bias_summary is None


def test_build_lineups_respects_locks_and_excludes():
    pool = _sample_pool()
    locked = {"p2"}
    excluded = {"p1"}

    output = build_lineups(
        pool,
        site="FD",
        sport="MLB",
        n_lineups=1,
        lock_player_ids=locked,
        exclude_player_ids=excluded,
    )

    lineup = output.lineups[0]

    ids = {player.player_id for player in lineup.players}
    assert "p1" not in ids
    assert "p2" in ids


def test_perturbation_window_shapes_variance():
    low = _perturbation_window(0.25, 0.4, 0.1)
    assert pytest.approx(low, rel=1e-6) == 0.4

    bottom = _perturbation_window(0.0, 0.4, 0.1)
    assert pytest.approx(bottom, rel=1e-6) == 0.4 * 1.5

    mid = _perturbation_window(0.5, 0.4, 0.1)
    assert pytest.approx(mid, rel=1e-6) == 0.25

    top = _perturbation_window(0.75, 0.4, 0.1)
    assert pytest.approx(top, rel=1e-6) == 0.1

    summit = _perturbation_window(1.0, 0.4, 0.1)
    assert pytest.approx(summit, rel=1e-6) == 0.1 * 0.5


def test_perturb_projections_respects_percentiles():
    records = [
        PlayerRecord(player_id=f"p{i}", name=f"Player {i}", team="TEAM", positions=["UTIL"], salary=5000 + i, projection=float(i))
        for i in range(1, 6)
    ]

    perturbed = _perturb_projections(records, seed=17, percentile_25=40.0, percentile_75=10.0)
    assert len(perturbed) == len(records)

    rng = random.Random(17)
    indexed = list(enumerate(records))
    sorted_pairs = sorted(indexed, key=lambda item: item[1].projection)
    max_rank = max(len(records) - 1, 1)
    expected_windows = {}
    for rank, (original_index, player) in enumerate(sorted_pairs):
        percentile = rank / max_rank
        expected_windows[original_index] = _perturbation_window(percentile, 0.4, 0.1)

    for idx, (original, updated) in enumerate(zip(records, perturbed)):
        window = expected_windows[idx]
        if window <= 0:
            assert pytest.approx(updated.projection, rel=1e-9) == original.projection
            continue
        expected_offset = rng.uniform(-window, window)
        expected_offset = max(-0.99, min(0.99, expected_offset))
        actual_offset = (updated.projection / original.projection) - 1.0
        assert pytest.approx(actual_offset, rel=1e-9) == expected_offset


def test_apply_bias_to_records_adjusts_projection():
    records = [
        PlayerRecord(player_id="p1", name="One", team="A", positions=["UTIL"], salary=5000, projection=10.0),
        PlayerRecord(player_id="p2", name="Two", team="A", positions=["UTIL"], salary=5200, projection=8.0),
    ]
    bias_map = {"p1": 1.2, "p2": 0.8}
    biased = _apply_bias_to_records(records, bias_map)
    assert biased[0].projection == pytest.approx(12.0)
    assert biased[1].projection == pytest.approx(6.4)
    assert biased[0].metadata["bias_factor"] == pytest.approx(1.2)
    assert biased[1].metadata["bias_factor"] == pytest.approx(0.8)


def test_build_lineups_single_game_includes_defense():
    pool = [
        PlayerRecord(player_id="qb1", name="Quarterback", team="DAL", positions=["QB"], salary=9000, projection=20.0),
        PlayerRecord(player_id="rb1", name="Running Back 1", team="DAL", positions=["RB"], salary=7000, projection=15.0),
        PlayerRecord(player_id="rb2", name="Running Back 2", team="PHI", positions=["RB"], salary=6500, projection=14.0),
        PlayerRecord(player_id="rb3", name="Flex Back", team="NYG", positions=["RB"], salary=6000, projection=13.0),
        PlayerRecord(player_id="wr1", name="Wide Receiver 1", team="DAL", positions=["WR"], salary=7500, projection=16.0),
        PlayerRecord(player_id="wr2", name="Wide Receiver 2", team="PHI", positions=["WR"], salary=6000, projection=14.5),
        PlayerRecord(player_id="wr3", name="Wide Receiver 3", team="NYG", positions=["WR"], salary=5500, projection=13.5),
        PlayerRecord(player_id="te1", name="Tight End", team="DAL", positions=["TE"], salary=5000, projection=12.0),
        PlayerRecord(player_id="def", name="Philadelphia Defense", team="PHI", positions=["D"], salary=4500, projection=9.0),
    ]

    output = build_lineups(pool, site="FD", sport="NFL", n_lineups=1, max_exposure=1.0)

    assert len(output.lineups) == 1
    lineup = output.lineups[0]
    assert any(player.player_id == "def" for player in lineup.players)
