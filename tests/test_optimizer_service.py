from pydfs.models import PlayerRecord
from pydfs.optimizer import build_lineups


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
    lineups = build_lineups(
        _sample_pool(),
        site="FD",
        sport="MLB",
        n_lineups=1,
    )

    assert len(lineups) == 1
    lineup = lineups[0]
    assert lineup.lineup_id == "L001"
    assert len(lineup.players) == 9
    total_salary = sum(player.salary for player in lineup.players)
    assert total_salary <= 35_000


def test_build_lineups_respects_locks_and_excludes():
    pool = _sample_pool()
    locked = {"p2"}
    excluded = {"p1"}

    lineup = build_lineups(
        pool,
        site="FD",
        sport="MLB",
        n_lineups=1,
        lock_player_ids=locked,
        exclude_player_ids=excluded,
    )[0]

    ids = {player.player_id for player in lineup.players}
    assert "p1" not in ids
    assert "p2" in ids
