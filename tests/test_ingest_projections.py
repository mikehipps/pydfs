from pathlib import Path

import pytest

from pydfs.ingest import ProjectionRow, merge_player_and_projection_files, rows_to_records


def _row(**kwargs):
    mapping = {
        "player_id": "player_id",
        "name": "name",
        "team": "team",
        "position": "position",
        "salary": "salary",
        "projection": "projection",
        "ownership": "ownership",
    }
    return ProjectionRow.from_mapping(kwargs, mapping)


def test_rows_to_records_mlb_positions(tmp_path: Path):
    rows = [
        _row(player_id="p1", name="Pitcher", team="nym", position="P", salary="9500", projection="18.4"),
        _row(player_id="h1", name="Hitter", team="lan", position="OF", salary="3200", projection="9.1", ownership="12.5"),
    ]

    records = rows_to_records(rows, site="FD", sport="MLB")
    assert len(records) == 2
    assert records[0].positions == ["P"]
    assert records[1].metadata["projected_ownership"] == pytest.approx(12.5)


def test_rows_to_records_nfl_defense():
    row = _row(player_id="d1", name="Defense", team="KC", position="DST", salary="3400", projection="7.2")

    records = rows_to_records([row], site="FD", sport="NFL")
    assert records[0].positions == ["D"]


def test_merge_players_and_projections(tmp_path: Path):
    players_csv = tmp_path / "players.csv"
    players_csv.write_text(
        "Id,Position,First Name,Last Name,Team,Salary,FPPG\n"
        "1,WR,Ja'Marr,Chase,CIN,9300,14.1\n"
    )

    projections_csv = tmp_path / "projections.csv"
    projections_csv.write_text(
        "player,team,salary,fantasy\n"
        "Ja'Marr Chase,CIN,$9400,18.5\n"
    )

    records, report = merge_player_and_projection_files(
        players_path=players_csv,
        projections_path=projections_csv,
        site="FD",
        sport="NFL",
        projection_mapping={"name": "player", "team": "team", "salary": "salary", "projection": "fantasy"},
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.salary == 9400
    assert rec.projection == 18.5
    assert report.matched_players == 1
    assert not report.players_missing_projection


def test_merge_handles_full_team_names(tmp_path: Path):
    players_csv = tmp_path / "players.csv"
    players_csv.write_text(
        "Id,Position,First Name,Last Name,Team,Salary,FPPG\n"
        "1,QB,Joe,Burrow,CIN,9000,20.1\n"
        "2,WR,Tyreek,Hill,MIA,8900,19.4\n"
    )

    projections_csv = tmp_path / "projections.csv"
    projections_csv.write_text(
        "player,team,salary,fantasy\n"
        "Joe Burrow,Cincinnati Bengals,$9100,23.5\n"
        "Tyreek Hill,Miami Dolphins,$9000,22.1\n"
    )

    records, report = merge_player_and_projection_files(
        players_path=players_csv,
        projections_path=projections_csv,
        site="FD",
        sport="NFL",
        projection_mapping={"name": "player", "team": "team", "salary": "salary", "projection": "fantasy"},
    )

    assert report.matched_players == 2
    teams = {r.name: r.team for r in records}
    assert teams["Joe Burrow"] == "CIN"
    assert teams["Tyreek Hill"] == "MIA"


def test_merge_handles_missing_salary(tmp_path: Path):
    players_csv = tmp_path / "players.csv"
    players_csv.write_text(
        "Id,Position,First Name,Last Name,Team,Salary,FPPG\n"
        "1,QB,Joe,Burrow,CIN,9000,20.1\n"
    )

    projections_csv = tmp_path / "projections.csv"
    projections_csv.write_text(
        "player,team,salary,fantasy\n"
        "Joe Burrow,Cincinnati Bengals,N/A,23.5\n"
    )

    records, report = merge_player_and_projection_files(
        players_path=players_csv,
        projections_path=projections_csv,
        site="FD",
        sport="NFL",
        projection_mapping={"name": "player", "team": "team", "salary": "salary", "projection": "fantasy"},
    )

    assert report.matched_players == 1
    assert records[0].salary == 9000


def test_merge_handles_defense_names(tmp_path: Path):
    players_csv = tmp_path / "players.csv"
    players_csv.write_text(
        "Id,Position,First Name,Last Name,Team,Salary,FPPG\n"
        "10,DEF,Miami,Dolphins,MIA,4500,6.5\n"
    )

    projections_csv = tmp_path / "projections.csv"
    projections_csv.write_text(
        "player,team,salary,fantasy\n"
        "Miami D/ST,Miami Dolphins,$4400,7.1\n"
    )

    records, report = merge_player_and_projection_files(
        players_path=players_csv,
        projections_path=projections_csv,
        site="FD",
        sport="NFL",
        projection_mapping={"name": "player", "team": "team", "salary": "salary", "projection": "fantasy"},
    )

    assert report.matched_players == 1
    assert records[0].team == "MIA"
    assert records[0].positions == ["D"]
