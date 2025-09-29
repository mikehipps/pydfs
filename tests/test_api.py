from fastapi.testclient import TestClient

from pydfs.api import create_app


def _sample_players() -> str:
    return "Id,Position,First Name,Last Name,Team,Salary,FPPG\n1,WR,Ja'Marr,Chase,CIN,9300,14.1\n"


def _sample_projections() -> str:
    return "player,team,salary,fantasy\nJa'Marr Chase,CIN,$9400,18.5\n"


client = TestClient(create_app())


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_preview_endpoint():
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\"}",
    }
    resp = client.post("/preview", files=files, data=data)
    assert resp.status_code == 200
    body = resp.json()
    assert body["matched_players"] == 1


def test_lineups_endpoint():
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 1}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\"}",
    }
    resp = client.post("/lineups", files=files, data=data)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["report"]["matched_players"] == 1
    assert len(payload["lineups"]) == 1
    assert payload["lineups"][0]["players"]
