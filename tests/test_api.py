import pytest
from httpx import ASGITransport, AsyncClient

from pydfs.api import create_app


@pytest.fixture(scope="module")
async def client():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client


def _sample_players() -> str:
    return """Id,Position,First Name,Last Name,Team,Salary,FPPG
1,QB,Joe,Quarterback,CIN,8000,20
2,RB,Rob,Runner,CIN,7500,15
3,RB,Sam,Rusher,DEN,7200,14
4,WR,Will,Receiver,CIN,6900,12
5,WR,Max,Target,DEN,6600,11
6,WR,Leo,Fly,NYJ,6400,10
7,TE,Ted,End,DEN,5800,9
8,RB,Luke,Flex,NYJ,5500,8
9,DEF,Bengals,Defense,CIN,4000,5
"""


def _sample_projections() -> str:
    return """player,team,salary,fantasy,proj_own
Joe Quarterback,CIN,$8000,22.5,18.0
Rob Runner,CIN,$7500,16.0,12.5
Sam Rusher,DEN,$7200,15.5,10.0
Will Receiver,CIN,$6900,13.2,14.0
Max Target,DEN,$6600,12.0,9.0
Leo Fly,NYJ,$6400,11.0,8.5
Ted End,DEN,$5800,10.5,7.5
Luke Flex,NYJ,$5500,9.0,6.0
Bengals Defense,CIN,$4000,6.0,4.0
"""
@pytest.mark.anyio
async def test_health(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.anyio
async def test_preview_endpoint(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\"}",
    }
    resp = await client.post("/preview", files=files, data=data)
    assert resp.status_code == 200
    body = resp.json()
    assert body["matched_players"] == 9


@pytest.mark.anyio
async def test_ui_home(client: AsyncClient):
    resp = await client.get("/ui")
    assert resp.status_code == 200
    assert "pydfs Optimizer" in resp.text


@pytest.mark.anyio
async def test_lineups_endpoint(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 1}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    resp = await client.post("/lineups", files=files, data=data)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["run_id"]
    assert payload["report"]["matched_players"] == 9
    assert len(payload["lineups"]) == 1
    lineup = payload["lineups"][0]
    assert len(lineup["players"]) == 9
    # Ownership should be parsed through mapping
    assert lineup["players"][0]["ownership"] is not None

    run_id = payload["run_id"]
    resp = await client.get("/runs")
    assert resp.status_code == 200
    assert any(run["run_id"] == run_id for run in resp.json())

    resp = await client.get(f"/runs/{run_id}")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["run_id"] == run_id
    assert detail["site"] == "FD"
    assert detail["sport"] == "NFL"

    resp = await client.post(f"/runs/{run_id}/rerun")
    assert resp.status_code == 200
    rerun_payload = resp.json()
    assert rerun_payload["run_id"] == run_id
    assert rerun_payload["lineups"], "Stored lineups should be returned"
