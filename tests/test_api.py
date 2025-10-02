import json

import pytest
from httpx import ASGITransport, AsyncClient
from uuid import uuid4

from pydfs.api import create_app
from pydfs.api.schemas.lineup import LineupPlayerResponse, LineupResponse
from pydfs.pool.filtering import FilterCriteria, LineupCandidate, filter_lineups


@pytest.fixture(scope="module")
async def client():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        async_client.app = app
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


def _updated_projections() -> str:
    return """player,team,salary,fantasy,proj_own
Joe Quarterback,CIN,$8000,30.5,18.0
Rob Runner,CIN,$7500,17.0,12.5
Sam Rusher,DEN,$7200,14.0,10.0
Will Receiver,CIN,$6900,12.2,14.0
Max Target,DEN,$6600,13.0,9.0
Leo Fly,NYJ,$6400,7.0,8.5
Ted End,DEN,$5800,11.0,7.5
Luke Flex,NYJ,$5500,8.5,6.0
Bengals Defense,CIN,$4000,5.0,4.0
"""


def _candidate(
    lineup_id: str,
    players: list[str],
    baseline: float,
    run_ids: tuple[str, ...] = ("run",),
) -> LineupCandidate:
    lineup = LineupResponse(
        lineup_id=lineup_id,
        salary=50000,
        projection=baseline,
        baseline_projection=baseline,
        players=[
            LineupPlayerResponse(
                player_id=player_id,
                name=f"Player {player_id}",
                team="TEAM",
                positions=["POS"],
                salary=5000,
                projection=baseline,
                ownership=None,
                baseline_projection=baseline,
            )
            for player_id in players
        ],
    )
    signature = tuple(sorted(players))
    return LineupCandidate(
        signature=signature,
        lineup=lineup,
        count=1,
        run_ids=run_ids,
        salary=lineup.salary,
        projection=lineup.projection,
        baseline=baseline,
        usage_sum=0.0,
        uniqueness=1.0,
        baseline_percentile=0.0,
        usage_percentile=0.0,
        uniqueness_percentile=0.0,
    )
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
    assert payload.get("slate_id")
    assert payload.get("bias_summary") is not None

    run_id = payload["run_id"]
    resp = await client.get("/runs")
    assert resp.status_code == 200
    runs_payload = resp.json()
    assert any(run["run_id"] == run_id and run["state"] == "completed" for run in runs_payload)

    resp = await client.get(f"/runs/{run_id}")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["run_id"] == run_id
    assert detail["site"] == "FD"
    assert detail["sport"] == "NFL"
    assert detail["state"] == "completed"
    assert detail["job"] is not None
    assert detail["request"]["perturbation_p25"] == pytest.approx(0.0)
    assert detail["request"]["perturbation_p75"] == pytest.approx(0.0)
    assert detail["request"]["exposure_bias"] == pytest.approx(0.0)
    assert detail["request"]["exposure_bias_target"] == pytest.approx(50.0)
    assert detail["request"].get("slate_id")
    assert detail["request"].get("slate_projections_filename")
    assert detail.get("bias_summary")

    resp = await client.post(f"/runs/{run_id}/rerun")
    assert resp.status_code == 200
    rerun_payload = resp.json()
    assert rerun_payload["run_id"] == run_id
    assert rerun_payload["lineups"], "Stored lineups should be returned"


@pytest.mark.anyio
async def test_lineups_with_stored_slate(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 1}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    resp = await client.post("/lineups", files=files, data=data)
    resp.raise_for_status()
    first_payload = resp.json()
    slate_id = first_payload.get("slate_id")
    assert slate_id, "First run should store a slate"
    store = client.app.state.run_store
    slate_record = store.get_slate(slate_id)
    assert slate_record is not None
    assert slate_record.bias_factors

    # Re-run without uploading files, using stored slate
    resp2 = await client.post(
        "/lineups",
        data={
            "lineup_request": "{\"lineups\": 1}",
            "slate_id": slate_id,
        },
    )
    resp2.raise_for_status()
    second_payload = resp2.json()
    assert second_payload["slate_id"] == slate_id
    assert len(second_payload["lineups"]) == 1
    assert second_payload.get("bias_summary")
    detail_again = await client.get(f"/runs/{second_payload['run_id']}")
    detail_again.raise_for_status()
    run_detail = detail_again.json()
    assert run_detail["request"]["slate_id"] == slate_id
    assert run_detail.get("bias_summary")


@pytest.mark.anyio
async def test_lineups_with_custom_perturbation_ranges(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    request_payload = {
        "lineups": 1,
        "perturbation_p25": 40,
        "perturbation_p75": 10,
        "exposure_bias": 15,
        "exposure_bias_target": 35,
    }
    data = {
        "lineup_request": json.dumps(request_payload),
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    resp = await client.post("/lineups", files=files, data=data)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    detail_resp = await client.get(f"/runs/{run_id}")
    detail_resp.raise_for_status()
    detail = detail_resp.json()
    assert detail["request"]["perturbation_p25"] == pytest.approx(40.0)
    assert detail["request"]["perturbation_p75"] == pytest.approx(10.0)
    assert detail["request"].get("slate_id")
    assert detail["request"]["exposure_bias"] == pytest.approx(15.0)
    assert detail["request"]["exposure_bias_target"] == pytest.approx(35.0)
    assert detail.get("bias_summary")


@pytest.mark.anyio
async def test_cancel_endpoint(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 1}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    resp = await client.post("/lineups", files=files, data=data)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    cancel_resp = await client.post(f"/runs/{run_id}/cancel")
    assert cancel_resp.status_code == 200
    cancel_body = cancel_resp.json()
    assert cancel_body["state"] == "completed"

    store = client.app.state.run_store
    pending_id = uuid4().hex
    store.create_job(run_id=pending_id, site="FD", sport="NFL", state="running")

    pending_cancel = await client.post(f"/runs/{pending_id}/cancel")
    assert pending_cancel.status_code == 200
    pending_body = pending_cancel.json()
    assert pending_body["state"] == "cancel_requested"
    assert pending_body["cancel_requested_at"] is not None

    runs_resp = await client.get("/runs")
    runs_resp.raise_for_status()
    assert any(item["run_id"] == pending_id and item["state"] == "cancel_requested" for item in runs_resp.json())

    missing = await client.post("/runs/does-not-exist/cancel")
    assert missing.status_code == 404


@pytest.mark.anyio
async def test_lineup_pool_page(client: AsyncClient):
    resp = await client.get("/ui/pool")
    assert resp.status_code == 200
    assert "Lineup Pool" in resp.text
    assert "name=\"slate_id\"" in resp.text
    assert "Range: Today" in resp.text
    assert "Replace projections CSV" in resp.text
    store = client.app.state.run_store
    latest_slate = store.get_latest_slate()
    assert latest_slate is not None
    assert f"value=\"{latest_slate.slate_id}\" selected" in resp.text

    resp_filtered = await client.get("/ui/pool", params={"site": "FD", "sport": "NFL", "limit": 10})
    assert resp_filtered.status_code == 200
    assert "Runs in Pool" in resp_filtered.text

    resp_shortcut = await client.get("/ui/pool/nfl/fd")
    assert resp_shortcut.status_code == 200
    assert "Lineup Pool" in resp_shortcut.text
    assert f"value=\"{latest_slate.slate_id}\" selected" in resp_shortcut.text

    resp_sport_only = await client.get("/ui/pool/nfl")
    assert resp_sport_only.status_code == 200
    assert "Lineup Pool" in resp_sport_only.text
    assert "Current Slate" in resp_sport_only.text


def test_filter_lineups_applies_max_exposure_cap():
    candidates = [
        _candidate("L1", ["A", "B"], 100),
        _candidate("L2", ["A", "C"], 95),
        _candidate("L3", ["D", "E"], 90),
    ]
    criteria = FilterCriteria(limit=2, max_player_exposure=0.5)
    result = filter_lineups(candidates, criteria)

    selected_ids = [item.candidate.lineup.lineup_id for item in result.lineups]
    assert selected_ids == ["L1", "L3"], "Lineups exceeding exposure cap should be skipped"

    exposure_counts: dict[str, int] = {}
    for lineup in result.lineups:
        for player in lineup.candidate.lineup.players:
            exposure_counts[player.player_id] = exposure_counts.get(player.player_id, 0) + 1
    assert all(count <= 1 for count in exposure_counts.values())


def test_filter_lineups_respects_player_specific_caps():
    candidates = [
        _candidate("L1", ["A", "B"], 100),
        _candidate("L2", ["C", "D"], 95),
    ]
    criteria = FilterCriteria(limit=2, player_exposure_caps=(("A", 0.0),))
    result = filter_lineups(candidates, criteria)

    selected_ids = [item.candidate.lineup.lineup_id for item in result.lineups]
    assert selected_ids == ["L2"], "Lineups containing capped players should be excluded"


@pytest.mark.anyio
async def test_update_slate_projections_from_pool(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 1}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    resp = await client.post("/lineups", files=files, data=data)
    resp.raise_for_status()
    payload = resp.json()
    slate_id = payload["slate_id"]
    store = client.app.state.run_store

    update_resp = await client.post(
        f"/ui/pool/{slate_id}/update",
        files={"projections": ("updated.csv", _updated_projections(), "text/csv")},
        follow_redirects=False,
    )
    assert update_resp.status_code == 303

    slate = store.get_slate(slate_id)
    assert slate is not None
    assert slate.projections_filename == "updated.csv"
    projection_lookup = {record["player_id"]: record["projection"] for record in slate.records}
    assert projection_lookup["1"] == pytest.approx(30.5)

    follow_url = update_resp.headers["location"]
    follow_resp = await client.get(follow_url)
    assert follow_resp.status_code == 200
    assert "Projections updated" in follow_resp.text
    assert "updated.csv" in follow_resp.text


@pytest.mark.anyio
async def test_reset_slate_bias(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": json.dumps({"lineups": 1, "exposure_bias": 20, "exposure_bias_target": 30}),
    }
    resp = await client.post("/lineups", files=files, data=data)
    resp.raise_for_status()
    slate_id = resp.json()["slate_id"]
    store = client.app.state.run_store
    slate = store.get_slate(slate_id)
    assert slate and slate.bias_factors

    reset_resp = await client.post(
        f"/slates/{slate_id}/reset-bias",
        data={"redirect": "/ui"},
        follow_redirects=False,
    )
    assert reset_resp.status_code == 303
    slate_after = store.get_slate(slate_id)
    assert slate_after is not None
    assert slate_after.bias_factors == {}


@pytest.mark.anyio
async def test_pool_filter_and_export(client: AsyncClient):
    files = {
        "projections": ("projections.csv", _sample_projections(), "text/csv"),
        "players": ("players.csv", _sample_players(), "text/csv"),
    }
    data = {
        "lineup_request": "{\"lineups\": 3}",
        "projection_mapping": "{\"name\": \"player\", \"team\": \"team\", \"salary\": \"salary\", \"projection\": \"fantasy\", \"ownership\": \"proj_own\"}",
    }
    run_resp = await client.post("/lineups", files=files, data=data)
    run_resp.raise_for_status()
    run_payload = run_resp.json()
    slate_id = run_payload.get("slate_id")
    assert slate_id

    filter_request = {
        "slate_id": slate_id,
        "site": "FD",
        "sport": "NFL",
        "limit": 3,
        "include_player_ids": ["1"],
    }
    filter_resp = await client.post("/pool/filter", json=filter_request)
    filter_resp.raise_for_status()
    body = filter_resp.json()
    assert body["pool_summary"]["available_lineups"] >= body["summary"]["available_lineups"]
    assert body["summary"]["available_lineups"] >= 1
    assert body["lineups"], "Filtered lineups should not be empty"
    assert body["pool_usage"], "Pool usage table should be populated"
    assert body["filtered_usage"], "Filtered usage table should be populated"
    first_lineup = body["lineups"][0]
    assert first_lineup["players"], "Lineup payload should include players"

    export_url = f"/pool/export.csv?slate_id={slate_id}&include_players=1&filter_limit=2"
    export_resp = await client.get(export_url)
    export_resp.raise_for_status()
    lines = [line for line in export_resp.text.strip().splitlines() if line]
    assert lines, "Export CSV should contain data"
    header = lines[0].split(",")
    assert header[0] == "EntryName"
    assert any(cell.startswith("QB") for cell in header[1:])
