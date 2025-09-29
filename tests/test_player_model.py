import pytest
from pydantic import ValidationError

from pydfs.models import PlayerRecord


def test_player_record_is_frozen():
    record = PlayerRecord(
        player_id="p1",
        name="Test Player",
        team="NYM",
        positions=["P"],
        salary=9000,
        projection=20.5,
    )

    assert record.player_id == "p1"
    assert record.positions == ["P"]

    with pytest.raises((TypeError, ValidationError)):
        record.player_id = "p2"  # type: ignore[attr-defined]
