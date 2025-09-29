import pytest

from pydfs.config import get_rules, get_rules_by_key


def test_get_rules_handles_site_and_sport_uppercase():
    rules = get_rules("fd", "mlb")
    assert rules.site == "FD"
    assert "P" in rules.slot_positions


def test_get_rules_by_key_string_alias():
    rules = get_rules_by_key("FD_NFL")
    assert rules.salary_cap == 60_000


def test_get_rules_missing_raises():
    with pytest.raises(KeyError):
        get_rules("FD", "CURLING")
