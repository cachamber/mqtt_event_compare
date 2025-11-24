import pytest

from mqtt_compare import diff_events


def test_diff_simple_added_removed_changed() -> None:
    a = {"x": 1, "y": 2}
    b = {"x": 1, "z": 3}
    d = diff_events(a, b)
    assert ("z", 3) in [(p.split(".")[-1], v) for p, v in d["added"]]
    assert ("y", 2) in [(p.split(".")[-1], v) for p, v in d["removed"]]
    # x unchanged
    assert all("x" not in p for p, _ in d["changed"])


def test_diff_nested_and_list() -> None:
    a = {"a": {"b": [1, 2, {"c": 3}]}}
    b = {"a": {"b": [1, 2, {"c": 4}, 5]}}
    d = diff_events(a, b)
    # changed c
    changed_paths = [p for p, _ in d["changed"]]
    assert any("c" in p for p in changed_paths)
    # added index [3]
    added_paths = [p for p, _ in d["added"]]
    assert any("[3]" in p for p in added_paths)
