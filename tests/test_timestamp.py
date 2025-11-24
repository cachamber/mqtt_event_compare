from datetime import datetime

from mqtt_compare import extract_timestamp, parse_timestamp_from_value


def test_parse_numeric_seconds():
    ts = parse_timestamp_from_value(1600000000)
    assert isinstance(ts, datetime)


def test_parse_numeric_ms():
    ts = parse_timestamp_from_value(1600000000000)
    assert isinstance(ts, datetime)


def test_parse_iso_string():
    ts = parse_timestamp_from_value("2020-09-13T12:00:00Z")
    assert ts.tzinfo is not None


def test_extract_from_payload_dict():
    payload = {"timestamp": 1600000000}
    ts = extract_timestamp(payload)
    assert isinstance(ts, datetime)
