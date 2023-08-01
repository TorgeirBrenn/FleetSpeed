from datetime import datetime, timedelta

import polars as pl
import pytest
from process import rank_by_speed, strip_data, validate_extract_vessel_data


@pytest.mark.parametrize(
    "input_line,expected_result",
    [
        (
            '{"mmsi": "123456789", "speedOverGround": 5.0, "msgtime": "2022-12-12T10:10:10"}',
            {
                "mmsi": "123456789",
                "speedOverGround": 5.0,
                "msgtime": datetime(2022, 12, 12, 10, 10, 10),
            },
        ),
        (
            '{"mmsi": 123456789, "speedOverGround": 5.0, "msgtime": "2022-12-12T10:10:10"}',
            {
                "mmsi": "123456789",
                "speedOverGround": 5.0,
                "msgtime": datetime(2022, 12, 12, 10, 10, 10),
            },
        ),
        (
            '{"mmsi": "abcd", "speedOverGround": 5.0, "msgtime": "2022-12-12T10:10:10"}',
            None,
        ),
        (
            '{"mmsi": "123", "speedOverGround": 5.0, "msgtime": "2022-12-12T10:10:10"}',
            None,
        ),
        (
            '{"mmsi": "123456789", "speedOverGround": -5.0, "msgtime": "2022-12-12T10:10:10"}',
            None,
        ),
        (
            '{"mmsi": "123456789", "speedOverGround": 5.0, "extra": "field", "msgtime": "2022-12-12T10:10:10"}',
            {
                "mmsi": "123456789",
                "speedOverGround": 5.0,
                "msgtime": datetime(2022, 12, 12, 10, 10, 10),
            },
        ),
        ("not_a_valid_json", None),
        (
            '{"mmsi": "123456789", "speedOverGround": 5.0, "msgtime": "2022-12-12T10:10:10.123456"}',
            {
                "mmsi": "123456789",
                "speedOverGround": 5.0,
                "msgtime": datetime(2022, 12, 12, 10, 10, 10),
            },
        ),
    ],
)
def test_validate_extract_vessel_data(input_line, expected_result):
    actual = validate_extract_vessel_data(input_line)

    if expected_result is None:
        assert actual is None

    else:
        assert "created_at" in actual
        assert isinstance(actual.pop("created_at"), int)

        assert actual == expected_result


def test_strip_data():
    # Common Data: This data set would be used in all cases for consistency.
    data = [
        {"created_at": (datetime.now() - timedelta(seconds=5)).timestamp() * 1e9},
        {"created_at": (datetime.now() - timedelta(seconds=3)).timestamp() * 1e9},
        {"created_at": datetime.now().timestamp() * 1e9},
    ]

    # Case 1: If the data has fewer messages than num_messages_to_keep,
    # it should return all messages.
    result_case1 = strip_data(data, time_to_live_secs=1, num_messages_to_keep=5)
    assert result_case1 == data, "Case 1 failed."

    # Case 2: If the time to live cutoff would result fewer than num_messages_to_keep,
    # it should return fixed number of recent messages.
    result_case2 = strip_data(data, time_to_live_secs=1, num_messages_to_keep=2)
    assert result_case2 == data[-2:], "Case 2 failed."

    # Case 3: Return all data that are more recent than the time cutoff.
    result_case3 = strip_data(data, time_to_live_secs=4, num_messages_to_keep=1)
    assert result_case3 == data[1:], "Case 3 failed."


def test_rank_by_speed():
    # Prepare test data
    df = pl.DataFrame(
        {
            "mmsi": ["1", "2", "3", "1", "2"],
            "speedOverGround": [1.0, 2.0, 3.0, 4.0, 5.0],
            "msgtime": [
                "2000-01-01",
                "2000-01-01",
                "2000-01-01",
                "2000-01-01",
                "2000-01-01",
            ],
        }
    )

    # Generate expected output
    expected = pl.DataFrame(
        {
            "speedOverGround": [5.0, 4.0, 3.0],
            "mmsi": ["2", "1", "3"],
            "msgtime": ["2000-01-01", "2000-01-01", "2000-01-01"],
        }
    )

    # Call the function and compare the result with expected using Polars frame_equal method
    result = rank_by_speed(df)
    assert result.frame_equal(expected)
