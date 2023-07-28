import pytest
from process import validate_extract_vessel_data


# fmt: off
@pytest.mark.parametrize(
    "input_line,expected_result",
    [
        ('{"mmsi": "123456789", "speedOverGround": 5.0}', {"mmsi": "123456789", "speedOverGround": 5.0},),
        ('{"mmsi": 123456789, "speedOverGround": 5.0}', {"mmsi": "123456789", "speedOverGround": 5.0},),
        ('{"mmsi": "abcd", "speedOverGround": 5.0}', None),
        ('{"mmsi": "123", "speedOverGround": 5.0}', None),
        ('{"mmsi": "123456789", "speedOverGround": -5.0}', None),
        ('{"mmsi": "123456789", "speedOverGround": 5.0, "extra": "field"}', {"mmsi": "123456789", "speedOverGround": 5.0},),
        ('not_a_valid_json', None),
    ],
)
# fmt: on
def test_validate_extract_vessel_data(input_line, expected_result):
    assert validate_extract_vessel_data(input_line) == expected_result
