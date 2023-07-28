import pytest
from streaming import get_bw_token


class MockHttpxResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception("Mock http error")


def mock_httpx_post(*_, **__):
    return MockHttpxResponse(json_data={"access_token": "mock_token"})


@pytest.fixture
def mock_functions(monkeypatch):
    monkeypatch.setattr("httpx.post", mock_httpx_post)


def test_get_bw_token(mock_functions):
    token = get_bw_token()
    assert token == "mock_token"


# Decided not to unit test get_bw_stream due to complexity, including extra dependencies.
