import pytest
from streaming import get_bw_token, handle_chunks


class MockAiohttpResponse:
    def __init__(self, content):
        self.content = self.MockContent(content)

    class MockContent:
        def __init__(self, chunks):
            self.data = chunks

        async def iter_any(self):
            for chunk in self.data:
                yield chunk


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


@pytest.mark.asyncio
async def test_handle_chunks():
    chunks_bytes = [b"mock_chunk1", b"mock_chunk2"]
    response = MockAiohttpResponse(chunks_bytes)

    result = [chunk async for chunk in handle_chunks(response)]
    expected_result = [chunk.decode() for chunk in chunks_bytes]

    assert result == expected_result


def test_get_bw_token(mock_functions):
    token = get_bw_token()
    assert token == "mock_token"


# Decided not to unit test get_bw_stream due to complexity, including extra dependencies.
