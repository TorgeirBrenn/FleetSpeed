import os
from typing import AsyncGenerator

import aiohttp
import dotenv
import httpx


def get_bw_token() -> str:
    dotenv.load_dotenv()

    url = "https://id.barentswatch.no/connect/token"

    payload = {
        "grant_type": "client_credentials",
        "scope": "ais",
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = httpx.post(url, headers=headers, data=payload)
    response.raise_for_status()

    response_json = response.json()

    try:
        return response_json["access_token"]
    except KeyError:
        raise ValueError(f"Unexpected response format '{response.text}'.")


async def handle_chunks(response) -> AsyncGenerator[str, None]:
    async for chunk in response.content.iter_any():
        yield chunk.decode()


async def get_bw_stream(token) -> AsyncGenerator[str, None]:
    auth_str = f"Bearer {token}"  # complete authorization string

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": auth_str,
    }

    # Create a ClientSession with disabled SSL verification
    conn = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.get(
            "https://live.ais.barentswatch.no/v1/ais", headers=headers
        ) as response:
            async for chunk in handle_chunks(response):
                yield chunk
