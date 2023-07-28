import os
from typing import AsyncGenerator

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


async def get_bw_stream(token) -> AsyncGenerator[str, None]:
    auth_str = f"Bearer {token}"  # complete authorization string

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": auth_str,
    }

    async with httpx.AsyncClient() as client:
        async with client.stream(
            "GET", "https://live.ais.barentswatch.no/v1/ais", headers=headers
        ) as response:
            response.raise_for_status()
            async for chunk in response.aiter_lines():
                yield chunk
