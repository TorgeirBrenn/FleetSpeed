import asyncio
import os

import dotenv
from streaming import get_bw_stream, get_bw_token

dotenv.load_dotenv()

GET_NEW_TOKEN = False


async def main():
    token = get_bw_token() if GET_NEW_TOKEN else os.environ["TOKEN"]

    async for chunk in get_bw_stream(token):
        print(chunk)


if __name__ == "__main__":
    asyncio.run(main())
