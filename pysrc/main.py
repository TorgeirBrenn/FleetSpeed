import asyncio
import os

import dotenv
from streaming import get_bw_stream, get_bw_token

dotenv.load_dotenv()

GET_NEW_TOKEN = True


async def main():
    token = get_bw_token() if GET_NEW_TOKEN else os.environ["TOKEN"]

    async for line in get_bw_stream(token):
        print(line)


if __name__ == "__main__":
    asyncio.run(main())
