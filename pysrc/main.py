import asyncio
import os

import dotenv
from process import (
    add_batch_to_df,
    convert_batch_to_dataframe,
    empty_batch,
    rank_by_speed,
    validate_and_batch_stream,
)
from rich.console import Console
from streaming import get_bw_stream, get_bw_token
from visualize import dataframe_to_table

dotenv.load_dotenv()

GET_NEW_TOKEN = True


async def main():
    token = get_bw_token() if GET_NEW_TOKEN else os.environ["TOKEN"]

    console = Console()

    batch_number = 0
    full_df = empty_batch()

    async for batch in validate_and_batch_stream(get_bw_stream(token)):
        batch_df = convert_batch_to_dataframe(batch, batch_number)
        batch_number += 1

        full_df = add_batch_to_df(batch_df, full_df, num_batches=10)
        top = rank_by_speed(full_df).head(10)

        table = dataframe_to_table(top)
        console.clear()
        console.print(table)


if __name__ == "__main__":
    asyncio.run(main())
