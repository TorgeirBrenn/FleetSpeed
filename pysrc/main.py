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
from rich.live import Live
from streaming import get_bw_stream, get_bw_token
from visualize import dataframe_to_table

dotenv.load_dotenv()

GET_NEW_TOKEN = True


async def main():
    print("Initializing FleetSpeed! Remember to always put safety first at sea.")
    token = get_bw_token() if GET_NEW_TOKEN else os.environ["TOKEN"]

    batch_number = 0
    full_df = empty_batch()

    with Live(dataframe_to_table(full_df), refresh_per_second=5) as live:
        async for batch in validate_and_batch_stream(get_bw_stream(token)):
            batch_df = convert_batch_to_dataframe(batch, batch_number)
            batch_number += 1

            full_df = add_batch_to_df(batch_df, full_df, num_batches=20)
            top = rank_by_speed(full_df).head(10)

            table = dataframe_to_table(top)
            live.update(table)


if __name__ == "__main__":
    asyncio.run(main())
