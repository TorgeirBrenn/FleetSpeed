import asyncio
import os
import time

import dotenv
from process import (
    add_batch_to_df,
    batch_stream,
    convert_batch_to_dataframe,
    convert_to_dataframe,
    empty_batch,
    rank_by_speed,
    strip_data,
    validate_stream,
)
from rich.live import Live
from streaming import get_bw_stream, get_bw_token
from visualize import dataframe_to_table

dotenv.load_dotenv()

DEBUG_MODE = False
GET_NEW_TOKEN = True
UPDATE_INTERVAL_SECONDS = 1
NUM_VESSELS_DISPLAYED = 10

# Keep all data from the last X seconds or the most recent Y entries, whichever is more inclusive
DATA_TIME_TO_LIVE_SECONDS = 10
NUM_MESSAGES_TO_KEEP = 1000


def initialize_with_token(func):
    """
    A decorator that initializes a FleetSpeed client with a token.

    Before the execution of the decorated function, it prints an initialization message and
    fetches a Bearer token. The token is either newly obtained by calling `get_bw_token` or
    fetched from the environment variable "TOKEN".

    This decorator allows for reusability of token fetching and the initialization message for
    multiple functions that act as clients under different circumstances.

    Args:
        func (function): The function being decorated. The function must be structurally compatible
        for receiving a token as the first argument.

    Returns:
        function: The decorated function, with the token as the first argument.
    """

    def wrapper_func(*args, **kwargs):
        print("Initializing FleetSpeed! Remember to always put safety first at sea.")
        token = get_bw_token() if GET_NEW_TOKEN else os.environ["TOKEN"]
        return func(token, *args, **kwargs)

    return wrapper_func


@initialize_with_token
async def main_single_batch(token: str):
    """
    Asynchronously streams AIS messages, processes and ranks data for vessel speeds.

    This function initializes with an empty data frame. It then continuously streams and
    validates the AIS messages by using the provided token.

    Every `UPDATE_INTERVAL_SECONDS`, it processes the received AIS messages into a dataframe,
    ranks them by speed, and updates a live table with the top vessels.

    Finally, it uses the `strip_data` function to manage the size of the stored messages.

    Args:
        token (str): Token to authenticate the AIS stream source.
    """
    full_df = empty_batch()

    data = []
    start_time = time.time()

    with Live(dataframe_to_table(full_df), refresh_per_second=5) as live:
        async for ais_msg in validate_stream(get_bw_stream(token)):
            data.append(ais_msg)

            if time.time() - start_time > UPDATE_INTERVAL_SECONDS:
                # Process the data. Usually takes less than 1/100th of a second.
                timer = time.time()  # debug purposes

                df = convert_to_dataframe(data)
                top = rank_by_speed(df).head(NUM_VESSELS_DISPLAYED)
                table = dataframe_to_table(top)
                live.update(table)

                # Reset
                start_time = time.time()
                data = strip_data(
                    data,
                    time_to_live_secs=DATA_TIME_TO_LIVE_SECONDS,
                    num_messages_to_keep=NUM_MESSAGES_TO_KEEP,
                )

                if DEBUG_MODE:
                    print(
                        f"Used {len(df)} messages and {time.time() - timer}s to display."
                    )  # -> Used 1518 messages and 0.004235029220581055s to display.


@initialize_with_token
async def main_multi_batch(token: str):
    """Alternative approach to main_single_batch(), found to be less effective and was not
    sufficiently tuned."""
    batch_number = 0
    full_df = empty_batch()

    with Live(dataframe_to_table(full_df), refresh_per_second=5) as live:
        async for batch in batch_stream(validate_stream(get_bw_stream(token))):
            batch_df = convert_batch_to_dataframe(batch, batch_number)
            batch_number += 1

            full_df = add_batch_to_df(batch_df, full_df, num_batches=20)
            top = rank_by_speed(full_df).head(10)

            table = dataframe_to_table(top)
            live.update(table)


if __name__ == "__main__":
    asyncio.run(main_single_batch())
