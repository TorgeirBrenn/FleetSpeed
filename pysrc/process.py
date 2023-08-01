from datetime import datetime
from time import time, time_ns
from typing import AsyncGenerator, TypedDict

import polars as pl
from pydantic import BaseModel, ValidationError, confloat, constr, field_validator


class VesselDataModel(BaseModel):
    """
    A Pydantic model that represents vessel data.

    This model describes the data structure of a vessel, including its MMSI number, its speed
    over ground, and a timestamp. It also includes two methods for validating and casting the
    'mmsi' and 'msgtime' fields.

    Attributes:
        mmsi (str): An nine-digit Maritime Mobile Service Identity number.
        speedOverGround (float): The speed of the vessel over ground. Must be >= 0.0.
        msgtime (datetime): A datetime object representing when the message was sent.
    """

    mmsi: constr(min_length=9, max_length=9, pattern=r"^\d+$")
    speedOverGround: confloat(ge=0.0)
    msgtime: datetime

    @field_validator("mmsi", mode="before")
    @classmethod
    def cast_mmsi(cls, val: str | int) -> str:
        return str(val)

    @field_validator("msgtime", mode="after")
    @classmethod
    def truncate_ms(cls, val: datetime) -> datetime:
        return val.replace(microsecond=0)


class VesselDataDict(TypedDict):
    """
    A TypedDict representing the vessel data and its creation time.

    This TypedDict is similar to VesselDataModel, but includes an additional field - the ns
    precision creation time of the object itself.

    Attributes:
        mmsi (str): An nine-digit Maritime Mobile Service Identity number.
        speedOverGround (float): The speed of the vessel over ground.
        msgtime (datetime): A datetime object representing when the message was sent.
        created_at (int): The ns precision creation time of this object.
    """

    mmsi: str
    speedOverGround: float
    msgtime: datetime
    created_at: int  # ns


def validate_extract_vessel_data(line: str) -> VesselDataDict | None:
    """
    Validates and extracts certain fields from a JSON string representing vessel data.

    This function takes a JSON string as input, attempts to parse and validate it according to the
    `VesselData` model. If the validation is successful, it returns a dictionary containing
    'mmsi' and 'speedOverGround'.

    If the validation fails due to a ValidationError (i.e., the input does not match the
    `VesselData` model), the function will return None.

    Args:
        line (str): A JSON string representing vessel data.

    Returns:
        VesselData | None: An object with 'mmsi', 'speedOverGround' and 'msgtime' attributes if the
        input string is valid; None otherwise.
    """
    try:
        vessel_data_model = VesselDataModel.model_validate_json(line)
    except ValidationError:
        # Ignore data errors for now
        return

    vessel_dict = vessel_data_model.model_dump()
    vessel_dict["created_at"] = time_ns()

    return vessel_dict


async def validate_stream(
    line_generator: AsyncGenerator[str, None]
) -> AsyncGenerator[VesselDataDict, None]:
    """
    Asynchronously validate and extract vessel data from a text stream.

    The function takes lines from the `line_generator` and validates the line content. If the
    line contains valid vessel data, it is yielded in the VesselDataDict format.

    Args:
        line_generator (AsyncGenerator[str, None]): An asynchronous generator yielding lines of
        text.

    Yields:
        VesselDataDict: A dictionary containing validated vessel data.
    """
    async for line in line_generator:
        vessel_data = validate_extract_vessel_data(line)
        if vessel_data:
            yield vessel_data


async def batch_stream(
    ais_msg_generator: AsyncGenerator[VesselDataDict, None],
    batch_interval_secs: int = 1,
) -> AsyncGenerator[list[VesselDataDict], None]:
    """
    Asynchronously batch vessel data messages in the given time interval.

    The function forms batches from the `ais_msg_generator`, each batch covering the provided
    `batch_interval_secs` duration. Each batch is then yielded as a list of VesselDataDict.

    Args:
        ais_msg_generator (AsyncGenerator[VesselDataDict, None]): An asynchronous generator
            yielding vessel data messages represented as dictionaries.
        batch_interval_secs (int, optional): The time interval for each batch in seconds.

    Yields:
        list[VesselDataDict]: A list containing batches of vessel data messages.
    """
    next_batch_time = time() + batch_interval_secs
    batch = []

    async for ais_msg_generator in ais_msg_generator:
        batch.append(ais_msg_generator)

        if (curr_time := time()) > next_batch_time:
            yield batch
            batch = []
            next_batch_time = curr_time + batch_interval_secs


def strip_data(
    data: list[VesselDataDict], time_to_live_secs: int, num_messages_to_keep: int
) -> list[VesselDataDict]:
    """
    Filters and returns the most recent data based on the Time To Live and the number of
    messages to keep.

    Data is retained if it is either withing the Time To Live or the most recent messages, or both.

    Args:
        data (list[VesselDataDict]): A list of vessel data.
        time_to_live_secs (int): The duration in seconds for which messages are considered recent.
        num_messages_to_keep (int): The minimum number of messages to keep.

    Returns:
        list[VesselDataDict]: A list of data that matched the filtering rules mentioned above.
    """
    # Assumes ordered data!
    now = time_ns()
    time_cutoff = now - time_to_live_secs * 1e9

    # Case 1: Fewer messages than what we want to keep => keep everything
    if len(data) <= num_messages_to_keep:
        return data

    # Case 2: The time cutoff would result in fewer than num_messages_to_keep => return fixed number
    # of messages
    if data[-num_messages_to_keep]["created_at"] < time_cutoff:
        return data[-num_messages_to_keep:]

    # Case 3: Return data more recent than the cutoff
    for n, entry in enumerate(data):
        if entry["created_at"] >= time_cutoff:
            return data[n:]


def convert_batch_to_dataframe(
    data: list[VesselDataDict], batch_number: int
) -> pl.DataFrame:
    """
    Converts a data batch into a dataframe and assigns a batch number to it.

    Args:
        data (list[VesselDataDict]): A list of vessel data dictionaries.
        batch_number (int): The batch number to be assigned.

    Returns:
        DataFrame: Polars DataFrame created from the batch data.
    """
    df = convert_to_dataframe(data)
    df = df.with_columns(pl.lit(batch_number).alias("batch_number"))

    return df


def convert_to_dataframe(data: list[VesselDataDict]) -> pl.DataFrame:
    """
    Converts a list of vessel data dictionaries into a dataframe.

    Args:
        data (list[VesselDataDict]): A list of vessel data dictionaries.

    Returns:
        DataFrame: Polars DataFrame created from the data.
    """
    df = pl.from_dicts(data, schema=VesselDataDict.__annotations__)

    return df


def empty_batch() -> pl.DataFrame:
    """
    Returns an empty dataframe structured as a batch of vessel data.

    Returns:
        DataFrame: an empty Polars DataFrame.
    """
    return convert_batch_to_dataframe([], batch_number=0)


def add_batch_to_df(
    batch: pl.DataFrame, full_df: pl.DataFrame, num_batches: int | None = None
) -> pl.DataFrame:
    """
    Adds a batch of data onto an existing dataframe, keeping the most recent batches.

    Args:
        batch (DataFrame): New batch of data.
        full_df (DataFrame): Existing data.
        num_batches (int, optional): Number of recent batches to keep in the full dataframe.

    Returns:
        DataFrame: The dataframe with new batch added, keeping the most recent batches.
    """
    df = pl.concat([full_df, batch])

    if num_batches is not None:
        max_batch_number = batch.select("batch_number").max().item()

        # If there is no data, then max_batch_number will be None and we have to allow that
        if max_batch_number is not None:
            df = df.filter(pl.col("batch_number") > max_batch_number - num_batches)

    return df


def rank_by_speed(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ranks the vessel data by maximum speed over ground.

    Args:
        df (DataFrame): Vessel data in DataFrame form.

    Returns:
        DataFrame: The input dataframe with vessels ranked by speed in descending order.
    """
    ranked = (
        df.select(pl.col("speedOverGround").max().over("mmsi"), "mmsi", "msgtime")
        .unique()
        .sort("speedOverGround", descending=True)
    )
    return ranked
