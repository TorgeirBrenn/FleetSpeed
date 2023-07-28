from datetime import datetime
from time import time
from typing import AsyncGenerator, TypedDict

import polars as pl
from pydantic import BaseModel, ValidationError, confloat, constr, field_validator


class VesselData(BaseModel):
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
    mmsi: str
    speedOverGround: float
    msgtime: datetime


def validate_extract_vessel_data(line: str) -> VesselDataDict | None:
    """
    Validates and extracts certain fields from a JSON string representing vessel data.

    This function takes a JSON string as input, attempts to parse and validate it
    according to the `VesselData` model. If the validation is successful,
    it returns a dictionary containing 'mmsi' and 'speedOverGround'.

    If the validation fails due to a ValidationError (i.e., the input does not match
    the `VesselData` model), the function will return None.

    Args:
        line (str): A JSON string representing vessel data.

    Returns:
        VesselDataDict | None: A dictionary with 'mmsi' and 'speedOverGround' fields
        if the input string is valid; None otherwise.
    """
    try:
        vessel_data = VesselData.model_validate_json(line)
    except ValidationError:
        # Ignore data errors for now
        return
    return vessel_data.model_dump()


async def validate_and_batch_stream(
    line_generator: AsyncGenerator[str, None]
) -> AsyncGenerator[list[VesselDataDict], None]:
    """Validates each line of data and strips out fields which are not needed. Then returns mini-
    batches corresponding to one second."""
    next_batch_time = time() + 1
    batch = []

    async for line in line_generator:
        vessel_data = validate_extract_vessel_data(line)
        if vessel_data:
            batch.append(vessel_data)

        if (curr_time := time()) > next_batch_time:
            yield batch
            batch = []
            next_batch_time = curr_time + 1


def convert_batch_to_dataframe(
    data: list[VesselDataDict], batch_number: int
) -> pl.DataFrame:
    df = pl.from_dicts(data, schema=VesselDataDict.__annotations__)
    df = df.with_columns(pl.lit(batch_number).alias("batch_number"))

    return df


def empty_batch() -> pl.DataFrame:
    return convert_batch_to_dataframe([], batch_number=0)


def add_batch_to_df(
    batch: pl.DataFrame, full_df: pl.DataFrame, num_batches: int | None = None
) -> pl.DataFrame:
    df = pl.concat([full_df, batch])

    if num_batches is not None:
        max_batch_number = batch.select("batch_number").max().item()

        # If there is no data, then max_batch_number will be None and we have to allow that
        if max_batch_number is not None:
            df = df.filter(pl.col("batch_number") > max_batch_number - num_batches)

    return df


def rank_by_speed(df: pl.DataFrame) -> pl.DataFrame:
    ranked = (
        df.select(pl.col("speedOverGround").max().over("mmsi"), "mmsi", "msgtime")
        .unique()
        .sort("speedOverGround", descending=True)
    )
    return ranked
