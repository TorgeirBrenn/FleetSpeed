import polars as pl
from rich.table import Table


def dataframe_to_table(df: pl.DataFrame) -> Table:
    table = Table(
        show_header=True,
        header_style="bold magenta",
        show_lines=True,
        title="FleetSpeed - The Fastest Vessels in Norway Right Now",
    )

    # column ordering is important
    df = df.select("mmsi", "speedOverGround", "msgtime")

    table.add_column("Vessel MMSI", style="cyan")
    table.add_column("Speed (kn)", style="bold red", justify="right")
    table.add_column("Measured at", style="orange_red1")

    for row in df.iter_rows():
        table.add_row(*(str(itm) for itm in row))
    return table
