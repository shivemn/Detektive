"""Loading the IBM AML transaction files.

The raw CSV has two columns both called "Account" (sender and receiver), so we
never rely on the header names. Columns are renamed by position.
"""

from pathlib import Path

import polars as pl

COLUMNS = [
    "timestamp",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering",
]

# Bank ids can have leading zeros and account ids are hex, so both stay strings.
RAW_SCHEMA = {
    "timestamp": pl.Utf8,
    "from_bank": pl.Utf8,
    "from_account": pl.Utf8,
    "to_bank": pl.Utf8,
    "to_account": pl.Utf8,
    "amount_received": pl.Float64,
    "receiving_currency": pl.Utf8,
    "amount_paid": pl.Float64,
    "payment_currency": pl.Utf8,
    "payment_format": pl.Utf8,
    "is_laundering": pl.Int8,
}

TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M"


def read_raw_csv(path: str | Path) -> pl.DataFrame:
    """Read an IBM *_Trans.csv file into a typed frame, sorted by time.

    A transaction id is assigned in file order before sorting, so every row
    keeps a stable id that later stages (and the Rust engine) can refer to.
    """
    df = pl.read_csv(
        path,
        has_header=True,
        new_columns=COLUMNS,
        schema_overrides=RAW_SCHEMA,
    )
    if df.columns != COLUMNS:
        raise ValueError(f"unexpected columns in {path}: {df.columns}")

    df = df.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime("us"), TIMESTAMP_FORMAT),
        # an account is only unique together with its bank
        (pl.col("from_bank") + ":" + pl.col("from_account")).alias("src"),
        (pl.col("to_bank") + ":" + pl.col("to_account")).alias("dst"),
    ).with_row_index("tx_id")

    return df.sort(["timestamp", "tx_id"])


def load(path: str | Path) -> pl.DataFrame:
    """Load a prepared Parquet file, or fall back to the raw CSV."""
    path = Path(path)
    if path.suffix == ".parquet":
        return pl.read_parquet(path)
    return read_raw_csv(path)
