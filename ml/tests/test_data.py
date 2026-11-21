from pathlib import Path

import polars as pl

from detektive_ml.data import COLUMNS, read_raw_csv

FIXTURE = Path(__file__).parent / "fixtures" / "tiny_trans.csv"


def test_duplicate_account_headers_are_split_into_sender_and_receiver():
    df = read_raw_csv(FIXTURE)
    assert set(COLUMNS) <= set(df.columns)
    row = df.filter(pl.col("from_bank") == "3208").row(0, named=True)
    assert row["from_account"] == "8000F4580"
    assert row["to_account"] == "8000F5340"


def test_bank_ids_keep_leading_zeros():
    df = read_raw_csv(FIXTURE)
    assert "010" in df["from_bank"].to_list()
    assert df.filter(pl.col("to_bank") == "001").height == 1


def test_rows_are_sorted_by_time_and_keep_file_order_ids():
    df = read_raw_csv(FIXTURE)
    assert df["timestamp"].is_sorted()
    # the 00:00 transaction was third in the file
    assert df.row(0, named=True)["tx_id"] == 2
    # same-minute rows stay in file order
    assert df.filter(pl.col("timestamp").dt.minute() == 20)["tx_id"].to_list() == [0, 1]


def test_account_keys_include_the_bank():
    df = read_raw_csv(FIXTURE)
    assert df.row(0, named=True)["src"] == "3209:8000F4670"
