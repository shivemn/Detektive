from datetime import datetime, timedelta

import polars as pl
import pytest

from detektive_ml.split import sparse_tail_start, time_split


def frame(minutes: list[int]) -> pl.DataFrame:
    start = datetime(2022, 9, 1)
    return pl.DataFrame({
        "timestamp": [start + timedelta(minutes=m) for m in minutes],
        "is_laundering": [i % 7 == 0 for i in range(len(minutes))],
    }).with_columns(pl.col("is_laundering").cast(pl.Int8))


def test_splits_are_in_time_order_and_cover_everything():
    df = frame(list(range(1000)))
    s = time_split(df)
    assert s.train.height + s.val.height + s.test.height == df.height
    assert s.train["timestamp"].max() < s.val["timestamp"].min()
    assert s.val["timestamp"].max() < s.test["timestamp"].min()


def test_shares_are_close_to_60_20_20():
    s = time_split(frame(list(range(1000))))
    assert (s.train.height, s.val.height, s.test.height) == (600, 200, 200)


def test_same_minute_never_straddles_a_boundary():
    # 100 transactions all in minute 600, right where the train/val cut falls
    minutes = list(range(550)) + [600] * 100 + list(range(601, 951))
    s = time_split(frame(minutes))
    stamps = lambda part: set(part["timestamp"].to_list())
    assert not stamps(s.train) & stamps(s.val)
    assert not stamps(s.val) & stamps(s.test)


def test_rejects_bad_fractions():
    with pytest.raises(ValueError):
        time_split(frame(list(range(10))), train_frac=0.9, val_frac=0.2)


def test_finds_sparse_tail_at_the_end():
    # ten busy days of 300 transactions, then three days with one each
    minutes = [d * 1440 + i for d in range(10) for i in range(300)] + [d * 1440 for d in range(10, 13)]
    start = sparse_tail_start(frame(minutes))
    assert start == datetime(2022, 9, 11)


def test_no_tail_when_volume_is_even():
    minutes = [d * 1440 + i for d in range(10) for i in range(100)]
    assert sparse_tail_start(frame(minutes)) is None


def test_quiet_day_in_the_middle_is_not_a_tail():
    minutes = [d * 1440 + i for d in range(10) if d != 4 for i in range(100)] + [4 * 1440]
    assert sparse_tail_start(frame(minutes)) is None
