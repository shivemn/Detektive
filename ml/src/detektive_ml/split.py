"""The time split every model in this repo is scored on.

Transactions are cut at two timestamps so that the first ~60% (by count) are
for training, the next ~20% for validation and the last ~20% for testing.
Cuts fall on timestamp boundaries: two transactions in the same minute always
land in the same split, so nothing from the future leaks into training.
"""

from dataclasses import dataclass
from datetime import datetime

import polars as pl

TRAIN_FRAC = 0.6
VAL_FRAC = 0.2
# a day counts as "sparse" when it has under 1% of a typical day's transactions
SPARSE_DAY_RATIO = 0.01


def sparse_tail_start(df: pl.DataFrame, ratio: float = SPARSE_DAY_RATIO) -> datetime | None:
    """Start of the run of near-empty days at the end of the data, if any.

    The IBM simulator stops generating normal traffic before laundering chains
    finish, so its last days hold a few hundred transactions that are mostly
    laundering. They are easy to catch and inflate test scores, so the
    protocol reports scores with and without them.
    """
    daily = (
        df.group_by(pl.col("timestamp").dt.truncate("1d").alias("day"))
        .len()
        .sort("day")
    )
    cutoff = daily["len"].median() * ratio
    start = None
    for day, count in daily.iter_rows():
        if count < cutoff:
            start = start or day
        else:
            start = None
    return start


@dataclass(frozen=True)
class TimeSplit:
    train: pl.DataFrame
    val: pl.DataFrame
    test: pl.DataFrame
    val_start: datetime
    test_start: datetime

    def summary(self) -> dict:
        n = self.train.height + self.val.height + self.test.height
        out = {"val_start": str(self.val_start), "test_start": str(self.test_start)}
        for name, part in (("train", self.train), ("val", self.val), ("test", self.test)):
            positives = int(part["is_laundering"].sum())
            out[name] = {
                "rows": part.height,
                "share": round(part.height / n, 4),
                "laundering": positives,
                "laundering_rate": round(positives / max(part.height, 1), 6),
            }
        return out


def time_split(
    df: pl.DataFrame, train_frac: float = TRAIN_FRAC, val_frac: float = VAL_FRAC
) -> TimeSplit:
    if not 0 < train_frac < train_frac + val_frac < 1:
        raise ValueError("need 0 < train_frac < train_frac + val_frac < 1")

    ts = df["timestamp"].sort()
    n = ts.len()
    val_start = ts[int(n * train_frac)]
    test_start = ts[int(n * (train_frac + val_frac))]
    if val_start == test_start:
        raise ValueError("validation window is empty; too few distinct timestamps")

    train = df.filter(pl.col("timestamp") < val_start)
    val = df.filter((pl.col("timestamp") >= val_start) & (pl.col("timestamp") < test_start))
    test = df.filter(pl.col("timestamp") >= test_start)
    return TimeSplit(train, val, test, val_start, test_start)
