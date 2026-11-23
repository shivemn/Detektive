from datetime import datetime, timedelta

import polars as pl

from detektive_ml.graph_check import recount


def frame(rows):
    """(minute, src, dst) rows -> the columns recount() reads, in time order."""
    start = datetime(2022, 9, 1)
    return pl.DataFrame(
        {
            "tx_id": list(range(len(rows))),
            "timestamp": [start + timedelta(minutes=m) for m, _, _ in rows],
            "src": [s for _, s, _ in rows],
            "dst": [d for _, _, d in rows],
        }
    )


def test_a_fan_in_then_a_payment_out():
    # five accounts pay "m", one of them twice; then "m" pays "x"
    rows = [(i, s, "m") for i, s in enumerate(["a", "b", "c", "d", "e", "a"])]
    rows.append((10, "m", "x"))
    last = recount(frame(rows), window_seconds=3600).row(-1, named=True)
    assert (last["src_in_degree"], last["src_fan_in"]) == (6, 5)
    assert (last["src_out_degree"], last["src_fan_out"]) == (1, 1)
    assert (last["dst_in_degree"], last["dst_out_degree"]) == (1, 0)
    assert last["pair_count"] == 1


def test_payments_older_than_the_window_are_left_out():
    rows = [(0, "a", "b"), (30, "a", "b"), (90, "a", "b")]
    f = recount(frame(rows), window_seconds=3600)
    # at minute 90 the window starts at minute 30, inclusive
    assert f["pair_count"].to_list() == [1, 2, 2]
    assert f["tx_id"].to_list() == [0, 1, 2]
