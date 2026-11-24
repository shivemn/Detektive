from datetime import datetime, timedelta

import polars as pl

from detektive_ml.nx_check import recount


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


def test_a_ring_closes_only_in_time_order():
    in_order = recount(frame([(0, "a", "b"), (1, "b", "c"), (2, "c", "a")]), 3600, 5)
    assert in_order["cycles"].to_list() == [0, 0, 1]
    assert in_order["shortest_cycle"][-1] == 3
    # b pays c before a pays b: the money could not have gone round
    backwards = recount(frame([(1, "a", "b"), (0, "b", "c"), (2, "c", "a")]).sort("timestamp"), 3600, 5)
    assert backwards["cycles"].to_list() == [0, 0, 0]


def test_the_widest_scatter_gather():
    rows = [(i, "s", m) for i, m in enumerate("xyz")] + [(10 + i, m, "t") for i, m in enumerate("xyz")]
    f = recount(frame(rows), 3600, 5)
    assert f["scatter_gather"][-1] == 3
