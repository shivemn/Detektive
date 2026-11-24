"""Check the Rust engine's graph features against NetworkX.

`graph_check` recounts the degree features with numpy. This goes further and
uses a different, well-known graph library for everything structural: for
every payment, in time order, it keeps the payments of the window in a
NetworkX MultiDiGraph and asks NetworkX for

- degrees and distinct counterparties (`out_degree`, `successors`, ...),
- repeats between the pair (`number_of_edges`),
- the cycles the payment closes: every simple path from the receiver back
  to the sender (`all_simple_paths`), kept if its payments can run in time
  order,
- the widest scatter-gather the payment is part of, from neighbour sets.

None of the engine's ideas (adjacency lists, expiry index, per-counterparty
counts, backward search, latest-payment-per-pair) are used here, so if the
two agree, the engine's shortcuts are not changing the answers.

    detektive-graph sample.csv --window-hours 6 --max-cycle-len 5 --features features.csv
    python -m detektive_ml.nx_check sample.csv features.csv --window-hours 6 --max-cycle-len 5

Slow (NetworkX is pure Python); meant for samples of a few thousand
payments. Exits non-zero, printing the first disagreements, if any differ.
"""

import argparse
import sys
from collections import deque
from pathlib import Path

import networkx as nx
import polars as pl

from detektive_ml.data import read_raw_csv

CHECKED = [
    "src_out_degree", "src_fan_out", "src_in_degree", "src_fan_in",
    "dst_in_degree", "dst_fan_in", "dst_out_degree", "dst_fan_out",
    "pair_count", "cycles", "shortest_cycle", "scatter_gather",
    "src_gather_scatter", "dst_gather_scatter",
]


def runs_in_time_order(g: nx.MultiDiGraph, path, until: int, own_key) -> bool:
    """Whether the path's hops can be taken by payments in time order, all
    no later than `until`, without the payment keyed `own_key`. Taking the
    earliest usable payment at each hop leaves the most room for the next,
    so this greedy choice finds a run whenever one exists."""
    t = None
    for a, b in zip(path, path[1:]):
        times = [e["ts"] for k, e in g.get_edge_data(a, b).items() if k != own_key]
        usable = [x for x in times if (t is None or x >= t) and x <= until]
        if not usable:
            return False
        t = min(usable)
    return True


def middles(g: nx.MultiDiGraph, s, t) -> int:
    if s == t:
        return 0
    return len((set(g.successors(s)) & set(g.predecessors(t))) - {s, t})


def recount(transactions: pl.DataFrame, window_seconds: int, max_len: int) -> pl.DataFrame:
    g = nx.MultiDiGraph()
    live = deque()  # (ts, src, dst, key) in insertion order
    rows = []
    ts_all = transactions["timestamp"].dt.epoch("s").to_list()
    for tx, ts, s, d in zip(transactions["tx_id"].to_list(), ts_all,
                            transactions["src"].to_list(), transactions["dst"].to_list()):
        # the window at this payment: no payment older than ts - window
        while live and live[0][0] < ts - window_seconds:
            _, a, b, key = live.popleft()
            g.remove_edge(a, b, key=key)
        g.add_edge(s, d, key=tx, ts=ts)
        live.append((ts, s, d, tx))

        cycles = []
        if s != d:
            for path in {tuple(p) for p in nx.all_simple_paths(g, d, s, cutoff=max_len - 1)}:
                # the path must not use this payment itself
                if runs_in_time_order(g, path, ts, tx):
                    cycles.append(len(path))
        sg = 0
        if s != d:
            sg = max(
                [middles(g, a, d) for a in set(g.predecessors(s)) if a != s]
                + [middles(g, s, b) for b in set(g.successors(d)) if b != d]
                + [0]
            )
        fan = lambda nodes: len(set(nodes))  # noqa: E731
        rows.append({
            "tx_id": tx,
            "src_out_degree": g.out_degree(s),
            "src_fan_out": fan(g.successors(s)),
            "src_in_degree": g.in_degree(s),
            "src_fan_in": fan(g.predecessors(s)),
            "dst_in_degree": g.in_degree(d),
            "dst_fan_in": fan(g.predecessors(d)),
            "dst_out_degree": g.out_degree(d),
            "dst_fan_out": fan(g.successors(d)),
            "pair_count": g.number_of_edges(s, d),
            "cycles": len(cycles),
            "shortest_cycle": min(cycles, default=0),
            "scatter_gather": sg,
            "src_gather_scatter": min(fan(g.predecessors(s)), fan(g.successors(s))),
            "dst_gather_scatter": min(fan(g.predecessors(d)), fan(g.successors(d))),
        })
    return pl.DataFrame(rows)


def compare(transactions_csv: Path, features_csv: Path, window_hours: float, max_len: int) -> int:
    transactions = read_raw_csv(transactions_csv).with_columns(pl.col("tx_id").cast(pl.Int64))
    expected = recount(transactions, round(window_hours * 3600), max_len)
    engine = pl.read_csv(features_csv).select(["tx_id", *CHECKED])
    joined = expected.join(engine, on="tx_id", how="left", suffix="_engine")
    differs = pl.any_horizontal([pl.col(c) != pl.col(f"{c}_engine") for c in CHECKED])
    bad = joined.filter(differs | pl.col("cycles_engine").is_null())
    closing = expected.filter(pl.col("cycles") > 0).height
    wide = expected.filter(pl.col("scatter_gather") >= 3).height
    if bad.height:
        print(f"{bad.height} of {joined.height} payments differ; first few:")
        with pl.Config(tbl_cols=-1):
            print(bad.head(5))
    else:
        print(
            f"all {joined.height} payments match NetworkX on {len(CHECKED)} features "
            f"({closing} close a cycle, {wide} are in a scatter-gather 3 or more wide)"
        )
    return bad.height


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("transactions", type=Path)
    parser.add_argument("features", type=Path)
    parser.add_argument("--window-hours", type=float, default=6.0)
    parser.add_argument("--max-cycle-len", type=int, default=5)
    args = parser.parse_args()
    sys.exit(1 if compare(args.transactions, args.features, args.window_hours, args.max_cycle_len) else 0)


if __name__ == "__main__":
    main()
