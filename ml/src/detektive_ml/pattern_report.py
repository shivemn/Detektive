"""How much of the dataset's laundering patterns the engine sees.

    detektive-graph data/raw/HI-Small_Trans.csv --features features.csv
    python -m detektive_ml.pattern_report data/raw/HI-Small_Trans.csv \\
        data/raw/HI-Small_Patterns.txt features.csv

Scores the engine per laundering attempt listed in the patterns file:

- CYCLE: caught if the attempt's last payment (in time order) closes at
  least one cycle;
- SCATTER-GATHER: the width the engine reports at the attempt's last
  payment, against the attempt's own width (accounts between its source and
  target);
- GATHER-SCATTER: the gather-scatter the engine reports for the centre's
  payments out, against the attempt's own (the smaller of its payers and
  payees).

Then, for each kind of feature, how many of the payments it flags are
laundering, against the base rate.

This reads the labels, so it is evaluation only: nothing here feeds the
engine.
"""

import argparse
from collections import Counter
from pathlib import Path

import polars as pl

from detektive_ml.data import read_raw_csv
from detektive_ml.patterns import Attempt, attach_tx_ids, parse


def length_band(n: int) -> str:
    return "2" if n == 2 else "3-4" if n <= 4 else "5-8" if n <= 8 else "9-12"


def _accounts(row: str) -> tuple[str, str]:
    f = row.split(",")
    return f[1] + ":" + f[2], f[3] + ":" + f[4]


def scatter_gather_width(a: Attempt) -> int:
    """Accounts between the attempt's source and target."""
    edges = [_accounts(r) for r in a.rows]
    source = Counter(s for s, _ in edges).most_common(1)[0][0]
    target = Counter(t for _, t in edges).most_common(1)[0][0]
    paid = {t for s, t in edges if s == source}
    paid_target = {s for s, t in edges if t == target}
    return len((paid & paid_target) - {source, target})


def gather_scatter(a: Attempt) -> tuple[str, int]:
    """The centre account and the smaller of its payers and payees."""
    edges = [_accounts(r) for r in a.rows]
    centre = Counter(t for _, t in edges).most_common(1)[0][0]
    payers = {s for s, t in edges if t == centre and s != centre}
    payees = {t for s, t in edges if s == centre and t != centre}
    return centre, min(len(payers), len(payees))


def report(transactions_csv: Path, patterns: Path, features_csv: Path) -> dict[str, pl.DataFrame]:
    tx = read_raw_csv(transactions_csv).select(
        pl.col("tx_id").cast(pl.Int64), "timestamp", "is_laundering", "src"
    )
    features = pl.read_csv(features_csv).join(tx, on="tx_id")

    attempts = parse(patterns)
    missing = attach_tx_ids(attempts, transactions_csv)
    assert missing == 0, f"{missing} pattern rows not found in {transactions_csv}"
    # only the attempts' own payments are needed row by row
    wanted = {t for a in attempts for t in a.tx_ids}
    by_tx = {
        r["tx_id"]: r
        for r in features.filter(pl.col("tx_id").is_in(list(wanted))).iter_rows(named=True)
    }

    def order(t: int):
        return by_tx[t]["timestamp"], t

    out = {}

    rows = []
    for a in (a for a in attempts if a.kind == "CYCLE"):
        last = max(a.tx_ids, key=order)
        rows.append({"band": length_band(len(a.tx_ids)), "caught": by_tx[last]["cycles"] > 0})
    cycles = pl.DataFrame(rows)
    out["cycles"] = (
        cycles.group_by("band")
        .agg(pl.len().alias("attempts"), pl.col("caught").sum().alias("caught"))
        .sort("band")
    )
    print(f"CYCLE attempts caught as they close: {cycles['caught'].sum()} of {cycles.height}")
    print(out["cycles"])

    rows = []
    for a in (a for a in attempts if a.kind == "SCATTER-GATHER"):
        last = max(a.tx_ids, key=order)
        rows.append({
            "true_width": scatter_gather_width(a),
            "seen_at_last": by_tx[last]["scatter_gather"],
            "widest_seen": max(by_tx[t]["scatter_gather"] for t in a.tx_ids),
        })
    sg = pl.DataFrame(rows)
    out["scatter_gather"] = sg
    full = sg.filter(pl.col("seen_at_last") >= pl.col("true_width")).height
    wide = sg.filter(pl.col("true_width") >= 3)
    print(
        f"\nSCATTER-GATHER attempts: {sg.height}; the last payment sees the full width in {full}; "
        f"of the {wide.height} with 3 or more in the middle, {wide.filter(pl.col('seen_at_last') >= 3).height} "
        "show 3 or more at the last payment"
    )

    rows = []
    for a in (a for a in attempts if a.kind == "GATHER-SCATTER"):
        centre, true = gather_scatter(a)
        outs = [t for t in a.tx_ids if by_tx[t]["src"] == centre]
        seen = max((by_tx[t]["src_gather_scatter"] for t in outs), default=0)
        rows.append({"true": true, "seen": seen})
    gs = pl.DataFrame(rows)
    out["gather_scatter"] = gs
    print(
        f"GATHER-SCATTER attempts: {gs.height}; the centre's payments out show the full "
        f"gather-scatter in {gs.filter(pl.col('seen') >= pl.col('true')).height}"
    )

    base = features["is_laundering"].mean()
    print(f"\nlaundering among all payments: {base:.3%}")
    for name, cond in [
        ("payments closing a cycle", pl.col("cycles") > 0),
        ("  shortest cycle 2", pl.col("shortest_cycle") == 2),
        ("  shortest cycle 3+", pl.col("shortest_cycle") >= 3),
        ("  3+, ring not closed before in the window", (pl.col("shortest_cycle") >= 3) & (pl.col("ring_repeats") == 0)),
        ("  3+, ring closed 3+ times before", (pl.col("shortest_cycle") >= 3) & (pl.col("ring_repeats") >= 3)),
        ("payments by or to an account in a cycle", (pl.col("src_cycles") > 0) | (pl.col("dst_cycles") > 0)),
        ("scatter-gather width 2+", pl.col("scatter_gather") >= 2),
        ("scatter-gather width 3+", pl.col("scatter_gather") >= 3),
        ("scatter-gather width 5+", pl.col("scatter_gather") >= 5),
        ("sender gather-scatter 5+", pl.col("src_gather_scatter") >= 5),
    ]:
        part = features.filter(cond)
        rate = part["is_laundering"].mean() if part.height else 0.0
        print(f"laundering among {name}: {rate:.2%} of {part.height:,}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("transactions", type=Path)
    parser.add_argument("patterns", type=Path)
    parser.add_argument("features", type=Path)
    args = parser.parse_args()
    report(args.transactions, args.patterns, args.features)


if __name__ == "__main__":
    main()
