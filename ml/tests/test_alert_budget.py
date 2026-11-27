from datetime import date

import polars as pl

from detektive_ml.models.alert_budget import at_budgets, rules_alone


def toy() -> pl.DataFrame:
    # two days, five payments each; the laundering ones score highest on day 1
    return pl.DataFrame({
        "day": [date(2022, 9, 1)] * 5 + [date(2022, 9, 2)] * 5,
        "score": [0.9, 0.8, 0.1, 0.05, 0.0, 0.7, 0.6, 0.5, 0.2, 0.1],
        "is_laundering": [1, 1, 0, 0, 0, 0, 1, 0, 0, 1],
        "shortest_cycle": [3, 0, 0, 2, 0, 0, 0, 0, 0, 4],
        "scatter_gather": [0] * 10,
        "dst_fan_in": [1] * 10,
        "pair_count": [2] * 10,
        "payment_currency": ["USD"] * 10,
        "receiving_currency": ["USD"] * 10,
    })


def test_each_day_raises_its_own_top_k():
    t = at_budgets(toy(), pl.col("score"), budgets=(1, 2, 10))
    one, two, all_ = t.to_dicts()
    assert (one["alerts"], one["laundering"]) == (2, 1)  # 0.9 on day 1, 0.7 on day 2
    assert (two["alerts"], two["laundering"]) == (4, 3)
    assert two["recall"] == 3 / 4 and two["precision"] == 3 / 4
    # a payment scoring 0 never alerts, whatever the budget
    assert all_["alerts"] == 9 and all_["alerts_per_day"] == 4.5


def test_rules_rank_by_points_and_skip_what_does_not_fire():
    t = at_budgets(toy(), (pl.col("shortest_cycle") >= 3).cast(pl.Float64), budgets=(5,))
    assert t["alerts"][0] == 2 and t["laundering"][0] == 2


def test_rules_alone_counts_each_rule():
    r = rules_alone(toy()).filter(pl.col("rule") == "closes a ring of 3+ payments").row(0, named=True)
    assert (r["fires"], r["laundering"], r["recall"]) == (2, 2, 0.5)
