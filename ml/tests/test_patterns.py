from pathlib import Path

from detektive_ml.patterns import attach_tx_ids, parse

FIXTURE = Path(__file__).parent / "fixtures" / "tiny_trans.csv"

PATTERNS = """\
BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 2 hops
2022/09/01 00:20,3208,8000F4580,001,8000F5340,0.01,US Dollar,0.01,US Dollar,Cheque,0
2022/09/01 00:02,012,8000F5030,012,8000F5030,2806.97,US Dollar,2806.97,US Dollar,Reinvestment,1
END LAUNDERING ATTEMPT - CYCLE

BEGIN LAUNDERING ATTEMPT - FAN-IN:  Max 16-degree Fan-In
2022/09/01 00:00,3209,8000F4670,3209,8000F4670,14675.57,US Dollar,14675.57,US Dollar,Reinvestment,0
END LAUNDERING ATTEMPT - FAN-IN
"""


def test_reads_attempts_and_finds_their_transactions(tmp_path):
    path = tmp_path / "patterns.txt"
    path.write_text(PATTERNS)
    attempts = parse(path)
    assert [a.kind for a in attempts] == ["CYCLE", "FAN-IN"]
    assert attempts[0].max_hops == 2
    assert attempts[1].max_hops is None

    assert attach_tx_ids(attempts, FIXTURE) == 0
    # row numbers in the transactions file, header not counted
    assert attempts[0].tx_ids == [1, 3]
    assert attempts[1].tx_ids == [2]


def test_counts_rows_it_cannot_find(tmp_path):
    path = tmp_path / "patterns.txt"
    path.write_text(PATTERNS.replace("14675.57,US Dollar,14675.57", "1.00,US Dollar,1.00"))
    assert attach_tx_ids(parse(path), FIXTURE) == 1


def _row(src: str, dst: str) -> str:
    return f"2022/09/01 00:00,001,{src},001,{dst},1.00,US Dollar,1.00,US Dollar,ACH,1"


def test_widths_of_scatter_gather_and_gather_scatter():
    from detektive_ml.pattern_report import gather_scatter, scatter_gather_width
    from detektive_ml.patterns import Attempt

    # S pays A, B, C; each pays T
    sg = Attempt("SCATTER-GATHER", "", [_row("S", m) for m in "ABC"] + [_row(m, "T") for m in "ABC"])
    assert scatter_gather_width(sg) == 3

    # four accounts pay C; C pays two
    gs = Attempt("GATHER-SCATTER", "", [_row(p, "C") for p in "WXYZ"] + [_row("C", q) for q in "PQ"])
    assert gather_scatter(gs) == ("001:C", 2)
