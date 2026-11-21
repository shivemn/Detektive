from detektive_ml.explore import parse_patterns

PATTERNS = """BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 16-degree Fan-Out
2022/09/01 05:14,00952,8139F54E0,0111632,8062C56E0,5331.44,US Dollar,5331.44,US Dollar,ACH,1
END LAUNDERING ATTEMPT - FAN-OUT

BEGIN LAUNDERING ATTEMPT - CYCLE:  Max 12 hops
2022/09/01 06:00,001,A,002,B,10.00,US Dollar,10.00,US Dollar,ACH,1
END LAUNDERING ATTEMPT - CYCLE

BEGIN LAUNDERING ATTEMPT - STACK
END LAUNDERING ATTEMPT - STACK

BEGIN LAUNDERING ATTEMPT - FAN-OUT:  Max 4-degree Fan-Out
END LAUNDERING ATTEMPT - FAN-OUT
"""


def test_counts_attempts_per_pattern(tmp_path):
    path = tmp_path / "patterns.txt"
    path.write_text(PATTERNS)
    assert parse_patterns(path) == {"FAN-OUT": 2, "CYCLE": 1, "STACK": 1}
