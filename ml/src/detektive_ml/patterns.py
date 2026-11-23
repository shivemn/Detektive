"""The laundering attempts listed in an IBM *_Patterns.txt file.

Each attempt is one pattern (CYCLE, FAN-IN, SCATTER-GATHER, ...) and the
transactions that make it up, written exactly as rows of the matching
*_Trans.csv file. Matching those rows back to the transactions file gives
each attempt's transaction ids, so detectors can be scored per attempt:
"did the engine see this cycle close?" rather than only per transaction.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Attempt:
    kind: str
    """CYCLE, FAN-IN, FAN-OUT, GATHER-SCATTER, SCATTER-GATHER, BIPARTITE, STACK or RANDOM."""
    detail: str
    """The rest of the BEGIN line, e.g. "Max 10 hops"."""
    rows: list[str] = field(default_factory=list)
    tx_ids: list[int] = field(default_factory=list)

    @property
    def max_hops(self) -> int | None:
        m = re.search(r"Max (\d+) hops", self.detail)
        return int(m.group(1)) if m else None


def parse(path: str | Path) -> list[Attempt]:
    attempts, current = [], None
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line.startswith("BEGIN LAUNDERING ATTEMPT"):
            kind, _, detail = line.split(" - ", 1)[1].partition(":")
            current = Attempt(kind.strip(), detail.strip())
        elif line.startswith("END LAUNDERING ATTEMPT"):
            attempts.append(current)
            current = None
        elif line and current is not None:
            current.rows.append(line)
    return attempts


def attach_tx_ids(attempts: list[Attempt], transactions_csv: str | Path) -> int:
    """Fill in each attempt's tx_ids (row numbers in the transactions file,
    as everywhere else in Detektive). Returns how many rows found no match."""
    rows: dict[str, list[int]] = {}
    with open(transactions_csv) as f:
        next(f)  # header
        for tx_id, line in enumerate(f):
            rows.setdefault(line.strip(), []).append(tx_id)
    missing = 0
    for a in attempts:
        a.tx_ids = []
        for row in a.rows:
            ids = rows.get(row)
            if ids:
                a.tx_ids.append(ids[0])
            else:
                missing += 1
    return missing
