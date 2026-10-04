"""Independent check of a cycle against the source data, without Memgraph.

Replaces test_cycle_existence.py, whose final ``FROM hop1, hop2, ...`` had no join
conditions: it returned the cross product of all hops, including combinations that run
backwards in time, and pytest would also try to run it (and scan the full CSV) on import.
Here each hop joins to the previous one, so every returned row is a real chronological
chain.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd

from aml.db import query_df


def verify_cycle_sql(n: int, *, max_window_seconds: int | None = None, limit: int = 1000) -> str:
    ctes = [
        "hop1 AS (SELECT tx_id AS tx_1, ts_epoch AS ts_1, Amount AS amount_1 FROM tx "
        "WHERE Sender_account = ? AND Receiver_account = ?)"
    ]
    window = ""
    if max_window_seconds is not None:
        window = f" AND t.ts_epoch <= h.ts_1 + {int(max_window_seconds)}"
    for i in range(2, n + 1):
        ctes.append(
            f"hop{i} AS (SELECT h.*, t.tx_id AS tx_{i}, t.ts_epoch AS ts_{i}, "
            f"t.Amount AS amount_{i} FROM hop{i - 1} h JOIN tx t "
            f"ON t.Sender_account = ? AND t.Receiver_account = ? "
            f"AND t.ts_epoch >= h.ts_{i - 1}{window})"
        )
    return (
        "WITH " + ",\n".join(ctes)
        + f"\nSELECT * FROM hop{n} ORDER BY ts_1, ts_{n}, tx_1 LIMIT {int(limit)}"
    )


def verify_cycle(
    con,
    accounts: Sequence[int],
    *,
    max_window_seconds: int | None = None,
    limit: int = 1000,
) -> pd.DataFrame:
    """Every chronological chain accounts[0] -> accounts[1] -> ... -> accounts[0]."""
    n = len(accounts)
    if n < 2:
        raise ValueError("a cycle needs at least two accounts")
    params: list[int] = []
    for i in range(n):
        params += [int(accounts[i]), int(accounts[(i + 1) % n])]
    sql = verify_cycle_sql(n, max_window_seconds=max_window_seconds, limit=limit)
    return query_df(con, sql, params)
