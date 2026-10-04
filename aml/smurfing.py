"""Smurfing / fan-in bursts: many distinct senders paying one receiver inside a short window.

The original query required a receiver's *entire history* to fit inside the window, so any
account with ordinary activity before or after a burst could never be flagged. Here every
incoming transaction closes a rolling window; windows that clear the bars are merged into
episodes, and an episode's transactions are what gets reported.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from aml.db import load_frame, query_df
from aml.temporal import format_ts

DAY = 86_400

# Original smurfing_suspects.csv columns first, in the same order; new ones appended.
OUTPUT_COLUMNS = [
    "Receiver_account", "Duration_Days", "Date", "Time", "Total_amount", "Sender_account",
    "Sender_count", "Payment_currency", "Received_currency", "Amount", "Laundering_type",
    "Payment_type", "Episode_ID", "Episode_Start", "Episode_End", "Tx_ID", "Is_laundering",
]


@dataclass(frozen=True)
class SmurfRules:
    window_seconds: int = 30 * DAY
    min_distinct_senders: int = 10  # inclusive
    min_total: float = 100_000.0  # inclusive
    sender_currency: str | None = "UK pounds"  # None: any currency
    receiver_currency: str | None = "UK pounds"


def _currency_filter(alias: str, rules: SmurfRules) -> tuple[str, list]:
    clauses, params = [], []
    if rules.sender_currency is not None:
        clauses.append(f"{alias}.Payment_currency = ?")
        params.append(rules.sender_currency)
    if rules.receiver_currency is not None:
        clauses.append(f"{alias}.Received_currency = ?")
        params.append(rules.receiver_currency)
    return (" AND ".join(clauses) or "TRUE"), params


def windows_sql(rules: SmurfRules) -> tuple[str, list]:
    """Stage 1 (DuckDB): every window, ending at an incoming payment, that clears the bars.

    The ``candidates`` CTE is a cheap necessary condition (a receiver can't reach the bars
    in any window if it doesn't over its whole history), so the windowed DISTINCT count
    only runs for a small set of receivers.
    """
    where, params = _currency_filter("t", rules)
    w = int(rules.window_seconds)
    sql = f"""
        WITH base AS (
            SELECT t.ts_epoch, t.Sender_account, t.Receiver_account, t.Amount
            FROM tx t
            WHERE {where}
        ),
        candidates AS (
            SELECT Receiver_account
            FROM base
            GROUP BY Receiver_account
            HAVING COUNT(DISTINCT Sender_account) >= ? AND SUM(Amount) >= ?
        ),
        windows AS (
            SELECT Receiver_account,
                   ts_epoch AS window_end,
                   COUNT(DISTINCT Sender_account) OVER w AS distinct_senders,
                   SUM(Amount) OVER w AS window_total
            FROM base
            WHERE Receiver_account IN (SELECT Receiver_account FROM candidates)
            WINDOW w AS (
                PARTITION BY Receiver_account
                ORDER BY ts_epoch
                RANGE BETWEEN {w} PRECEDING AND CURRENT ROW
            )
        )
        SELECT DISTINCT Receiver_account,
               window_end - {w} AS window_start,
               window_end,
               distinct_senders,
               window_total
        FROM windows
        WHERE distinct_senders >= ? AND window_total >= ?
        ORDER BY Receiver_account, window_end
    """
    bars = [rules.min_distinct_senders, rules.min_total]
    return sql, params + bars + bars


def episodes_sql(rules: SmurfRules) -> tuple[str, list]:
    """Stage 2 (portable): merge overlapping flagged windows into episodes, list their rows.

    Reads the ``smurf_windows`` table that stage 1 produced.
    """
    where, params = _currency_filter("t", rules)
    sql = f"""
        WITH ordered AS (
            SELECT Receiver_account, window_start, window_end,
                   MAX(window_end) OVER (
                       PARTITION BY Receiver_account ORDER BY window_start, window_end
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS prev_end
            FROM smurf_windows
        ),
        numbered AS (
            SELECT Receiver_account, window_start, window_end,
                   SUM(CASE WHEN prev_end IS NOT NULL AND window_start <= prev_end
                            THEN 0 ELSE 1 END) OVER (
                       PARTITION BY Receiver_account ORDER BY window_start, window_end
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS episode_no
            FROM ordered
        ),
        episodes AS (
            SELECT ROW_NUMBER() OVER (ORDER BY Receiver_account, MIN(window_start)) AS episode_id,
                   Receiver_account,
                   MIN(window_start) AS episode_start,
                   MAX(window_end) AS episode_end
            FROM numbered
            GROUP BY Receiver_account, episode_no
        ),
        episode_tx AS (
            SELECT e.episode_id, t.tx_id, t.ts_epoch, t."Date", t."Time", t.Sender_account,
                   t.Receiver_account, t.Amount, t.Payment_currency, t.Received_currency,
                   t.Payment_type, t.Laundering_type, t.Is_laundering
            FROM episodes e
            JOIN tx t
              ON t.Receiver_account = e.Receiver_account
             AND t.ts_epoch BETWEEN e.episode_start AND e.episode_end
            WHERE {where}
        ),
        stats AS (
            SELECT episode_id,
                   COUNT(DISTINCT Sender_account) AS Sender_count,
                   SUM(Amount) AS Total_amount,
                   CAST(MAX(ts_epoch) - MIN(ts_epoch) AS DOUBLE) / 86400 AS Duration_Days,
                   MIN(ts_epoch) AS first_ts,
                   MAX(ts_epoch) AS last_ts
            FROM episode_tx
            GROUP BY episode_id
        )
        SELECT x.Receiver_account, s.Duration_Days, x."Date", x."Time", s.Total_amount,
               x.Sender_account, s.Sender_count, x.Payment_currency, x.Received_currency,
               x.Amount, x.Laundering_type, x.Payment_type,
               x.episode_id AS Episode_ID, s.first_ts AS Episode_Start,
               s.last_ts AS Episode_End, x.tx_id AS Tx_ID, x.Is_laundering
        FROM episode_tx x
        JOIN stats s ON s.episode_id = x.episode_id
        ORDER BY x.Receiver_account, x.episode_id, x.ts_epoch, x.tx_id
    """
    return sql, params


def episodes_from_windows(con, windows: pd.DataFrame, rules: SmurfRules) -> pd.DataFrame:
    """Stage 2 on any backend, given stage-1 windows."""
    load_frame(con, "smurf_windows", windows)
    frame = query_df(con, *episodes_sql(rules))
    for column in ("Episode_Start", "Episode_End"):
        frame[column] = frame[column].map(format_ts)
    return frame[OUTPUT_COLUMNS]


def detect_smurfing(con, rules: SmurfRules = SmurfRules()) -> pd.DataFrame:
    """Full detector on a DuckDB connection that has a ``tx`` view (see aml.db)."""
    windows = query_df(con, *windows_sql(rules))
    return episodes_from_windows(con, windows, rules)
