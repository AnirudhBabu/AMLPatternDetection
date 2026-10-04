"""Deposit-send: cash goes into an account and most of it leaves electronically soon after.

    cash deposit --> account --> non-cash payment to another account, within hours

This is the step where cash enters the banking system and is moved straight on, so it never
sits long enough to be questioned. The rule is deliberately narrow: one cash deposit, then a
non-cash payment from the same account to a different account that passes on 80-105% of
the deposit within 72 hours. Each deposit is paired with the first such payment after it.

Which Payment_type values count as cash deposits, and as cash, is read from the data unless
given explicitly. Only cash deposits are joined against later payments, using the same
time-bucket trick as scatter-gather, so the join stays small; the SQL is portable (DuckDB in
the pipeline, SQLite in the tests) and the Python side is vectorized.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from aml.db import query_df

HOUR = 3_600

OUTPUT_COLUMNS = [
    "Alert_ID", "Account", "Deposit_Tx_ID", "Deposit_Timestamp", "Deposit_Amount",
    "Deposit_Type", "Depositor", "Send_Tx_ID", "Send_Timestamp", "Send_Amount", "Send_Type",
    "Send_To", "Send_Location", "Send_Currency", "Hours_Held", "Forward_Ratio",
    "Account_Pairs", "Deposit_Label", "Send_Label",
]


@dataclass(frozen=True)
class DepositSendRules:
    max_hold_seconds: int = 72 * HOUR  # longest the cash may sit before it is sent on
    min_send_ratio: float = 0.8  # payment amount / deposit amount
    max_send_ratio: float = 1.05
    min_deposit: float = 0.0
    deposit_types: tuple[str, ...] | None = None  # None: types containing "cash" and "deposit"
    cash_types: tuple[str, ...] | None = None  # None: types containing "cash"
    cross_border_only: bool = False  # payment must go to a different bank location


def resolve_payment_types(con, rules: DepositSendRules) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(deposit types, cash types) to use, checked against the Payment_type values present."""
    present = sorted(
        str(t) for t in query_df(con, "SELECT DISTINCT Payment_type FROM tx").iloc[:, 0]
        if t is not None
    )
    deposits = tuple(
        t for t in present
        if (t in rules.deposit_types if rules.deposit_types
            else "cash" in t.lower() and "deposit" in t.lower())
    )
    if not deposits:
        raise ValueError(
            "no cash-deposit payment type found in the data. Payment types present: "
            + ", ".join(present) + ". Name the right one(s) with --deposit-types."
        )
    cash = tuple(
        t for t in present
        if t in deposits or (t in rules.cash_types if rules.cash_types else "cash" in t.lower())
    )
    return deposits, cash


_PAIR_JOIN = """
    SELECT d.Receiver_account AS account,
           d.tx_id AS deposit_tx, d.ts_epoch AS deposit_ts, d.Amount AS deposit_amount,
           d.Payment_type AS deposit_type, d.Sender_account AS depositor,
           s.tx_id AS send_tx, s.ts_epoch AS send_ts, s.Amount AS send_amount,
           s.Payment_type AS send_type, s.Receiver_account AS send_to,
           s.Receiver_bank_location AS send_location, s.Received_currency AS send_currency,
           d.Laundering_type AS deposit_label, s.Laundering_type AS send_label
    FROM deposits d
    JOIN sends s
      ON s.Sender_account = d.Receiver_account
     AND s.bucket = d.bucket{offset}
    WHERE s.ts_epoch >= d.ts_epoch
      AND s.ts_epoch <= d.ts_epoch + {hold}
      AND s.Amount >= d.Amount * ?
      AND s.Amount <= d.Amount * ?
"""


def pairs_sql(
    rules: DepositSendRules, deposit_types: tuple[str, ...], cash_types: tuple[str, ...]
) -> tuple[str, list]:
    """Every cash deposit with the first qualifying non-cash payment out after it."""
    hold = int(rules.max_hold_seconds)
    cross = (
        "\n              AND Sender_bank_location <> Receiver_bank_location"
        if rules.cross_border_only else ""
    )
    sql = f"""
        WITH moves AS (
            SELECT tx_id, ts_epoch, Sender_account, Receiver_account, Amount, Payment_type,
                   Sender_bank_location, Receiver_bank_location, Received_currency,
                   Laundering_type,
                   (ts_epoch - ts_epoch % {hold}) / {hold} AS bucket
            FROM tx
        ),
        deposits AS (
            SELECT * FROM moves
            WHERE Payment_type IN ({", ".join("?" * len(deposit_types))})
              AND Amount >= ?
        ),
        sends AS (
            SELECT * FROM moves
            WHERE Payment_type NOT IN ({", ".join("?" * len(cash_types))})
              AND Sender_account <> Receiver_account{cross}
        ),
        pairs AS (
            {_PAIR_JOIN.format(offset="", hold=hold)}
            UNION ALL
            {_PAIR_JOIN.format(offset=" + 1", hold=hold)}
        ),
        ranked AS (
            SELECT pairs.*,
                   ROW_NUMBER() OVER (PARTITION BY deposit_tx ORDER BY send_ts, send_tx) AS pick
            FROM pairs
        )
        SELECT * FROM ranked
        WHERE pick = 1
        ORDER BY deposit_ts, account, deposit_tx
    """
    ratios = [rules.min_send_ratio, rules.max_send_ratio]
    params = [*deposit_types, rules.min_deposit, *cash_types, *ratios, *ratios]
    return sql, params


def _timestamps(epoch_seconds: pd.Series) -> pd.Series:
    seconds = epoch_seconds.to_numpy(dtype="int64").astype("datetime64[s]")
    stamps = pd.Series(np.datetime_as_string(seconds, unit="s"), index=epoch_seconds.index)
    return stamps.str.replace("T", " ", regex=False)


def deposit_send_frame(pairs: pd.DataFrame) -> pd.DataFrame:
    """One row per deposit-send pair, built column-wise (no per-row Python)."""
    if pairs.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    p = pairs.sort_values(["deposit_ts", "account", "deposit_tx"]).reset_index(drop=True)
    frame = pd.DataFrame(
        {
            "Alert_ID": range(1, len(p) + 1),
            "Account": p["account"].astype("int64"),
            "Deposit_Tx_ID": p["deposit_tx"].astype("int64"),
            "Deposit_Timestamp": _timestamps(p["deposit_ts"]),
            "Deposit_Amount": p["deposit_amount"].astype(float),
            "Deposit_Type": p["deposit_type"],
            "Depositor": p["depositor"].astype("int64"),
            "Send_Tx_ID": p["send_tx"].astype("int64"),
            "Send_Timestamp": _timestamps(p["send_ts"]),
            "Send_Amount": p["send_amount"].astype(float),
            "Send_Type": p["send_type"],
            "Send_To": p["send_to"].astype("int64"),
            "Send_Location": p["send_location"],
            "Send_Currency": p["send_currency"],
            "Hours_Held": ((p["send_ts"] - p["deposit_ts"]) / HOUR).round(2),
            "Forward_Ratio": (p["send_amount"] / p["deposit_amount"]).round(4),
            "Account_Pairs": p.groupby("account")["deposit_tx"].transform("count").astype("int64"),
            "Deposit_Label": p["deposit_label"],
            "Send_Label": p["send_label"],
        }
    )
    return frame[OUTPUT_COLUMNS]


def detect_deposit_send(con, rules: DepositSendRules = DepositSendRules()) -> pd.DataFrame:
    """Full detector on a connection with a ``tx`` table/view (DuckDB in the pipeline)."""
    deposit_types, cash_types = resolve_payment_types(con, rules)
    pairs = query_df(con, *pairs_sql(rules, deposit_types, cash_types))
    return deposit_send_frame(pairs)
