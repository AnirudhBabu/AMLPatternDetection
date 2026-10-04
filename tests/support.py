"""Helpers shared by the tests: tiny SAML-D-shaped tables on SQLite and (if installed) DuckDB."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pandas as pd

from aml.db import load_frame

try:
    import duckdb
except ImportError:  # the portable-SQL tests still run on SQLite
    duckdb = None

DAY = 86_400
HOUR = 3_600
T0 = int(datetime(2022, 10, 7, tzinfo=timezone.utc).timestamp())

DEFAULTS = {
    "Amount": 100.0,
    "Payment_currency": "UK pounds",
    "Received_currency": "UK pounds",
    "Payment_type": "Cross-border",
    "Is_laundering": 0,
    "Laundering_type": "Normal_Small_Fan_Out",
    "Sender_bank_location": "UK",
    "Receiver_bank_location": "UK",
}


def tx_frame(rows: list[dict]) -> pd.DataFrame:
    """Prepared-schema transactions. Each row needs Sender_account, Receiver_account, ts_epoch."""
    records = []
    for i, row in enumerate(rows, start=1):
        record = {**DEFAULTS, **row}
        record.setdefault("tx_id", i)
        moment = datetime.fromtimestamp(record["ts_epoch"], tz=timezone.utc)
        record["Date"] = moment.strftime("%Y-%m-%d")
        record["Time"] = moment.strftime("%H:%M:%S")
        records.append(record)
    columns = [
        "tx_id", "ts_epoch", "Time", "Date", "Sender_account", "Receiver_account", "Amount",
        "Payment_currency", "Received_currency", "Payment_type", "Is_laundering",
        "Laundering_type", "Sender_bank_location", "Receiver_bank_location",
    ]
    return pd.DataFrame(records, columns=columns)


def sqlite_backend(frame: pd.DataFrame):
    con = sqlite3.connect(":memory:")
    frame.to_sql("tx", con, index=False)
    return con


def duckdb_backend(frame: pd.DataFrame):
    con = duckdb.connect()
    load_frame(con, "tx", frame)
    return con


def backends(frame: pd.DataFrame):
    """(name, connection) for every SQL engine available here."""
    yield "sqlite", sqlite_backend(frame)
    if duckdb is not None:
        yield "duckdb", duckdb_backend(frame)
