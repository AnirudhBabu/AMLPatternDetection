"""Small helpers shared by the SQL steps.

The SQL in aml.verify, aml.evaluate and the episode stage of aml.smurfing sticks to a
portable subset, so the same query text runs on DuckDB (the real pipeline) and on SQLite
(the unit tests). Only the windowed COUNT(DISTINCT ...) in aml.smurfing is DuckDB-only.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd


def sql_literal(value: str | Path) -> str:
    """Quote a string (e.g. a file path) for inlining into SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def duckdb_connect():
    """A DuckDB connection without DuckDB's own progress bar, which draws over the spinner."""
    import duckdb

    # enable_progress_bar is a session (local) setting: DuckDB >= 1.5 rejects it in
    # connect(config=...), which only accepts global options, so SET it per connection.
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    return con


def open_transactions(parquet: Path):
    """DuckDB connection with a ``tx`` view over the prepared transactions."""
    con = duckdb_connect()
    con.execute(f"CREATE VIEW tx AS SELECT * FROM read_parquet({sql_literal(parquet)})")
    return con


def query_df(con, sql: str, params: Sequence = ()) -> pd.DataFrame:
    """Run a query on DuckDB or SQLite and return a DataFrame."""
    cursor = con.execute(sql, list(params))
    if hasattr(cursor, "df"):  # DuckDB: fast columnar fetch
        return cursor.df()
    columns = [d[0] for d in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def load_frame(con, name: str, frame: pd.DataFrame) -> None:
    """Materialise a DataFrame as a temporary table on DuckDB or SQLite."""
    if hasattr(con, "register"):  # DuckDB
        strings = [c for c in frame.columns if isinstance(frame[c].dtype, pd.StringDtype)]
        if strings:
            frame = frame.astype({c: object for c in strings})
        con.register("_aml_frame", frame)
        try:
            con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM _aml_frame")
        finally:
            con.unregister("_aml_frame")
    else:
        frame.to_sql(name, con, if_exists="replace", index=False)
