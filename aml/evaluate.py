"""Score the detectors against SAML-D's ground truth (Is_laundering, Laundering_type).

Detections are joined back to the labels through ``tx_id``. Metrics are reported at two
levels: transactions (what was flagged) and accounts (who an analyst would investigate,
i.e. every sender/receiver touched by a flagged transaction).
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from aml import outputs
from aml.db import load_frame, query_df

CYCLING = "cycling"
SMURFING = "smurfing"
SCATTER_GATHER = "scatter-gather"
DEPOSIT_SEND = "deposit-send"
SWEEP_DAYS = (1, 3, 7, 14, 30)
SWEEP_MULES = (3, 4, 5, 6, 8, 10)
SWEEP_HOURS = (6, 12, 24, 48, 72)

BASE_SQL = """
SELECT COUNT(*) AS n_tx,
       SUM(CASE WHEN Is_laundering = 1 THEN 1 ELSE 0 END) AS n_laundering_tx
FROM tx
"""

ACCOUNT_BASE_SQL = """
WITH all_acct AS (
    SELECT Sender_account AS account FROM tx
    UNION
    SELECT Receiver_account FROM tx
),
bad_acct AS (
    SELECT Sender_account AS account FROM tx WHERE Is_laundering = 1
    UNION
    SELECT Receiver_account FROM tx WHERE Is_laundering = 1
)
SELECT (SELECT COUNT(*) FROM all_acct) AS n_accounts,
       (SELECT COUNT(*) FROM bad_acct) AS n_laundering_accounts
"""

TX_SQL = """
SELECT f.detector,
       COUNT(*) AS flagged_tx,
       SUM(CASE WHEN t.Is_laundering = 1 THEN 1 ELSE 0 END) AS laundering_tx
FROM (SELECT DISTINCT detector, tx_id FROM flagged) f
JOIN tx t ON t.tx_id = f.tx_id
GROUP BY f.detector
"""

_FLAGGED_ACCOUNTS = """
f AS (SELECT DISTINCT detector, tx_id FROM flagged),
flagged_acct AS (
    SELECT f.detector, t.Sender_account AS account FROM f JOIN tx t ON t.tx_id = f.tx_id
    UNION
    SELECT f.detector, t.Receiver_account FROM f JOIN tx t ON t.tx_id = f.tx_id
)
"""

ACCOUNTS_SQL = f"""
WITH {_FLAGGED_ACCOUNTS},
bad_acct AS (
    SELECT Sender_account AS account FROM tx WHERE Is_laundering = 1
    UNION
    SELECT Receiver_account FROM tx WHERE Is_laundering = 1
)
SELECT fa.detector,
       COUNT(*) AS flagged_accounts,
       SUM(CASE WHEN b.account IS NOT NULL THEN 1 ELSE 0 END) AS laundering_accounts
FROM flagged_acct fa
LEFT JOIN bad_acct b ON b.account = fa.account
GROUP BY fa.detector
"""

_TYPOLOGY_ACCOUNTS = """
typ_acct AS (
    SELECT Laundering_type AS typology, Sender_account AS account FROM tx WHERE Is_laundering = 1
    UNION
    SELECT Laundering_type, Receiver_account FROM tx WHERE Is_laundering = 1
)
"""

TYPOLOGY_TOTALS_SQL = f"""
WITH {_TYPOLOGY_ACCOUNTS}
SELECT tx_totals.typology, tx_totals.tx_total, acct_totals.acct_total
FROM (
    SELECT Laundering_type AS typology, COUNT(*) AS tx_total
    FROM tx WHERE Is_laundering = 1 GROUP BY Laundering_type
) tx_totals
JOIN (SELECT typology, COUNT(*) AS acct_total FROM typ_acct GROUP BY typology) acct_totals
  ON acct_totals.typology = tx_totals.typology
"""

TYPOLOGY_TX_SQL = """
SELECT f.detector, t.Laundering_type AS typology, COUNT(*) AS hits
FROM (SELECT DISTINCT detector, tx_id FROM flagged) f
JOIN tx t ON t.tx_id = f.tx_id
WHERE t.Is_laundering = 1
GROUP BY f.detector, t.Laundering_type
"""

TYPOLOGY_ACCOUNTS_SQL = f"""
WITH {_FLAGGED_ACCOUNTS}, {_TYPOLOGY_ACCOUNTS}
SELECT fa.detector, ta.typology, COUNT(*) AS hits
FROM typ_acct ta
JOIN flagged_acct fa ON fa.account = ta.account
GROUP BY fa.detector, ta.typology
"""


def _ratio(numerator, denominator) -> float:
    return float(numerator) / float(denominator) if denominator else math.nan


@dataclass
class Report:
    summary: pd.DataFrame
    by_typology: pd.DataFrame
    cycle_sweep: pd.DataFrame
    notes: list[str] = field(default_factory=list)
    scatter_gather_sweep: pd.DataFrame = field(default_factory=pd.DataFrame)
    deposit_send_sweep: pd.DataFrame = field(default_factory=pd.DataFrame)

    def write(self, out_dir: Path) -> list[Path]:
        """Write each table to its file in aml.outputs; drop files a table no longer has."""
        paths = []
        for key, frame in (
            ("summary", self.summary),
            ("by_typology", self.by_typology),
            ("cycle_sweep", self.cycle_sweep),
            ("scatter_gather_sweep", self.scatter_gather_sweep),
            ("deposit_send_sweep", self.deposit_send_sweep),
        ):
            path = outputs.path(out_dir, key)
            if frame.empty:
                path.unlink(missing_ok=True)  # e.g. a sweep for a detector that's gone
                continue
            frame.to_csv(path, index=False)
            paths.append(path)
        return paths

    def render(self) -> str:
        def pct(frame: pd.DataFrame) -> pd.DataFrame:
            shown = frame.copy()
            for column in shown.columns:
                if column.startswith(("precision", "recall")):
                    shown[column] = shown[column].map(
                        lambda v: "-" if pd.isna(v) else f"{100 * v:.1f}%"
                    )
                elif column.startswith("lift"):
                    shown[column] = shown[column].map(lambda v: "-" if pd.isna(v) else f"{v:,.1f}x")
            return shown

        parts = ["\n== Detector summary ==", pct(self.summary).to_string(index=False)]
        if not self.cycle_sweep.empty:
            parts += [
                "\n== Cycling: max cycle duration vs precision/recall ==",
                pct(self.cycle_sweep).to_string(index=False),
            ]
        if not self.scatter_gather_sweep.empty:
            parts += [
                "\n== Scatter-gather: minimum distinct mules vs precision/recall ==",
                pct(self.scatter_gather_sweep).to_string(index=False),
            ]
        if not self.deposit_send_sweep.empty:
            parts += [
                "\n== Deposit-send: max hours held vs precision/recall ==",
                pct(self.deposit_send_sweep).to_string(index=False),
            ]
        if not self.by_typology.empty:
            parts += [
                "\n== Recall by laundering typology ==",
                pct(self.by_typology).to_string(index=False),
            ]
        parts += [f"note: {note}" for note in self.notes]
        return "\n".join(parts)


def evaluate(
    con,
    *,
    cycles: pd.DataFrame | None = None,
    smurfing: pd.DataFrame | None = None,
    scatter_gather: pd.DataFrame | None = None,
    deposit_send: pd.DataFrame | None = None,
    cycle_label: str = "Cycle",
    scatter_gather_label: str = "Scatter-Gather",
    deposit_send_label: str = "Deposit-Send",
    sweep_days: tuple[int, ...] = SWEEP_DAYS,
    sweep_mules: tuple[int, ...] = SWEEP_MULES,
    sweep_hours: tuple[int, ...] = SWEEP_HOURS,
) -> Report:
    """Precision, lift and per-typology recall for each detector output given."""
    flagged_parts: list[pd.DataFrame] = []
    main: list[str] = []
    cycle_sweep: list[tuple[str, int | str]] = []
    mule_sweep: list[tuple[str, int | str]] = []
    hold_sweep: list[tuple[str, int | str]] = []

    def add(detector: str, tx_ids: pd.Series) -> None:
        flagged_parts.append(
            pd.DataFrame({"detector": detector, "tx_id": tx_ids.astype("int64").to_numpy()})
        )

    if cycles is not None:
        main.append(CYCLING)
        add(CYCLING, outputs.tx_ids("cycles", cycles))
        for days in sweep_days:
            name = f"{CYCLING} <= {days}d"
            add(name, cycles.loc[cycles["Cycle_Duration_Hours"] <= days * 24, "Tx_ID"])
            cycle_sweep.append((name, days))
        cycle_sweep.append((CYCLING, "any"))
    if smurfing is not None:
        main.append(SMURFING)
        add(SMURFING, outputs.tx_ids("smurfing", smurfing))
    if scatter_gather is not None:
        main.append(SCATTER_GATHER)
        add(SCATTER_GATHER, outputs.tx_ids("scatter_gather", scatter_gather))
        for mules in sweep_mules:
            name = f"{SCATTER_GATHER} >= {mules} mules"
            selected = scatter_gather[scatter_gather["Mule_count"] >= mules]
            add(name, outputs.tx_ids("scatter_gather", selected))
            mule_sweep.append((name, mules))
        mule_sweep.append((SCATTER_GATHER, "all"))
    if deposit_send is not None:
        main.append(DEPOSIT_SEND)
        add(DEPOSIT_SEND, outputs.tx_ids("deposit_send", deposit_send))
        for hours in sweep_hours:
            name = f"{DEPOSIT_SEND} <= {hours}h"
            held = deposit_send[deposit_send["Hours_Held"] <= hours]
            add(name, outputs.tx_ids("deposit_send", held))
            hold_sweep.append((name, hours))
        hold_sweep.append((DEPOSIT_SEND, "all"))

    flagged = (
        pd.concat(flagged_parts, ignore_index=True)
        if flagged_parts
        else pd.DataFrame({"detector": pd.Series(dtype=str), "tx_id": pd.Series(dtype="int64")})
    )
    load_frame(con, "flagged", flagged)

    base = query_df(con, BASE_SQL).iloc[0]
    acct_base = query_df(con, ACCOUNT_BASE_SQL).iloc[0]
    base_rate = _ratio(base["n_laundering_tx"] or 0, base["n_tx"])
    acct_base_rate = _ratio(acct_base["n_laundering_accounts"], acct_base["n_accounts"])

    tx = query_df(con, TX_SQL).set_index("detector")
    accts = query_df(con, ACCOUNTS_SQL).set_index("detector")
    totals = query_df(con, TYPOLOGY_TOTALS_SQL).set_index("typology").sort_index()
    typ_tx = query_df(con, TYPOLOGY_TX_SQL).set_index(["detector", "typology"])["hits"]
    typ_acct = query_df(con, TYPOLOGY_ACCOUNTS_SQL).set_index(["detector", "typology"])["hits"]

    def count(frame: pd.DataFrame, detector: str, column: str) -> int:
        if detector in frame.index:
            value = frame.at[detector, column]
            return 0 if pd.isna(value) else int(value)
        return 0

    def hits(series: pd.Series, detector: str, typology: str) -> int:
        return int(series.get((detector, typology), 0))

    def detector_row(detector: str) -> dict:
        flagged_tx = count(tx, detector, "flagged_tx")
        flagged_acct = count(accts, detector, "flagged_accounts")
        precision_tx = _ratio(count(tx, detector, "laundering_tx"), flagged_tx)
        precision_acct = _ratio(count(accts, detector, "laundering_accounts"), flagged_acct)
        return {
            "flagged_tx": flagged_tx,
            "precision_tx": precision_tx,
            "lift_tx": _ratio(precision_tx, base_rate),
            "flagged_accounts": flagged_acct,
            "precision_accounts": precision_acct,
            "lift_accounts": _ratio(precision_acct, acct_base_rate),
        }

    summary = pd.DataFrame([{"detector": d, **detector_row(d)} for d in main])

    typology_rows = []
    for typology, row in totals.iterrows():
        entry = {
            "typology": typology,
            "tx": int(row["tx_total"]),
            "accounts": int(row["acct_total"]),
        }
        for d in main:
            entry[f"recall_tx_{d}"] = _ratio(hits(typ_tx, d, typology), row["tx_total"])
            entry[f"recall_accounts_{d}"] = _ratio(hits(typ_acct, d, typology), row["acct_total"])
        typology_rows.append(entry)
    by_typology = pd.DataFrame(typology_rows)

    notes = [
        f"base rates: {100 * base_rate:.3f}% of transactions, "
        f"{100 * acct_base_rate:.2f}% of accounts touch laundering"
    ]

    def sweep_frame(
        entries: list[tuple[str, int | str]], labels: Sequence[str], key: str
    ) -> pd.DataFrame:
        if not entries:
            return pd.DataFrame()
        for label in labels:
            if label not in totals.index:
                notes.append(
                    f"typology {label!r} not in the data; pick one of: "
                    + ", ".join(map(str, totals.index))
                )
        rows = []
        for name, value in entries:
            row = detector_row(name)
            entry = {
                key: value,
                "flagged_tx": row["flagged_tx"],
                "precision_tx": row["precision_tx"],
                "flagged_accounts": row["flagged_accounts"],
                "precision_accounts": row["precision_accounts"],
            }
            for label in labels:
                known = label in totals.index
                tx_total = totals.at[label, "tx_total"] if known else 0
                acct_total = totals.at[label, "acct_total"] if known else 0
                entry[f"recall_tx_{label}"] = _ratio(hits(typ_tx, name, label), tx_total)
                entry[f"recall_accounts_{label}"] = _ratio(hits(typ_acct, name, label), acct_total)
            rows.append(entry)
        return pd.DataFrame(rows)

    return Report(
        summary,
        by_typology,
        sweep_frame(cycle_sweep, [cycle_label], "max_duration_days"),
        notes,
        sweep_frame(mule_sweep, [scatter_gather_label], "min_mules"),
        sweep_frame(hold_sweep, [deposit_send_label], "max_hours_held"),
    )
