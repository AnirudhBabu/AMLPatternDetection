"""Scatter-gather (layering through mules): one source pays several intermediaries, and each
of them passes the money on to the same destination soon after receiving it.

    S --> M1 --> D
    S --> M2 --> D
    S --> M3 --> D

This is a fixed two-hop shape, which a relational self-join handles well, so it runs in DuckDB
rather than Memgraph. Memgraph is for the variable-length search behind cycles. The join is
cut into time buckets so busy accounts can't blow it up. Grouping the resulting legs into
time windows is plain Python with unit tests.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import pandas as pd

from aml.db import query_df
from aml.temporal import format_ts

DAY = 86_400

OUTPUT_COLUMNS = [
    "Episode_ID", "Source_account", "Mule_account", "Destination_account",
    "In_Tx_ID", "In_Timestamp", "In_Amount", "Out_Tx_ID", "Out_Timestamp", "Out_Amount",
    "Forward_Ratio", "Hold_Hours", "Mule_count", "Episode_Start", "Episode_End",
    "Episode_Duration_Days", "Total_scattered", "Total_gathered", "Gather_Ratio",
]


@dataclass(frozen=True)
class ScatterGatherRules:
    hop_seconds: int = 7 * DAY  # longest a mule holds the money before passing it on
    episode_seconds: int = 30 * DAY  # every leg of one episode falls inside this window
    min_mules: int = 3  # distinct intermediaries, inclusive
    min_forward_ratio: float = 0.5  # amount passed on / amount received
    max_forward_ratio: float = 1.05


@dataclass(frozen=True, slots=True)
class Leg:
    """Money in from the source, then out to the destination, through one mule."""

    source: int
    mule: int
    dest: int
    in_tx: int
    out_tx: int
    t_in: int
    t_out: int
    amount_in: float
    amount_out: float


_LEG_JOIN = """
    SELECT a.Sender_account AS source, a.Receiver_account AS mule,
           b.Receiver_account AS dest, a.tx_id AS in_tx, b.tx_id AS out_tx,
           a.ts_epoch AS t_in, b.ts_epoch AS t_out,
           a.Amount AS amount_in, b.Amount AS amount_out
    FROM moves a
    JOIN moves b
      ON b.Sender_account = a.Receiver_account
     AND b.bucket = a.bucket{offset}
    WHERE b.ts_epoch >= a.ts_epoch
      AND b.ts_epoch <= a.ts_epoch + {hop}
      AND b.Receiver_account <> a.Sender_account
      AND b.Amount >= a.Amount * ?
      AND b.Amount <= a.Amount * ?
"""


def legs_sql(rules: ScatterGatherRules) -> tuple[str, list]:
    """Every pass-through leg S -> M -> D, for (S, D) pairs with enough distinct mules.

    Portable SQL (DuckDB and SQLite). Joining on (account, time bucket of width ``hop``) and
    on (account, next bucket) covers every pair of transfers at most ``hop`` apart, without
    pairing every incoming payment of a busy account with every outgoing one.
    """
    hop = int(rules.hop_seconds)
    same_bucket = _LEG_JOIN.format(offset="", hop=hop)
    next_bucket = _LEG_JOIN.format(offset=" + 1", hop=hop)
    sql = f"""
        WITH moves AS (
            SELECT tx_id, ts_epoch, Sender_account, Receiver_account, Amount,
                   (ts_epoch - ts_epoch % {hop}) / {hop} AS bucket
            FROM tx
            WHERE Sender_account <> Receiver_account
        ),
        legs AS MATERIALIZED (
            {same_bucket}
            UNION ALL
            {next_bucket}
        ),
        pairs AS (
            SELECT source, dest
            FROM legs
            GROUP BY source, dest
            HAVING COUNT(DISTINCT mule) >= ?
        )
        SELECT l.source, l.mule, l.dest, l.in_tx, l.out_tx, l.t_in, l.t_out,
               l.amount_in, l.amount_out
        FROM legs l
        JOIN pairs p ON p.source = l.source AND p.dest = l.dest
        ORDER BY l.source, l.dest, l.t_out, l.t_in, l.in_tx, l.out_tx
    """
    ratios = [rules.min_forward_ratio, rules.max_forward_ratio]
    return sql, ratios + ratios + [rules.min_mules]


def find_episodes(legs: Sequence[Leg], rules: ScatterGatherRules) -> list[list[Leg]]:
    """Group the legs of one (source, destination) pair into episodes.

    Every leg's outgoing payment closes a window of ``rules.episode_seconds``. A window whose
    legs (both payments inside it) run through at least ``rules.min_mules`` distinct mules
    is suspicious, and overlapping suspicious windows merge into one episode.
    """
    ordered = sorted(legs, key=lambda leg: (leg.t_out, leg.t_in, leg.in_tx, leg.out_tx))
    spans: list[list[int]] = []
    for anchor in ordered:
        end = anchor.t_out
        start = end - rules.episode_seconds
        mules = {leg.mule for leg in ordered if leg.t_in >= start and leg.t_out <= end}
        if len(mules) < rules.min_mules:
            continue
        if spans and start <= spans[-1][1]:
            spans[-1][1] = end  # anchors come in time order, so this only ever extends
        else:
            spans.append([start, end])
    return [[leg for leg in ordered if leg.t_in >= s and leg.t_out <= e] for s, e in spans]


def group_episodes(legs: Iterable[Leg], rules: ScatterGatherRules) -> list[list[Leg]]:
    by_pair: dict[tuple[int, int], list[Leg]] = defaultdict(list)
    for leg in legs:
        by_pair[(leg.source, leg.dest)].append(leg)
    episodes = [ep for pair_legs in by_pair.values() for ep in find_episodes(pair_legs, rules)]
    episodes.sort(key=lambda ep: (min(leg.t_in for leg in ep), ep[0].source, ep[0].dest))
    return episodes


def scatter_gather_frame(episodes: list[list[Leg]]) -> pd.DataFrame:
    """One row per leg, with episode-level totals repeated on each row."""
    rows = []
    for episode_id, legs in enumerate(episodes, start=1):
        start = min(leg.t_in for leg in legs)
        end = max(leg.t_out for leg in legs)
        scattered = sum({leg.in_tx: leg.amount_in for leg in legs}.values())
        gathered = sum({leg.out_tx: leg.amount_out for leg in legs}.values())
        mules = len({leg.mule for leg in legs})
        for leg in legs:
            rows.append(
                {
                    "Episode_ID": episode_id,
                    "Source_account": leg.source,
                    "Mule_account": leg.mule,
                    "Destination_account": leg.dest,
                    "In_Tx_ID": leg.in_tx,
                    "In_Timestamp": format_ts(leg.t_in),
                    "In_Amount": leg.amount_in,
                    "Out_Tx_ID": leg.out_tx,
                    "Out_Timestamp": format_ts(leg.t_out),
                    "Out_Amount": leg.amount_out,
                    "Forward_Ratio": round(leg.amount_out / leg.amount_in, 4),
                    "Hold_Hours": round((leg.t_out - leg.t_in) / 3600, 2),
                    "Mule_count": mules,
                    "Episode_Start": format_ts(start),
                    "Episode_End": format_ts(end),
                    "Episode_Duration_Days": round((end - start) / DAY, 2),
                    "Total_scattered": scattered,
                    "Total_gathered": gathered,
                    "Gather_Ratio": round(gathered / scattered, 4) if scattered else None,
                }
            )
    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def detect_scatter_gather(con, rules: ScatterGatherRules = ScatterGatherRules()) -> pd.DataFrame:
    """Full detector on a connection with a ``tx`` table/view (DuckDB in the pipeline)."""
    frame = query_df(con, *legs_sql(rules))
    legs = [
        Leg(int(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5]), int(r[6]),
            float(r[7]), float(r[8]))
        for r in frame.itertuples(index=False, name=None)
    ]
    return scatter_gather_frame(group_episodes(legs, rules))
