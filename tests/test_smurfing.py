import random

import pandas as pd
import pytest
from support import DAY, HOUR, T0, backends, duckdb, duckdb_backend, tx_frame

from aml.db import query_df
from aml.smurfing import OUTPUT_COLUMNS, SmurfRules, episodes_from_windows, windows_sql

RULES = SmurfRules()  # 30 days, >= 10 senders, >= 100,000, UK pounds both ways
MULE, OTHER, EXACT = 100, 500, 900


def reference_windows(frame: pd.DataFrame, rules: SmurfRules) -> pd.DataFrame:
    """Brute force: every incoming payment closes a window [t - W, t]."""
    rows = frame
    if rules.sender_currency is not None:
        rows = rows[rows.Payment_currency == rules.sender_currency]
    if rules.receiver_currency is not None:
        rows = rows[rows.Received_currency == rules.receiver_currency]
    w = rules.window_seconds
    out = set()
    for receiver, group in rows.groupby("Receiver_account"):
        for end in group.ts_epoch.unique():
            window = group[(group.ts_epoch >= end - w) & (group.ts_epoch <= end)]
            senders, total = window.Sender_account.nunique(), window.Amount.sum()
            if senders >= rules.min_distinct_senders and total >= rules.min_total:
                out.add((int(receiver), int(end - w), int(end), int(senders), float(total)))
    columns = ["Receiver_account", "window_start", "window_end", "distinct_senders", "window_total"]
    return pd.DataFrame(sorted(out), columns=columns)


def scenario() -> pd.DataFrame:
    rows = [
        # MULE: ordinary payments months apart, with a 12-sender burst in between
        {"Sender_account": 1, "Receiver_account": MULE, "ts_epoch": T0, "Amount": 50.0},
        {"Sender_account": 2, "Receiver_account": MULE, "ts_epoch": T0 + 200 * DAY, "Amount": 60.0},
        *[
            {"Sender_account": 200 + i, "Receiver_account": MULE, "ts_epoch": T0 + (60 + i) * DAY,
             "Amount": 9500.0, "Is_laundering": 1, "Laundering_type": "Smurfing"}
            for i in range(12)
        ],
        # same burst period, but paid in another currency: filtered out
        {"Sender_account": 300, "Receiver_account": MULE, "ts_epoch": T0 + 65 * DAY,
         "Amount": 9000.0, "Payment_currency": "US dollar"},
        # OTHER: only five senders
        *[
            {"Sender_account": 400 + i, "Receiver_account": OTHER, "ts_epoch": T0 + i * HOUR,
             "Amount": 50_000.0}
            for i in range(5)
        ],
        # EXACT: exactly 10 senders and exactly 100,000 - the bars are inclusive
        *[
            {"Sender_account": 600 + i, "Receiver_account": EXACT, "ts_epoch": T0 + i * DAY,
             "Amount": 10_000.0}
            for i in range(10)
        ],
    ]
    return tx_frame(rows)


def legacy_rule(frame: pd.DataFrame, receiver: int) -> bool:
    """The original query: the receiver's *whole* history must fit inside the window."""
    rows = frame[(frame.Receiver_account == receiver) & (frame.Payment_currency == "UK pounds")]
    span = rows.ts_epoch.max() - rows.ts_epoch.min()
    return bool(
        rows.Sender_account.nunique() > 10 and span <= 30 * DAY and rows.Amount.sum() > 100_000
    )


def test_old_rule_missed_bursts_inside_longer_histories():
    frame = scenario()
    assert not legacy_rule(frame, MULE)  # history spans 200 days
    assert not legacy_rule(frame, EXACT)  # "> 10 senders" excluded exactly ten
    flagged = set(reference_windows(frame, RULES).Receiver_account)
    assert flagged == {MULE, EXACT}


def test_windows_merge_into_one_episode_per_burst():
    frame = scenario()
    windows = reference_windows(frame, RULES)
    for name, con in backends(frame):
        result = episodes_from_windows(con, windows, RULES)
        assert list(result.columns) == OUTPUT_COLUMNS, name
        mule = result[result.Receiver_account == MULE]
        assert mule.Episode_ID.nunique() == 1, name
        assert sorted(mule.Sender_account) == list(range(200, 212)), name
        assert mule.Sender_count.iloc[0] == 12, name
        assert mule.Total_amount.iloc[0] == 114_000, name
        assert mule.Duration_Days.iloc[0] == 11, name
        assert set(mule.Laundering_type) == {"Smurfing"}, name
        exact = result[result.Receiver_account == EXACT]
        assert (exact.Sender_count.iloc[0], len(exact)) == (10, 10), name


def test_separate_bursts_become_separate_episodes():
    rows = [
        {"Sender_account": 10 * burst + i, "Receiver_account": MULE,
         "ts_epoch": T0 + (100 * burst + i) * DAY, "Amount": 20_000.0}
        for burst in (1, 2)
        for i in range(10)
    ]
    frame = tx_frame(rows)
    windows = reference_windows(frame, RULES)
    for name, con in backends(frame):
        result = episodes_from_windows(con, windows, RULES)
        assert result.Episode_ID.nunique() == 2, name
        assert result.groupby("Episode_ID").size().tolist() == [10, 10], name


@pytest.mark.skipif(duckdb is None, reason="duckdb not installed")
def test_duckdb_windows_match_brute_force():
    rng = random.Random(3)
    rows = [
        {"Sender_account": rng.randint(1, 15), "Receiver_account": rng.randint(100, 103),
         "ts_epoch": T0 + rng.randint(0, 90) * DAY + rng.choice([0, HOUR]),
         "Amount": float(rng.choice([900, 4_000, 9_500])),
         "Payment_currency": rng.choice(["UK pounds", "UK pounds", "Euro"])}
        for _ in range(400)
    ]
    frame = tx_frame(rows)
    rules = SmurfRules(min_distinct_senders=6, min_total=30_000)
    expected = reference_windows(frame, rules)
    assert not expected.empty
    got = query_df(duckdb_backend(frame), *windows_sql(rules))
    assert _as_set(got) == _as_set(expected)


def _as_set(frame):
    return {
        (int(r[0]), int(r[1]), int(r[2]), int(r[3]), round(float(r[4]), 6)) for r in frame.values
    }
