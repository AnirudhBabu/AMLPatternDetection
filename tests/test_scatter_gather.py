import itertools
import random
from collections import defaultdict

from support import DAY, T0, backends, tx_frame

from aml.db import query_df
from aml.scatter_gather import (
    OUTPUT_COLUMNS,
    Leg,
    ScatterGatherRules,
    detect_scatter_gather,
    find_episodes,
    legs_sql,
)

RULES = ScatterGatherRules()  # 7-day hops, 30-day episodes, >= 3 mules, passes on 50-105%
SOURCE, DEST = 1, 99
_ids = itertools.count(1000)


def scenario():
    laundering = {"Is_laundering": 1, "Laundering_type": "Scatter-Gather"}
    return tx_frame(
        [
            # SOURCE pays 10k to mules 11, 12, 13; each passes 9.5k to DEST two days later
            *[{"Sender_account": SOURCE, "Receiver_account": mule, "ts_epoch": T0 + i * DAY,
               "Amount": 10_000.0, **laundering} for i, mule in enumerate((11, 12, 13))],
            *[{"Sender_account": mule, "Receiver_account": DEST, "ts_epoch": T0 + (i + 2) * DAY,
               "Amount": 9_500.0, **laundering} for i, mule in enumerate((11, 12, 13))],
            # decoys: 14 passes it on too late, 15 passes on too little, 16 paid DEST first
            {"Sender_account": SOURCE, "Receiver_account": 14, "ts_epoch": T0, "Amount": 10_000.0},
            {"Sender_account": 14, "Receiver_account": DEST, "ts_epoch": T0 + 10 * DAY,
             "Amount": 9_000.0},
            {"Sender_account": SOURCE, "Receiver_account": 15, "ts_epoch": T0, "Amount": 10_000.0},
            {"Sender_account": 15, "Receiver_account": DEST, "ts_epoch": T0 + DAY,
             "Amount": 3_000.0},
            {"Sender_account": 16, "Receiver_account": DEST, "ts_epoch": T0, "Amount": 9_000.0},
            {"Sender_account": SOURCE, "Receiver_account": 16, "ts_epoch": T0 + DAY,
             "Amount": 10_000.0},
            # another pair with only two mules: not enough
            *[{"Sender_account": 2, "Receiver_account": mule, "ts_epoch": T0, "Amount": 5_000.0}
              for mule in (21, 22)],
            *[{"Sender_account": mule, "Receiver_account": 98, "ts_epoch": T0 + DAY,
               "Amount": 5_000.0} for mule in (21, 22)],
        ]
    )


def test_finds_the_episode_and_skips_the_decoys():
    for name, con in backends(scenario()):
        result = detect_scatter_gather(con, RULES)
        assert list(result.columns) == OUTPUT_COLUMNS, name
        assert result.Episode_ID.nunique() == 1, name
        assert (set(result.Source_account), set(result.Destination_account)) == ({SOURCE}, {DEST})
        assert sorted(result.Mule_account) == [11, 12, 13], name
        assert sorted(result.In_Tx_ID) == [1, 2, 3] and sorted(result.Out_Tx_ID) == [4, 5, 6]
        first = result.iloc[0]
        totals = (first.Mule_count, first.Total_scattered, first.Total_gathered)
        assert totals == (3, 30_000, 28_500), name
        assert (first.Gather_Ratio, first.Forward_Ratio, first.Hold_Hours) == (0.95, 0.95, 48.0)
        assert (first.Episode_Start, first.Episode_End) == (
            "2022-10-07 00:00:00", "2022-10-11 00:00:00"), name
        assert first.Episode_Duration_Days == 4.0, name


def _brute_force_legs(frame, rules):
    rows = [r for r in frame.itertuples() if r.Sender_account != r.Receiver_account]
    legs = set()
    for a, b in itertools.product(rows, rows):
        if (
            b.Sender_account == a.Receiver_account
            and b.Receiver_account != a.Sender_account
            and a.ts_epoch <= b.ts_epoch <= a.ts_epoch + rules.hop_seconds
            and a.Amount * rules.min_forward_ratio <= b.Amount <= a.Amount * rules.max_forward_ratio
        ):
            legs.add((a.Sender_account, a.Receiver_account, b.Receiver_account, a.tx_id, b.tx_id))
    mules = defaultdict(set)
    for source, mule, dest, _, _ in legs:
        mules[(source, dest)].add(mule)
    return {leg for leg in legs if len(mules[(leg[0], leg[2])]) >= rules.min_mules}


def test_bucketed_join_matches_brute_force():
    # 3-day hop buckets over 40 days of random timestamps: plenty of pairs straddle a bucket
    # edge, which the "same bucket OR next bucket" join must still catch.
    rng = random.Random(11)
    frame = tx_frame(
        [
            {"Sender_account": rng.randint(1, 12), "Receiver_account": rng.randint(1, 12),
             "ts_epoch": T0 + rng.randint(0, 40 * DAY),
             "Amount": float(rng.choice([400, 600, 900, 1000, 1050]))}
            for _ in range(500)
        ]
    )
    rules = ScatterGatherRules(hop_seconds=3 * DAY, min_mules=2)
    expected = _brute_force_legs(frame, rules)
    assert len(expected) > 50
    for name, con in backends(frame):
        got = query_df(con, *legs_sql(rules))
        legs = {tuple(int(v) for v in row[:5]) for row in got.itertuples(index=False, name=None)}
        assert legs == expected, name


def _leg(mule, t_in, t_out):
    return Leg(SOURCE, mule, DEST, next(_ids), next(_ids), t_in, t_out, 100.0, 95.0)


def test_mules_must_fall_inside_one_window():
    spread_out = [_leg(11, 0, DAY), _leg(12, 40 * DAY, 41 * DAY), _leg(13, 80 * DAY, 81 * DAY)]
    assert find_episodes(spread_out, RULES) == []

    two_bursts = [
        _leg(mule, (100 * burst + i) * DAY, (100 * burst + i + 1) * DAY)
        for burst in (0, 1)
        for i, mule in enumerate((11, 12, 13))
    ]
    assert [len(ep) for ep in find_episodes(two_bursts, RULES)] == [3, 3]

    # a fourth mule 20 days in extends the first episode instead of starting a new one
    extended = two_bursts + [_leg(14, 20 * DAY, 21 * DAY)]
    assert [len(ep) for ep in find_episodes(extended, RULES)] == [4, 3]


def test_a_mule_used_twice_counts_once():
    legs = [_leg(11, 0, DAY), _leg(11, 2 * DAY, 3 * DAY), _leg(12, DAY, 2 * DAY)]
    assert find_episodes(legs, RULES) == []
    assert len(find_episodes(legs, ScatterGatherRules(min_mules=2))) == 1
