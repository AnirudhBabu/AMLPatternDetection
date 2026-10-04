from aml.cycles import EXTRA_COLUMNS, LEGACY_COLUMNS, CycleRules, cycles_frame, detect_cycles
from aml.graph import Candidate
from aml.temporal import Transfer

DAY = 86_400
T0 = 1_665_100_800  # 2022-10-07 00:00:00 UTC, the first day in SAML-D


def candidate(structure_id, accounts, edges):
    transfers = tuple(Transfer(s, d, ts, amount, tx) for tx, (s, d, ts, amount) in edges.items())
    return Candidate(structure_id, tuple(accounts), transfers)


RING = {  # 10 -> 20 -> 30 -> 10, twice: a slow round (40 days) and a quick one (3 days)
    1: (10, 20, T0, 9000.0),
    2: (20, 30, T0 + 20 * DAY, 8600.0),
    3: (30, 10, T0 + 40 * DAY, 8100.0),
    4: (10, 20, T0 + 60 * DAY, 5000.0),
    5: (20, 30, T0 + 61 * DAY, 4700.0),
    6: (30, 10, T0 + 63 * DAY, 4500.0),
}
NOT_A_CYCLE = {7: (40, 50, 0, 1.0), 8: (60, 50, 1, 1.0), 9: (60, 40, 2, 1.0)}


def test_detects_ring_once_even_when_memgraph_repeats_it():
    candidates = [
        candidate(1, [30, 10, 20], RING),
        candidate(2, [10, 30, 20], RING),  # same accounts, different order
        candidate(3, [40, 50, 60], NOT_A_CYCLE),
    ]
    results, stats = detect_cycles(candidates, CycleRules())
    assert [r.ring for r in results] == [(10, 20, 30)]
    assert (stats.candidates, stats.duplicate_sets, stats.directed_rings) == (3, 1, 1)
    assert results[0].tightest.tx_ids == (4, 5, 6)


def test_output_keeps_legacy_columns_first():
    results, _ = detect_cycles([candidate(1, [10, 20, 30], RING)], CycleRules())
    frame = cycles_frame(results, CycleRules())
    assert list(frame.columns) == LEGACY_COLUMNS + EXTRA_COLUMNS
    assert frame["Hop_Number"].tolist() == [1, 2, 3]
    assert frame["Tx_ID"].tolist() == [4, 5, 6]
    assert frame["Cycle_Duration_Hours"].iloc[0] == 72.0
    assert frame["Amount_Retention"].iloc[0] == 0.9
    assert frame["Timestamp"].iloc[0] == "2022-12-06 00:00:00"


def test_all_instances_and_window_options():
    candidates = [candidate(1, [10, 20, 30], RING)]
    everything, _ = detect_cycles(candidates, CycleRules())
    frame = cycles_frame(everything, CycleRules(all_instances=True))
    assert frame["Cycle_ID"].nunique() == len(everything[0].instances) > 1
    assert set(frame["Valid_Instances"]) == {len(everything[0].instances)}

    windowed, stats = detect_cycles(candidates, CycleRules(max_window_seconds=7 * DAY))
    assert [inst.tx_ids for inst in windowed[0].instances] == [(4, 5, 6)]
    assert stats.temporal_rings == 1


def test_length_bounds():
    candidates = [candidate(1, [10, 20, 30], RING)]
    assert detect_cycles(candidates, CycleRules(min_len=4))[0] == []
    assert detect_cycles(candidates, CycleRules(max_len=2))[0] == []
