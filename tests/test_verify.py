import itertools

from support import DAY, T0, backends, tx_frame

from aml.verify import verify_cycle

A, B, C = 2521152088, 718745407, 8657935466

ROWS = [
    {"Sender_account": A, "Receiver_account": B, "ts_epoch": T0 + 1 * DAY},   # tx 1
    {"Sender_account": A, "Receiver_account": B, "ts_epoch": T0 + 10 * DAY},  # tx 2
    {"Sender_account": B, "Receiver_account": C, "ts_epoch": T0 + 5 * DAY},   # tx 3
    {"Sender_account": C, "Receiver_account": A, "ts_epoch": T0 + 7 * DAY},   # tx 4
    {"Sender_account": C, "Receiver_account": A, "ts_epoch": T0 + 12 * DAY},  # tx 5
    {"Sender_account": C, "Receiver_account": B, "ts_epoch": T0 + 2 * DAY},   # tx 6 (noise)
]


def brute_force(frame, path, max_window=None):
    hops = []
    for i in range(len(path)):
        src, dst = path[i], path[(i + 1) % len(path)]
        rows = frame[(frame.Sender_account == src) & (frame.Receiver_account == dst)]
        hops.append(list(rows.itertuples()))
    chains = set()
    for combo in itertools.product(*hops):
        times = [row.ts_epoch for row in combo]
        if all(b >= a for a, b in zip(times, times[1:])):
            if max_window is None or times[-1] - times[0] <= max_window:
                chains.add(tuple(row.tx_id for row in combo))
    return chains


def as_chains(result, n):
    columns = [f"tx_{i}" for i in range(1, n + 1)]
    return {tuple(int(v) for v in row) for row in result[columns].values}


def test_only_real_chronological_chains_are_returned():
    frame = tx_frame(ROWS)
    # The old script cross-joined all hops: 2 x 1 x 2 = 4 rows, including the impossible
    # A->B on day 10 followed by B->C on day 5.
    assert brute_force(frame, [A, B, C]) == {(1, 3, 4), (1, 3, 5)}
    for name, con in backends(frame):
        result = verify_cycle(con, [A, B, C])
        assert as_chains(result, 3) == {(1, 3, 4), (1, 3, 5)}, name


def test_window_and_missing_hop():
    frame = tx_frame(ROWS)
    for name, con in backends(frame):
        windowed = verify_cycle(con, [A, B, C], max_window_seconds=6 * DAY)
        assert as_chains(windowed, 3) == {(1, 3, 4)}, name
        assert verify_cycle(con, [B, A, C]).empty, name  # there is no B -> A transfer
