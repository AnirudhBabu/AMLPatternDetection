import random

import pytest
from support import HOUR, T0, backends, tx_frame

from aml.db import query_df
from aml.deposit_send import (
    OUTPUT_COLUMNS,
    DepositSendRules,
    detect_deposit_send,
    pairs_sql,
    resolve_payment_types,
)

RULES = DepositSendRules()  # within 72 hours, sending on 80-105% of the deposit
DEPOSIT, WITHDRAW = "Cash Deposit", "Cash Withdrawal"
MULE, SLOW, KEEPER, CASHER, EARLY, SPLITTER = 7, 8, 9, 10, 11, 12


def cash_in(account, at, amount=9_000.0):
    return {"Sender_account": account * 10, "Receiver_account": account, "ts_epoch": at,
            "Amount": amount, "Payment_type": DEPOSIT}


def pay(account, to, at, amount, payment_type="Cross-border", location="UK"):
    return {"Sender_account": account, "Receiver_account": to, "ts_epoch": at, "Amount": amount,
            "Payment_type": payment_type, "Receiver_bank_location": location}


def scenario():
    return tx_frame(
        [
            cash_in(MULE, T0), pay(MULE, 900, T0 + 5 * HOUR, 8_800.0, location="Mexico"),
            cash_in(SLOW, T0), pay(SLOW, 901, T0 + 96 * HOUR, 8_800.0),  # four days later
            cash_in(KEEPER, T0), pay(KEEPER, 902, T0 + HOUR, 1_000.0),  # keeps most of it
            cash_in(CASHER, T0), pay(CASHER, 903, T0 + HOUR, 9_000.0, WITHDRAW),  # cash out
            pay(EARLY, 904, T0 - HOUR, 9_000.0), cash_in(EARLY, T0),  # paid before the deposit
            cash_in(SPLITTER, T0, 5_000.0),
            pay(SPLITTER, 905, T0 + 2 * HOUR, 4_500.0, "Debit card"),
            pay(SPLITTER, 906, T0 + 3 * HOUR, 4_600.0, "Debit card"),
        ]
    )


def test_pairs_each_deposit_with_the_first_payment_that_moves_it_on():
    for name, con in backends(scenario()):
        result = detect_deposit_send(con, RULES)
        assert list(result.columns) == OUTPUT_COLUMNS, name
        assert sorted(result.Account) == [MULE, SPLITTER], name
        mule = result[result.Account == MULE].iloc[0]
        assert (mule.Hours_Held, mule.Forward_Ratio) == (5.0, 0.9778), name
        assert (mule.Send_To, mule.Send_Location, mule.Deposit_Type) == (900, "Mexico", DEPOSIT)
        splitter = result[result.Account == SPLITTER].iloc[0]
        assert (splitter.Send_To, splitter.Hours_Held) == (905, 2.0), name  # the earlier one


def test_cross_border_only_and_type_detection():
    for name, con in backends(scenario()):
        assert resolve_payment_types(con, RULES) == ((DEPOSIT,), (DEPOSIT, WITHDRAW)), name
        abroad = detect_deposit_send(con, DepositSendRules(cross_border_only=True))
        assert list(abroad.Account) == [MULE], name


def test_explains_when_there_is_no_cash_deposit_type():
    frame = tx_frame([pay(1, 2, T0, 100.0)])
    for _, con in backends(frame):
        with pytest.raises(ValueError, match="Payment types present: Cross-border"):
            detect_deposit_send(con, RULES)


def _brute_force(frame, rules):
    rows = list(frame.itertuples())
    cash = {DEPOSIT, WITHDRAW}
    pairs = set()
    for d in rows:
        if d.Payment_type != DEPOSIT or d.Amount < rules.min_deposit:
            continue
        sends = [
            s for s in rows
            if s.Sender_account == d.Receiver_account
            and s.Sender_account != s.Receiver_account
            and s.Payment_type not in cash
            and d.ts_epoch <= s.ts_epoch <= d.ts_epoch + rules.max_hold_seconds
            and d.Amount * rules.min_send_ratio <= s.Amount <= d.Amount * rules.max_send_ratio
        ]
        if sends:
            first = min(sends, key=lambda s: (s.ts_epoch, s.tx_id))
            pairs.add((d.tx_id, first.tx_id))
    return pairs


def test_matches_brute_force_across_time_bucket_edges():
    # 24-hour buckets over 20 days of random times: many deposit/payment pairs straddle a
    # bucket edge, which the same-bucket-or-next-bucket join must still find.
    rng = random.Random(5)
    accounts = range(1, 9)
    frame = tx_frame(
        [
            {"Sender_account": rng.choice(accounts), "Receiver_account": rng.choice(accounts),
             "ts_epoch": T0 + rng.randint(0, 20 * 24 * HOUR),
             "Amount": float(rng.choice([800, 900, 1000, 2000, 5000])),
             "Payment_type": rng.choice([DEPOSIT, DEPOSIT, WITHDRAW, "Cross-border", "Debit card"])}
            for _ in range(400)
        ]
    )
    rules = DepositSendRules(max_hold_seconds=24 * HOUR)
    expected = _brute_force(frame, rules)
    assert len(expected) > 20
    for name, con in backends(frame):
        types = resolve_payment_types(con, rules)
        got = query_df(con, *pairs_sql(rules, *types))
        found = {(int(d), int(s)) for d, s in got[["deposit_tx", "send_tx"]].values}
        assert found == expected, name
        assert len(found) == len({d for d, _ in found}), name  # one payment per deposit


def test_ratio_bounds_are_inclusive():
    frame = tx_frame([cash_in(1, T0, 1_000.0), pay(1, 2, T0 + HOUR, 800.0)])
    for name, con in backends(frame):
        assert len(detect_deposit_send(con, RULES)) == 1, name
        stricter = DepositSendRules(min_send_ratio=0.81)
        assert detect_deposit_send(con, stricter).empty, name
