import math

import pandas as pd
from support import T0, backends, tx_frame

from aml.evaluate import evaluate


def labelled_transactions():
    rows = [
        (1, 2, 1, "Cycle"),
        (2, 3, 1, "Cycle"),
        (3, 1, 1, "Cycle"),
        (4, 5, 0, "Normal_Small_Fan_Out"),
        (6, 5, 1, "Fan_In"),
        (7, 5, 1, "Fan_In"),
        (8, 5, 0, "Normal_Fan_In"),
        (4, 1, 0, "Normal_Periodical"),
    ]
    return tx_frame(
        [
            {"Sender_account": s, "Receiver_account": r, "ts_epoch": T0 + i,
             "Is_laundering": label, "Laundering_type": typology}
            for i, (s, r, label, typology) in enumerate(rows)
        ]
    )


CYCLES = pd.DataFrame(
    {  # a real 3-hop cycle (48 h) and a spurious 2-hop one (500 h)
        "Cycle_ID": [1, 1, 1, 2, 2],
        "Tx_ID": [1, 2, 3, 4, 8],
        "Cycle_Duration_Hours": [48.0, 48.0, 48.0, 500.0, 500.0],
    }
)
SMURFING = pd.DataFrame({"Episode_ID": [1, 1, 1], "Tx_ID": [5, 6, 7]})
SCATTER_GATHER = pd.DataFrame(  # one episode, two legs: tx 4 -> 6 and 5 -> 7
    {"Episode_ID": [1, 1], "In_Tx_ID": [4, 5], "Out_Tx_ID": [6, 7], "Mule_count": [3, 3]}
)


def close(a, b):
    return (math.isnan(a) and math.isnan(b)) or math.isclose(a, b)


def test_precision_lift_and_recall():
    for name, con in backends(labelled_transactions()):
        report = evaluate(con, cycles=CYCLES, smurfing=SMURFING, sweep_days=(1, 3, 30))
        summary = report.summary.set_index("detector")

        # cycling flags tx {1,2,3,4,8}: 3 of 5 laundering; base rate 5/8
        assert summary.at["cycling", "flagged_tx"] == 5, name
        assert close(summary.at["cycling", "precision_tx"], 3 / 5), name
        assert close(summary.at["cycling", "lift_tx"], (3 / 5) / (5 / 8)), name
        # accounts {1,2,3,4,5}: 1,2,3,5 touch laundering; 6 of 8 accounts do overall
        assert summary.at["cycling", "flagged_accounts"] == 5, name
        assert close(summary.at["cycling", "precision_accounts"], 4 / 5), name
        assert close(summary.at["cycling", "lift_accounts"], (4 / 5) / (6 / 8)), name

        assert close(summary.at["smurfing", "precision_tx"], 2 / 3), name
        assert close(summary.at["smurfing", "precision_accounts"], 3 / 4), name

        typology = report.by_typology.set_index("typology")
        assert set(typology.index) == {"Cycle", "Fan_In"}, name
        assert close(typology.at["Cycle", "recall_tx_cycling"], 1.0), name
        assert close(typology.at["Fan_In", "recall_tx_cycling"], 0.0), name
        assert close(typology.at["Fan_In", "recall_accounts_cycling"], 1 / 3), name
        assert close(typology.at["Fan_In", "recall_tx_smurfing"], 1.0), name
        assert close(typology.at["Cycle", "recall_accounts_smurfing"], 0.0), name

        sweep = report.cycle_sweep.set_index("max_duration_days")
        assert list(sweep.index) == [1, 3, 30, "any"], name
        assert sweep.at[1, "flagged_tx"] == 0, name
        assert math.isnan(sweep.at[1, "precision_tx"]), name
        assert close(sweep.at[3, "precision_tx"], 1.0), name
        assert close(sweep.at[3, "recall_tx_Cycle"], 1.0), name
        assert close(sweep.at["any", "precision_tx"], 3 / 5), name
        assert "base rates" in report.render(), name


def test_unknown_cycle_label_is_reported():
    for name, con in backends(labelled_transactions()):
        report = evaluate(con, cycles=CYCLES, cycle_label="Round_Trip")
        assert any("Round_Trip" in note for note in report.notes), name
        assert report.cycle_sweep["recall_tx_Round_Trip"].isna().all(), name


def test_scatter_gather_scoring_and_mule_sweep():
    for name, con in backends(labelled_transactions()):
        report = evaluate(
            con, scatter_gather=SCATTER_GATHER, scatter_gather_label="Fan_In", sweep_mules=(3, 4)
        )
        summary = report.summary.set_index("detector")
        assert summary.at["scatter-gather", "flagged_tx"] == 4, name  # in and out legs
        assert close(summary.at["scatter-gather", "precision_tx"], 2 / 4), name

        sweep = report.scatter_gather_sweep.set_index("min_mules")
        assert list(sweep.index) == [3, 4, "all"], name
        assert close(sweep.at[3, "recall_tx_Fan_In"], 1.0), name
        assert sweep.at[4, "flagged_tx"] == 0, name

        typology = report.by_typology.set_index("typology")
        assert close(typology.at["Fan_In", "recall_tx_scatter-gather"], 1.0), name
        assert "Scatter-gather" in report.render(), name



DEPOSIT_SEND = pd.DataFrame(  # cash in as tx 5, sent on as tx 6, three hours later
    {"Alert_ID": [1], "Deposit_Tx_ID": [5], "Send_Tx_ID": [6], "Hours_Held": [3.0]}
)


def test_deposit_send_scoring_and_hold_sweep():
    for name, con in backends(labelled_transactions()):
        report = evaluate(
            con, deposit_send=DEPOSIT_SEND, deposit_send_label="Fan_In", sweep_hours=(2, 6)
        )
        summary = report.summary.set_index("detector")
        assert summary.at["deposit-send", "flagged_tx"] == 2, name
        assert close(summary.at["deposit-send", "precision_tx"], 1.0), name

        sweep = report.deposit_send_sweep.set_index("max_hours_held")
        assert list(sweep.index) == [2, 6, "all"], name
        assert sweep.at[2, "flagged_tx"] == 0, name
        assert close(sweep.at[6, "recall_tx_Fan_In"], 1.0), name
        assert "Deposit-send" in report.render(), name
