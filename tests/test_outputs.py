import tempfile
from pathlib import Path

import pandas as pd
import pytest

from aml import outputs


def test_load_reports_each_missing_file_with_the_command_that_creates_it():
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        pd.DataFrame({"Tx_ID": [1, 2]}).to_csv(data / "detected_cycles.csv", index=False)
        loaded = outputs.load(data)
        assert list(loaded.frames) == ["cycles"]
        assert [o.key for o in loaded.missing] == ["smurfing", "scatter_gather", "deposit_send"]
        assert loaded.missing_text() == (
            "smurfing_suspects.csv (run `aml smurfing`), "
            "scatter_gather_suspects.csv (run `aml scatter-gather`), "
            "deposit_send_suspects.csv (run `aml deposit-send`)"
        )


def test_sweeps_only_written_for_detectors_that_ran_are_not_reported_missing():
    with tempfile.TemporaryDirectory() as tmp:
        loaded = outputs.load(Path(tmp), outputs.EVALUATION)
        assert loaded.missing_text() == (
            "evaluation_summary.csv (run `aml evaluate`), "
            "evaluation_by_typology.csv (run `aml evaluate`)"
        )


def test_a_file_from_an_older_version_is_rejected_with_the_fix():
    with tempfile.TemporaryDirectory() as tmp:
        data = Path(tmp)
        pd.DataFrame({"Episode_ID": [1]}).to_csv(data / "scatter_gather_suspects.csv", index=False)
        stale = "In_Tx_ID, Out_Tx_ID.*aml scatter-gather"
        with pytest.raises(outputs.StaleOutputError, match=stale):
            outputs.load(data)


def test_tx_ids_cover_every_transaction_column():
    frame = pd.DataFrame({"Deposit_Tx_ID": [5, 7], "Send_Tx_ID": [6, 8]})
    assert outputs.tx_ids("deposit_send", frame).tolist() == [5, 7, 6, 8]


def test_one_name_per_file_and_table():
    assert len({o.file for o in outputs.ALL}) == len(outputs.ALL)
    assert outputs.path(Path("data"), "summary") == Path("data/evaluation_summary.csv")
    assert outputs.BY_TABLE["detected_cycles"].command == "aml cycles"
