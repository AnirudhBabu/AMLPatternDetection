import os
import sys
import tempfile
import types
from pathlib import Path

import pytest

import aml.dataset as dataset
from aml.dataset import KAGGLE_HANDLE, RAW_NAME, Layout, fetch_dataset

try:
    import duckdb
except ImportError:
    duckdb = None

CSV = "Time,Date,Sender_account\n10:35:19,2022-10-07,8724731955\n"


def _never(path, mode):
    return False


def _fetch(tmp: Path, *, download_dir: Path | None = None, writable: bool = True,
           local_copy: Path | None = None):
    """fetch_dataset with a private kagglehub cache and a fake kagglehub that records calls."""
    calls = []

    def dataset_download(handle):
        calls.append(handle)
        if download_dir is None:
            raise AssertionError("tried to download")
        return str(download_dir)

    saved = os.environ.get("KAGGLEHUB_CACHE"), sys.modules.get("kagglehub"), dataset.os.access
    os.environ["KAGGLEHUB_CACHE"] = str(tmp / "cache")
    sys.modules["kagglehub"] = types.SimpleNamespace(dataset_download=dataset_download)
    if not writable:
        dataset.os.access = _never
    try:
        return fetch_dataset(Layout(tmp / "data"), local_copy=local_copy), calls
    finally:
        env, module, access = saved
        if env is None:
            os.environ.pop("KAGGLEHUB_CACHE", None)
        else:
            os.environ["KAGGLEHUB_CACHE"] = env
        if module is None:
            sys.modules.pop("kagglehub", None)
        else:
            sys.modules["kagglehub"] = module
        dataset.os.access = access


def test_reuses_the_copy_a_failed_run_left_in_the_cache():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        cached = tmp / "cache" / "datasets" / KAGGLE_HANDLE / "versions" / "2" / RAW_NAME
        cached.parent.mkdir(parents=True)
        cached.write_text(CSV)
        (path, origin), calls = _fetch(tmp)
        assert calls == []  # no second 193 MB download
        assert origin == "kagglehub cache" and path.read_text() == CSV and not cached.exists()


def test_downloads_when_nothing_is_cached():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        download = tmp / "download"
        download.mkdir()
        (download / RAW_NAME).write_text(CSV)
        (path, origin), calls = _fetch(tmp, download_dir=download)
        assert calls == [KAGGLE_HANDLE] and origin == "Kaggle" and path.read_text() == CSV


def test_checks_the_data_folder_before_downloading():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        with pytest.raises(PermissionError, match="sudo chown -R"):
            _fetch(tmp, download_dir=tmp, writable=False)
        assert not (tmp / "data" / RAW_NAME).exists()


def test_does_nothing_when_the_csv_is_already_there():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "data").mkdir()
        (tmp / "data" / RAW_NAME).write_text(CSV)
        (path, origin), calls = _fetch(tmp)
        assert (origin, calls) == ("data folder", [])


def test_a_local_copy_is_copied_not_moved_and_beats_the_cache():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        mine = tmp / "elsewhere" / RAW_NAME
        mine.parent.mkdir()
        mine.write_text(CSV)
        cached = tmp / "cache" / "datasets" / KAGGLE_HANDLE / "versions" / "2" / RAW_NAME
        cached.parent.mkdir(parents=True)
        cached.write_text("other")
        (path, origin), calls = _fetch(tmp, local_copy=mine)
        assert (origin, calls) == ("local copy", []) and path.read_text() == CSV
        assert mine.exists() and cached.exists()  # neither was moved


def test_a_missing_local_copy_is_reported():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        with pytest.raises(FileNotFoundError, match="--from"):
            _fetch(tmp, local_copy=tmp / "nope.csv")


@pytest.mark.skipif(duckdb is None, reason="duckdb not installed")
def test_batch_readers_stream_the_parquet():
    from aml.dataset import account_batches, transfer_batches, transfer_count

    with tempfile.TemporaryDirectory() as tmp:
        parquet = Path(tmp) / "t.parquet"
        con = duckdb.connect()
        con.execute(
            "COPY (SELECT i AS tx_id, 1665100800 + i AS ts_epoch, "
            "epoch_ms((1665100800 + i) * 1000) AS ts, i % 3 AS Sender_account, "
            "10 + i % 2 AS Receiver_account, 100.0 * i AS Amount, 'Normal' AS Laundering_type "
            f"FROM range(1, 6) r(i)) TO '{parquet}' (FORMAT parquet)"
        )
        con.close()
        assert transfer_count(parquet) == 5
        batches = list(transfer_batches(parquet, 2))
        assert [len(b) for b in batches] == [2, 2, 1]
        assert batches[0][0] == [1, 11, 1, 100.0, 1665100801, "2022-10-07T00:00:01", "Normal"]
        assert sorted(i for b in account_batches(parquet, 2) for i in b) == [0, 1, 2, 10, 11]


SAML_D = (
    "Time,Date,Sender_account,Receiver_account,Amount,Payment_currency,Received_currency,"
    "Sender_bank_location,Receiver_bank_location,Payment_type,Is_laundering,Laundering_type\n"
    "10:35:19,2022-10-07,8724731955,2769355426,1459.15,UK pounds,UK pounds,UK,UK,"
    "Cash Deposit,0,Normal_Cash_Deposits\n"
    "10:35:20,2022-10-07,1491989064,8401255335,6019.64,UK pounds,Dirham,UK,UAE,"
    "Cross-border,0,Normal_Fan_Out\n"
    "00:00:05,2022-10-08,5376652437,9600420220,14.12,UK pounds,UK pounds,UK,UK,"
    "Cheque,1,Smurfing\n"
    "10:35:19,2022-10-07,1111111111,2222222222,10.00,UK pounds,UK pounds,UK,UK,"
    "Credit card,0,Normal_Small_Fan_Out\n"
)


@pytest.mark.skipif(duckdb is None, reason="duckdb not installed")
def test_prepare_converts_the_csv_to_typed_parquet():
    from aml.dataset import prepare

    with tempfile.TemporaryDirectory() as tmp:
        layout = Layout(Path(tmp))
        layout.raw_csv.write_text(SAML_D)
        prepare(layout)
        con = duckdb.connect()
        try:
            rows = con.execute(
                "SELECT tx_id, ts_epoch, Sender_account, Amount, Is_laundering "
                f"FROM read_parquet('{layout.parquet}') ORDER BY tx_id"
            ).fetchall()
            types_ = dict(con.execute(
                f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM "
                f"read_parquet('{layout.parquet}'))"
            ).fetchall())
        finally:
            con.close()
    # tx_id follows time, then sender: the out-of-order CSV rows come back sorted
    assert rows == [
        (1, 1665138919, 1111111111, 10.0, 0),
        (2, 1665138919, 8724731955, 1459.15, 0),
        (3, 1665138920, 1491989064, 6019.64, 0),
        (4, 1665187205, 5376652437, 14.12, 1),
    ]
    assert types_["tx_id"] == "BIGINT" and types_["ts_epoch"] == "BIGINT"
    assert types_["ts"] == "TIMESTAMP" and types_["Amount"] == "DOUBLE"


@pytest.mark.skipif(duckdb is None, reason="duckdb not installed")
def test_duckdb_connect_turns_off_the_progress_bar():
    from aml.db import duckdb_connect

    con = duckdb_connect()  # used to fail: enable_progress_bar isn't a global option
    try:
        setting = con.execute("SELECT current_setting('enable_progress_bar')").fetchone()[0]
    finally:
        con.close()
    assert setting is False
