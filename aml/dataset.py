"""Getting SAML-D, preparing it, and reading it back in batches."""

from __future__ import annotations

import os
import shutil
import stat
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

from aml.db import duckdb_connect, sql_literal

KAGGLE_HANDLE = "berkanoztas/synthetic-transaction-monitoring-dataset-aml"
RAW_NAME = "SAML-D.csv"


@dataclass(frozen=True)
class Layout:
    """Where the pipeline's files live under the data directory."""

    data_dir: Path = Path("data")

    @property
    def raw_csv(self) -> Path:
        return self.data_dir / RAW_NAME

    @property
    def prepared_dir(self) -> Path:
        return self.data_dir / "prepared"

    @property
    def parquet(self) -> Path:
        return self.prepared_dir / "transactions.parquet"

    @property
    def results_db(self) -> Path:
        return self.data_dir / "aml_results.sqlite"


def kagglehub_cache() -> Path:
    """Where kagglehub keeps SAML-D (it honours the KAGGLEHUB_CACHE variable)."""
    default = os.path.join(os.path.expanduser("~"), ".cache", "kagglehub")
    root = os.environ.get("KAGGLEHUB_CACHE") or default
    return Path(root) / "datasets" / KAGGLE_HANDLE


def require_writable(directory: Path) -> None:
    """Fail early, and say how to fix it, when ``directory`` can't be written to."""
    if os.access(directory, os.W_OK | os.X_OK):
        return
    info = directory.stat()
    why = (
        " Older versions of docker-compose.yml handed this folder to Memgraph's user (uid 101)."
        if info.st_uid == 101
        else ""
    )
    raise PermissionError(
        f"can't write to {directory}/ (owner uid {info.st_uid}, mode "
        f"{stat.filemode(info.st_mode)}).{why} "
        f"Take it back with: sudo chown -R $(id -u):$(id -g) {directory}"
    )


# --- SAML-D sources, tried in order ---------------------------------------------------
# Each sub-loader returns the path of a SAML-D.csv it can supply, or None. The first one
# that answers wins; ``keep`` says whether its file must stay where it is (copy) or can be
# moved into the data folder.


@dataclass(frozen=True)
class Source:
    name: str
    find: Callable[[], Path | None]
    keep: bool


def _local_copy(path: Path) -> Path:
    if not path.is_file():
        raise FileNotFoundError(f"--from {path}: no such file")
    return path


def _kagglehub_cache() -> Path | None:
    found = sorted(kagglehub_cache().glob(f"**/{RAW_NAME}"), key=lambda p: p.stat().st_mtime)
    return found[-1] if found else None


def _kaggle_download() -> Path:
    import kagglehub

    cache = kagglehub_cache()
    if cache.exists():  # a run moved the CSV out, so kagglehub would think it's still there
        shutil.rmtree(cache)
    return Path(kagglehub.dataset_download(KAGGLE_HANDLE)) / RAW_NAME


def sources(local_copy: Path | None = None) -> list[Source]:
    chain = [Source("kagglehub cache", _kagglehub_cache, keep=False),
             Source("Kaggle", _kaggle_download, keep=False)]
    if local_copy is not None:
        chain.insert(0, Source("local copy", lambda: _local_copy(local_copy), keep=True))
    return chain


def fetch_dataset(layout: Layout, *, local_copy: Path | None = None) -> tuple[Path, str]:
    """Make sure data/SAML-D.csv exists. Returns (path, name of the source that supplied it)."""
    if layout.raw_csv.is_file():
        return layout.raw_csv, "data folder"
    layout.data_dir.mkdir(parents=True, exist_ok=True)
    require_writable(layout.data_dir)  # before any download, not after
    for source in sources(local_copy):
        found = source.find()
        if found is None:
            continue
        if source.keep:
            shutil.copyfile(found, layout.raw_csv)
        else:
            # copyfile, not copy2: copying permission bits can fail on Docker-managed folders
            shutil.move(str(found), layout.raw_csv, copy_function=shutil.copyfile)
        return layout.raw_csv, source.name
    raise FileNotFoundError(f"no source could supply {RAW_NAME}")


# --- preparation and batch readers ----------------------------------------------------


def prepare(layout: Layout) -> None:
    """SAML-D.csv -> typed Parquet with a stable ``tx_id``, ``ts`` and ``ts_epoch``.

    ``tx_id`` gives every transaction an identity, so detections can be joined back to the
    labels; Parquet makes every later query a fraction of the cost of re-parsing the CSV.
    """
    if not layout.raw_csv.is_file():
        raise FileNotFoundError(f"{layout.raw_csv} not found - run `aml fetch-data` first")
    try:
        layout.prepared_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        require_writable(layout.data_dir)
        raise
    require_writable(layout.prepared_dir)
    con = duckdb_connect()
    try:
        con.execute(
            f"""
            COPY (
                WITH raw AS (
                    SELECT *, CAST("Date" AS DATE) + CAST("Time" AS TIME) AS ts
                    FROM read_csv({sql_literal(layout.raw_csv)}, header = true)
                )
                SELECT row_number() OVER (
                           ORDER BY ts, Sender_account, Receiver_account, Amount
                       ) AS tx_id,
                       CAST(epoch(ts) AS BIGINT) AS ts_epoch,
                       *
                FROM raw
            ) TO {sql_literal(layout.parquet)} (FORMAT parquet)
            """
        )
    finally:
        con.close()


def _batches(parquet: Path, sql: str, size: int) -> Iterator[list]:
    con = duckdb_connect()
    try:
        cursor = con.execute(sql.replace("__SOURCE__", f"read_parquet({sql_literal(parquet)})"))
        while batch := cursor.fetchmany(size):
            yield batch
    finally:
        con.close()


def account_batches(parquet: Path, size: int) -> Iterator[list[int]]:
    """Every account id, in lists of up to ``size``."""
    sql = "SELECT Sender_account FROM __SOURCE__ UNION SELECT Receiver_account FROM __SOURCE__"
    for batch in _batches(parquet, sql, size):
        yield [row[0] for row in batch]


def transfer_batches(parquet: Path, size: int) -> Iterator[list[list]]:
    """Rows of [sender, receiver, tx_id, amount, ts, ISO datetime, laundering type]."""
    sql = (
        "SELECT Sender_account, Receiver_account, tx_id, Amount, ts_epoch, "
        "strftime(ts, '%Y-%m-%dT%H:%M:%S'), Laundering_type FROM __SOURCE__"
    )
    for batch in _batches(parquet, sql, size):
        yield [list(row) for row in batch]


def transfer_count(parquet: Path) -> int:
    con = duckdb_connect()
    try:
        sql = f"SELECT COUNT(*) FROM read_parquet({sql_literal(parquet)})"
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()
