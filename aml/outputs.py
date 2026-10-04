"""Every result file the pipeline writes, and the one loader that reads them back.

Each detector and the evaluation write CSVs into the data folder. This registry is the only
list of those files: what each is called, which command writes it and, for detector outputs,
which columns hold transaction ids. ``load`` reads any of them and treats a missing file the
same way everywhere: it's reported along with the command that creates it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class OutputFile:
    key: str
    file: str
    command: str
    tx_columns: tuple[str, ...] = ()
    always_written: bool = True  # False: only exists when the matching detector has run

    @property
    def table(self) -> str:
        return Path(self.file).stem


DETECTORS = (
    OutputFile("cycles", "detected_cycles.csv", "aml cycles", ("Tx_ID",)),
    OutputFile("smurfing", "smurfing_suspects.csv", "aml smurfing", ("Tx_ID",)),
    OutputFile("scatter_gather", "scatter_gather_suspects.csv", "aml scatter-gather",
               ("In_Tx_ID", "Out_Tx_ID")),
    OutputFile("deposit_send", "deposit_send_suspects.csv", "aml deposit-send",
               ("Deposit_Tx_ID", "Send_Tx_ID")),
)
EVALUATION = (
    OutputFile("summary", "evaluation_summary.csv", "aml evaluate"),
    OutputFile("by_typology", "evaluation_by_typology.csv", "aml evaluate"),
    OutputFile("cycle_sweep", "evaluation_cycle_sweep.csv", "aml evaluate",
               always_written=False),
    OutputFile("scatter_gather_sweep", "evaluation_scatter_gather_sweep.csv", "aml evaluate",
               always_written=False),
    OutputFile("deposit_send_sweep", "evaluation_deposit_send_sweep.csv", "aml evaluate",
               always_written=False),
)
ALL = DETECTORS + EVALUATION
BY_KEY = {o.key: o for o in ALL}
BY_TABLE = {o.table: o for o in ALL}


class StaleOutputError(ValueError):
    """A result file written by an older version, without columns this one needs."""


@dataclass
class Loaded:
    frames: dict[str, pd.DataFrame]
    missing: list[OutputFile]

    def missing_text(self) -> str:
        return describe(self.missing)


def describe(missing: Iterable[OutputFile]) -> str:
    """The missing files someone can actually create, each with the command that does it."""
    return ", ".join(f"{o.file} (run `{o.command}`)" for o in missing if o.always_written)


def path(data_dir: Path, key: str) -> Path:
    return Path(data_dir) / BY_KEY[key].file


def load(data_dir: Path, files: Iterable[OutputFile] = DETECTORS) -> Loaded:
    frames: dict[str, pd.DataFrame] = {}
    missing: list[OutputFile] = []
    for output in files:
        csv = Path(data_dir) / output.file
        if not csv.is_file():
            missing.append(output)
            continue
        frame = pd.read_csv(csv)
        absent = [c for c in output.tx_columns if c not in frame.columns]
        if absent:
            raise StaleOutputError(
                f"{csv} has no {', '.join(absent)} column; an older version wrote it. "
                f"Rerun `{output.command}`."
            )
        frames[output.key] = frame
    return Loaded(frames, missing)


def tx_ids(key: str, frame: pd.DataFrame) -> pd.Series:
    """Every transaction id a detector output refers to."""
    return pd.concat([frame[c] for c in BY_KEY[key].tx_columns], ignore_index=True)
