"""Command-line interface: ``aml <command>`` (or ``python -m aml <command>``)."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from aml import outputs
from aml.dataset import Layout, require_writable
from aml.graph import DEFAULT_URI

DAY = 86_400


class CliError(Exception):
    """A problem the user can fix (missing input, wrong order of steps)."""


_active_steps = 0  # Steps currently running; only the outermost may draw a spinner


def _mark_reported(exc: BaseException) -> None:
    try:
        exc._aml_reported = True  # main() then doesn't print the same error a second time
    except AttributeError:
        pass


class Step:
    """One line of progress per pipeline step.

    On a terminal it's a halo spinner; anywhere else (pipes, CI logs) plain lines. Only one
    spinner runs at a time: a Step started inside another prints plain lines instead of
    drawing a second spinner over the first. A failure is reported here, once.
    """

    def __init__(self, text: str, color: str = "cyan", *, spinner: bool = True) -> None:
        self.text = text
        self.color = color
        self.result: str | None = None
        self._want_spinner = spinner  # off for steps that print their own progress (downloads)
        self._spinner = None
        self._last_update: float | None = None

    def __enter__(self) -> Step:
        global _active_steps
        _active_steps += 1
        if self._want_spinner and _active_steps == 1 and sys.stdout.isatty():
            try:
                from halo import Halo
            except ImportError:
                pass
            else:
                self._spinner = Halo(text=self.text, color=self.color, spinner="dots")
                self._spinner.start()
        if self._spinner is None:
            print(f"... {self.text}", flush=True)
        return self

    def update(self, text: str) -> None:
        """Show progress: in the spinner, or as a plain line at most every 10 seconds."""
        if self._spinner is not None:
            self._spinner.text = text
            return
        now = time.monotonic()  # the first update always prints, even just after boot
        if self._last_update is None or now - self._last_update >= 10:
            self._last_update = now
            print(f"... {text}", flush=True)

    def __exit__(self, exc_type, exc, tb) -> bool:
        global _active_steps
        _active_steps -= 1
        if exc is None:
            message = self.result or self.text
            if self._spinner is not None:
                self._spinner.succeed(message)
            else:
                print(f"ok  {message}", flush=True)
        elif isinstance(exc, KeyboardInterrupt):
            if self._spinner is not None:
                self._spinner.stop()
        else:
            failure = f"{self.text}: {exc}"
            if self._spinner is not None:
                self._spinner.fail(failure)
            else:
                print(f"FAIL {failure}", file=sys.stderr, flush=True)
            _mark_reported(exc)
        return False


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CliError(message)


def _layout(args: argparse.Namespace) -> Layout:
    return Layout(Path(args.data_dir))


def _ready(layout: Layout, *, writes_output: bool = True) -> None:
    """Check inputs exist and outputs can be written before starting a long step."""
    _require(layout.parquet.is_file(), "No prepared data - run `aml prepare` first.")
    if writes_output:
        require_writable(layout.data_dir)


def _save(frame: pd.DataFrame, layout: Layout, key: str) -> Path:
    """Write a detector's output to its file in aml.outputs."""
    path = outputs.path(layout.data_dir, key)
    frame.to_csv(path, index=False)
    return path


def _days_to_seconds(days: float | None) -> int | None:
    return None if days is None else int(days * DAY)


# --- commands -----------------------------------------------------------------------


def cmd_fetch_data(args: argparse.Namespace) -> None:
    from aml.dataset import fetch_dataset

    layout = _layout(args)
    with Step(f"Making sure {layout.raw_csv} is present", spinner=False) as step:
        path, source = fetch_dataset(layout, local_copy=args.local_copy)
        step.result = {
            "data folder": f"Found {path}",
            "local copy": f"Copied {args.local_copy} to {path}",
            "kagglehub cache": f"Moved the copy in kagglehub's cache to {path}",
            "Kaggle": f"Downloaded {path} from Kaggle",
        }[source]


def cmd_prepare(args: argparse.Namespace) -> None:
    from aml.dataset import prepare

    layout = _layout(args)
    if layout.parquet.is_file() and not args.force:
        print(f"ok  {layout.parquet} is already prepared (use --force to rebuild)")
        return
    with Step("Converting SAML-D.csv to typed Parquet", "magenta") as step:
        prepare(layout)
        step.result = f"Prepared {layout.parquet}"


def cmd_load_graph(args: argparse.Namespace) -> None:
    from aml.dataset import account_batches, transfer_batches, transfer_count
    from aml.graph import connect, load_graph

    layout = _layout(args)
    _ready(layout, writes_output=False)
    total = transfer_count(layout.parquet)
    with Step("Loading accounts and transfers into Memgraph") as step:

        def progress(loaded: int) -> None:
            step.update(f"Loading transfers into Memgraph: {loaded:,} of {total:,}")

        driver = connect(args.memgraph_uri)
        try:
            result = load_graph(
                driver,
                account_batches(layout.parquet, args.batch_size),
                transfer_batches(layout.parquet, args.batch_size),
                workers=args.workers,
                reload=args.reload,
                progress=progress,
            )
        finally:
            driver.close()
        counts = f"{result.nodes:,} accounts and {result.edges:,} transfers"
        step.result = (
            f"Graph already loaded: {counts} (use --reload to rebuild)"
            if result.skipped
            else f"Loaded {counts} into Memgraph"
        )


def cmd_cycles(args: argparse.Namespace) -> None:
    from aml.cycles import CycleRules, cycles_frame, detect_cycles
    from aml.graph import connect, fetch_cycle_candidates

    layout = _layout(args)
    rules = CycleRules(
        min_len=args.min_len,
        max_len=args.max_len,
        max_window_seconds=_days_to_seconds(args.max_window_days),
        strict=args.strict,
        all_instances=args.all_instances,
    )
    layout.data_dir.mkdir(parents=True, exist_ok=True)
    require_writable(layout.data_dir)
    with Step("Finding loops with Memgraph, then checking direction and chronology") as step:
        driver = connect(args.memgraph_uri)
        try:
            candidates = fetch_cycle_candidates(
                driver, min_len=rules.min_len, max_len=rules.max_len
            )
            results, stats = detect_cycles(candidates, rules)
        finally:
            driver.close()
        path = _save(cycles_frame(results, rules), layout, "cycles")
        step.result = (
            f"{stats.temporal_rings:,} time-respecting cycles "
            f"({stats.directed_rings:,} directed rings in {stats.candidates:,} loops) -> {path}"
        )
    if stats.skipped_dense:
        print(f"note: skipped {stats.skipped_dense} densely connected loops")


def cmd_smurfing(args: argparse.Namespace) -> None:
    from aml.db import open_transactions
    from aml.smurfing import SmurfRules, detect_smurfing

    layout = _layout(args)
    _ready(layout)
    rules = SmurfRules(
        window_seconds=int(args.window_days * DAY),
        min_distinct_senders=args.min_senders,
        min_total=args.min_total,
        sender_currency=None if args.any_currency else args.sender_currency,
        receiver_currency=None if args.any_currency else args.receiver_currency,
    )
    text = f"Scanning rolling {args.window_days:g}-day windows for fan-in bursts (DuckDB)"
    with Step(text, "magenta") as step:
        con = open_transactions(layout.parquet)
        try:
            frame = detect_smurfing(con, rules)
        finally:
            con.close()
        path = _save(frame, layout, "smurfing")
        step.result = (
            f"{frame['Episode_ID'].nunique():,} episodes across "
            f"{frame['Receiver_account'].nunique():,} receivers ({len(frame):,} transactions) "
            f"-> {path}"
        )


def cmd_scatter_gather(args: argparse.Namespace) -> None:
    from aml.db import open_transactions
    from aml.scatter_gather import ScatterGatherRules, detect_scatter_gather

    layout = _layout(args)
    _ready(layout)
    rules = ScatterGatherRules(
        hop_seconds=int(args.hop_days * DAY),
        episode_seconds=int(args.episode_days * DAY),
        min_mules=args.min_mules,
        min_forward_ratio=args.min_forward_ratio,
        max_forward_ratio=args.max_forward_ratio,
    )
    with Step("Joining pass-through legs into scatter-gather episodes", "magenta") as step:
        con = open_transactions(layout.parquet)
        try:
            frame = detect_scatter_gather(con, rules)
        finally:
            con.close()
        path = _save(frame, layout, "scatter_gather")
        step.result = (
            f"{frame['Episode_ID'].nunique():,} episodes through "
            f"{frame['Mule_account'].nunique():,} mules ({len(frame):,} legs) -> {path}"
        )


def cmd_deposit_send(args: argparse.Namespace) -> None:
    from aml.db import open_transactions
    from aml.deposit_send import DepositSendRules, detect_deposit_send

    layout = _layout(args)
    _ready(layout)
    rules = DepositSendRules(
        max_hold_seconds=int(args.max_hold_hours * 3600),
        min_send_ratio=args.min_send_ratio,
        max_send_ratio=args.max_send_ratio,
        min_deposit=args.min_deposit,
        deposit_types=tuple(args.deposit_types) if args.deposit_types else None,
        cash_types=tuple(args.cash_types) if args.cash_types else None,
        cross_border_only=args.cross_border_only,
    )
    with Step("Pairing cash deposits with the payments that move them on", "magenta") as step:
        con = open_transactions(layout.parquet)
        try:
            frame = detect_deposit_send(con, rules)
        finally:
            con.close()
        path = _save(frame, layout, "deposit_send")
        step.result = (
            f"{len(frame):,} deposit-send pairs on {frame['Account'].nunique():,} accounts "
            f"-> {path}"
        )


def cmd_evaluate(args: argparse.Namespace) -> None:
    from aml.db import open_transactions
    from aml.evaluate import evaluate

    layout = _layout(args)
    _ready(layout)
    loaded = outputs.load(layout.data_dir)
    _require(bool(loaded.frames), f"Nothing to score yet. Missing: {loaded.missing_text()}.")
    con = open_transactions(layout.parquet)
    try:
        report = evaluate(
            con,
            **loaded.frames,
            cycle_label=args.cycle_label,
            scatter_gather_label=args.scatter_gather_label,
            deposit_send_label=args.deposit_send_label,
        )
    finally:
        con.close()
    print(report.render())
    saved = report.write(layout.data_dir)
    if saved:
        print("\nSaved " + ", ".join(str(p) for p in saved))
    if loaded.missing:
        print(f"note: not scored, no output yet: {loaded.missing_text()}")


def cmd_export(args: argparse.Namespace) -> None:
    from aml.dashboard import export_results

    layout = _layout(args)
    _require(layout.data_dir.is_dir(), f"{layout.data_dir} doesn't exist - run the pipeline first.")
    require_writable(layout.data_dir)
    path, tables, missing = export_results(layout.data_dir)
    _require(bool(tables), "Nothing to export yet - run the pipeline first.")
    print(f"ok  Exported {len(tables)} tables to {path} (for Metabase)")
    not_yet = outputs.describe(missing)
    if not_yet:
        print(f"note: not exported, no output yet: {not_yet}")


def _metabase_logins(args: argparse.Namespace):
    """Your admin login, plus the view-only login to create (if any).

    Passwords come from MB_ADMIN_PASSWORD / MB_VIEWER_PASSWORD or a hidden prompt, never
    from the command line (shell history) or the repo. Asked for before any spinner starts,
    since a prompt and a spinner can't share the terminal.
    """
    from aml.dashboard import Login

    _require(bool(args.admin_email),
             "Pass --admin-email (or set MB_ADMIN_EMAIL): the admin account you created on "
             "Metabase's Welcome screen (or, with --create-admin, the one to create).")
    password = os.environ.get("MB_ADMIN_PASSWORD")
    if not password:
        password = getpass.getpass(f"Metabase password for {args.admin_email}: ")
        if args.create_admin:  # a typo here would become the admin password
            _require(password == getpass.getpass("Same password again: "),
                     "The two admin passwords didn't match.")
    _require(bool(password), "No admin password given.")
    admin = Login(args.admin_email, password)
    if not args.viewer_email:
        return admin, None
    password = os.environ.get("MB_VIEWER_PASSWORD")
    if not password:
        password = getpass.getpass(f"Password for the view-only login {args.viewer_email}: ")
        _require(password == getpass.getpass("Same password again: "),
                 "The two viewer passwords didn't match.")
    _require(bool(password), "No viewer password given.")
    return admin, Login(args.viewer_email, password)


def cmd_metabase(args: argparse.Namespace) -> None:
    from aml.dashboard import COLLECTION_NAME, DASHBOARD_NAME, setup_metabase

    admin, viewer = getattr(args, "logins", None) or _metabase_logins(args)
    layout = _layout(args)
    with Step(f"Building the dashboard in Metabase at {args.metabase_url}") as step:
        result = setup_metabase(
            layout.data_dir,
            admin=admin,
            create_admin=args.create_admin,
            viewer=viewer,
            url=args.metabase_url,
            container_data_dir=args.metabase_data_dir,
            wait_seconds=args.wait_seconds,
        )
        step.result = (
            f"'{DASHBOARD_NAME}' has {len(result.card_ids)} charts, in the "
            f"'{COLLECTION_NAME}' collection: {result.url}"
        )
    if result.created_admin:
        print(f"ok  Set Metabase up with {admin.email} as its admin account")
    if viewer is not None:
        print(f"ok  View-only login {viewer.email}: {result.viewer}. Share it privately; "
              f"it can only see '{COLLECTION_NAME}'.")
    for note in result.notes:
        print(f"note: {note}")
    for chart in result.skipped:
        print(f"note: no data for {chart}")


def cmd_site(args: argparse.Namespace) -> None:
    from aml.site import build_site

    layout = _layout(args)
    out = Path(args.out)
    with Step("Rendering the dashboard as a static page") as step:
        path, charts, skipped = build_site(layout.results_db, out, repo_url=args.repo_url)
        step.result = f"{charts} charts -> {path}; commit {out}/ and serve it with GitHub Pages"
    for name in skipped:
        print(f"note: no data for {name}")


def cmd_verify_cycle(args: argparse.Namespace) -> None:
    from aml.db import open_transactions
    from aml.temporal import format_ts
    from aml.verify import verify_cycle

    layout = _layout(args)
    _ready(layout, writes_output=False)
    accounts = args.accounts
    con = open_transactions(layout.parquet)
    try:
        chains = verify_cycle(
            con,
            accounts,
            max_window_seconds=_days_to_seconds(args.max_window_days),
            limit=args.limit,
        )
    finally:
        con.close()
    path = " -> ".join(map(str, [*accounts, accounts[0]]))
    print(f"--- Verifying {path} in the source data ---")
    if chains.empty:
        print("No chronological flow found.")
        return
    capped = " (limit reached, use --limit to see more)" if len(chains) >= args.limit else ""
    print(f"Found {len(chains):,} chronological sequence(s){capped}:")
    for i in range(1, len(accounts) + 1):
        chains[f"ts_{i}"] = chains[f"ts_{i}"].map(format_ts)
    print(chains.to_string(index=False))


def cmd_run_all(args: argparse.Namespace) -> None:
    if args.with_metabase:  # ask for passwords, and check them, now, not after a long run
        from aml.dashboard import check_ready

        args.logins = _metabase_logins(args)
        with Step(f"Checking Metabase at {args.metabase_url} and your login") as step:
            note = check_ready(args.metabase_url, args.logins[0],
                               create_admin=args.create_admin)
            step.result = ("Skipped the Metabase check" if note
                           else f"Metabase at {args.metabase_url} is ready")
        if note:
            print(f"note: {note}")
    cmd_fetch_data(args)
    cmd_prepare(args)
    if args.skip_graph:
        print("note: --skip-graph given, skipping Memgraph load and cycle detection")
    else:
        cmd_load_graph(args)
        cmd_cycles(args)
    cmd_smurfing(args)
    cmd_scatter_gather(args)
    cmd_deposit_send(args)
    cmd_evaluate(args)
    cmd_export(args)
    if args.with_metabase:
        cmd_metabase(args)
    else:
        print("note: `uv run aml metabase` builds the Metabase dashboard from these results")
    print("\nPipeline complete.")


# --- argument parsing -----------------------------------------------------------------


def _add_fetch_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--from", dest="local_copy", type=Path, default=None, metavar="CSV",
                   help="use a SAML-D.csv you already have instead of downloading it")


def _add_prepare_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--force", action="store_true", help="rebuild even if already prepared")


def _add_load_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--workers", type=int, default=1,
                   help="batches sent to Memgraph at once (try 4 if you have the RAM)")
    p.add_argument("--batch-size", type=int, default=50_000,
                   help="transfers per batch (default 50000)")
    p.add_argument("--reload", action="store_true", help="drop the graph and load it again")


def _add_cycle_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--min-len", type=int, default=3, help="fewest accounts in a cycle (default 3)")
    p.add_argument("--max-len", type=int, default=20, help="most accounts in a cycle (default 20)")
    p.add_argument("--max-window-days", type=float, default=None,
                   help="max time from first to last hop (default: no limit)")
    p.add_argument("--strict", action="store_true",
                   help="each hop must be strictly later than the previous one")
    p.add_argument("--all-instances", action="store_true",
                   help="write every trip around each ring, not just the tightest")


def _add_smurf_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--window-days", type=float, default=30, help="rolling window (default 30)")
    p.add_argument("--min-senders", type=int, default=10,
                   help="distinct senders in a window, inclusive (default 10)")
    p.add_argument("--min-total", type=float, default=100_000,
                   help="total received in a window, inclusive (default 100000)")
    p.add_argument("--sender-currency", default="UK pounds")
    p.add_argument("--receiver-currency", default="UK pounds")
    p.add_argument("--any-currency", action="store_true", help="don't filter on currency")


def _add_scatter_gather_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--hop-days", type=float, default=7,
                   help="longest a mule holds the money before passing it on (default 7)")
    p.add_argument("--episode-days", type=float, default=30,
                   help="window that holds a whole scatter-gather episode (default 30)")
    p.add_argument("--min-mules", type=int, default=3,
                   help="distinct intermediaries, inclusive (default 3)")
    p.add_argument("--min-forward-ratio", type=float, default=0.5,
                   help="lowest share of the money a mule passes on (default 0.5)")
    p.add_argument("--max-forward-ratio", type=float, default=1.05,
                   help="highest share of the money a mule passes on (default 1.05)")


def _add_deposit_send_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--max-hold-hours", type=float, default=72,
                   help="longest the cash may sit before it is sent on (default 72)")
    p.add_argument("--min-send-ratio", type=float, default=0.8,
                   help="lowest share of the deposit that must be sent on (default 0.8)")
    p.add_argument("--max-send-ratio", type=float, default=1.05,
                   help="highest share of the deposit sent on (default 1.05)")
    p.add_argument("--min-deposit", type=float, default=0,
                   help="ignore cash deposits smaller than this (default 0)")
    p.add_argument("--deposit-types", nargs="+", default=None,
                   help="Payment_type values that are cash deposits "
                        "(default: those containing 'cash' and 'deposit')")
    p.add_argument("--cash-types", nargs="+", default=None,
                   help="Payment_type values that are cash, so don't count as sending the money "
                        "on (default: those containing 'cash')")
    p.add_argument("--cross-border-only", action="store_true",
                   help="only payments into a different bank location")


def _add_label_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--cycle-label", default="Cycle",
                   help="Laundering_type the cycle sweep is scored against")
    p.add_argument("--scatter-gather-label", default="Scatter-Gather",
                   help="Laundering_type the scatter-gather sweep is scored against")
    p.add_argument("--deposit-send-label", default="Deposit-Send",
                   help="Laundering_type the deposit-send sweep is scored against")


def _add_metabase_args(p: argparse.ArgumentParser) -> None:
    from aml.dashboard import DEFAULT_URL

    p.add_argument("--metabase-url", default=os.environ.get("METABASE_URL", DEFAULT_URL))
    p.add_argument("--admin-email", default=os.environ.get("MB_ADMIN_EMAIL"),
                   help="your Metabase admin account; its password comes from "
                        "MB_ADMIN_PASSWORD or a prompt")
    p.add_argument("--create-admin", action="store_true",
                   help="if Metabase hasn't been set up yet, create that admin account "
                        "instead of using the Welcome screen")
    p.add_argument("--viewer-email", default=os.environ.get("MB_VIEWER_EMAIL"),
                   help="also create (or reset) a view-only login; its password comes from "
                        "MB_VIEWER_PASSWORD or a prompt")
    p.add_argument("--metabase-data-dir", default="/data",
                   help="where --data-dir is mounted inside the Metabase container")
    p.add_argument("--wait-seconds", type=float, default=300,
                   help="how long to wait for Metabase to start (default 300)")


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", default="data",
                        help="where SAML-D.csv and all outputs live (default: data)")
    common.add_argument("--memgraph-uri", default=os.environ.get("MEMGRAPH_URI", DEFAULT_URI))

    parser = argparse.ArgumentParser(
        prog="aml", description="AML typology detection on SAML-D."
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="command")

    def command(name: str, handler, help_text: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, parents=[common], help=help_text, description=help_text)
        p.set_defaults(handler=handler)
        return p

    _add_fetch_args(command("fetch-data", cmd_fetch_data,
                            "get SAML-D.csv: data folder, --from, kagglehub cache, then Kaggle"))
    _add_prepare_args(command("prepare", cmd_prepare, "SAML-D.csv -> typed Parquet"))
    _add_load_args(command("load-graph", cmd_load_graph,
                           "stream the Parquet into Memgraph over Bolt"))
    _add_cycle_args(command("cycles", cmd_cycles, "detect cycling / round-tripping"))
    _add_smurf_args(command("smurfing", cmd_smurfing, "detect smurfing / fan-in bursts"))
    _add_scatter_gather_args(
        command("scatter-gather", cmd_scatter_gather, "detect scatter-gather layering via mules")
    )
    _add_deposit_send_args(
        command("deposit-send", cmd_deposit_send,
                "detect cash deposits moved straight on by non-cash payment")
    )
    _add_label_args(
        command("evaluate", cmd_evaluate, "score detector outputs against SAML-D labels")
    )
    command("export", cmd_export, "write all results into data/aml_results.sqlite for Metabase")
    _add_metabase_args(command("metabase", cmd_metabase,
                               "set up Metabase and build the dashboard from the exported results"))

    site = command("site", cmd_site, "render the dashboard as a static page for GitHub Pages")
    site.add_argument("--out", default="docs", help="folder for index.html (default: docs)")
    site.add_argument("--repo-url", default=os.environ.get("AML_REPO_URL"),
                      help="link back to the repository, shown on the page")

    verify = command("verify-cycle", cmd_verify_cycle,
                     "check a cycle against the source data, without Memgraph")
    verify.add_argument("accounts", nargs="+", type=int, help="account ids in path order")
    verify.add_argument("--max-window-days", type=float, default=None)
    verify.add_argument("--limit", type=int, default=1000)

    run_all = command("run-all", cmd_run_all, "fetch, prepare, load, detect, evaluate, export")
    for add in (_add_fetch_args, _add_prepare_args, _add_load_args, _add_cycle_args,
                _add_smurf_args, _add_scatter_gather_args, _add_deposit_send_args,
                _add_label_args, _add_metabase_args):
        add(run_all)
    run_all.add_argument("--with-metabase", action="store_true",
                         help="also set up Metabase and build the dashboard at the end")
    run_all.add_argument("--skip-graph", action="store_true",
                         help="no Memgraph: skip graph load and cycle detection")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.handler(args)
    except CliError as exc:
        if not getattr(exc, "_aml_reported", False):
            print(f"error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        if not getattr(exc, "_aml_reported", False):  # a failing Step already showed it
            print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0
