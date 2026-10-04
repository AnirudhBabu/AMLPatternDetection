# v0.9: public dashboard, Metabase dashboard, deposit-send and scatter-gather detection, correctness fixes, evaluation, tests and CI

## v0.9: a public copy of the dashboard

- `uv run aml site` runs the dashboard's own questions (the same SQL, on the same exported results) and renders them into `docs/index.html`: one static page with interactive charts for GitHub Pages, so anyone viewing the repo can see the dashboard. There's no server, login or database behind it, and it holds only aggregated results from the synthetic SAML-D dataset.
- Everything on the page is escaped, including the chart data embedded in it, so no value in the results can inject markup or script. Tables show at most 100 rows and charts at most 5,000 points.
- GitHub Pages serves the page from the `/docs` folder.

## v0.8: no credentials in the repo, and spinners that don't clash

- **No passwords in the README or the code.** Each user creates their own admin account on Metabase's Welcome screen. `aml metabase --admin-email ...` logs in with it, taking the password from `MB_ADMIN_PASSWORD` or a hidden prompt, instead of creating a guest admin. The guest login and default password that earlier versions of the README listed are removed; that password remains in the repository's history and should be treated as public.
- **View-only sharing.** `--viewer-email` creates (or resets the password of) a non-admin login, with its password from `MB_VIEWER_PASSWORD` or a prompt, and refuses an address that belongs to an admin. The questions and dashboard live in an "AML Forensics" collection. Everyone who isn't an admin can view only that collection and can't write queries (Metabase 0.50+; older versions get a note).
- Passwords are no longer accepted as flags, since command lines end up in shell history. `.env` is gitignored for anyone who keeps `MB_*` variables in a file.
- **Spinners.** Only one draws at a time; a step started inside another prints plain lines. When output isn't a terminal (pipes, CI logs) the CLI prints plain lines instead of spinner frames. DuckDB's own progress bar, which drew over the spinner, is off. Memgraph's notices no longer print over it. Password prompts happen before any spinner starts (`run-all --with-metabase` asks up front). A failed step reports its error once instead of twice. Without a terminal, progress lines print at most every 10 s, and the first one always prints.

## v0.7: one loader per kind of file, nothing generated in git

- **`load-graph` streams instead of using `LOAD CSV`.** It reads the prepared Parquet in batches and sends them to Memgraph over Bolt (`UNWIND`). Memgraph never opens a file, so there's no container path to get wrong, no mount, and no half-loaded graph from a missing chunk. `prepare` now writes only the Parquet (no more `accounts.csv` / `transfers_*.csv`). `--chunks` and `--memgraph-data-dir` are gone; `--batch-size` and `--workers` control the stream. Memgraph no longer mounts `data/`, its init step no longer touches it, and the CI integration job builds its test graph with this loader.
- **`fetch-data` tries sources in order**: the data folder, a file you already have (`--from path/to/SAML-D.csv`, copied, never moved), kagglehub's cache, then Kaggle.
- **One registry and loader for result files** (`aml/outputs.py`). `evaluate`, `export` and `metabase` all read results through it and report a missing file the same way: the file, and the command that creates it. A file written by an older version is rejected with the command to rerun, and `evaluate` deletes sweep files for detectors that no longer have output.
- **`aml metabase`**: a rejected login now says how to recover, and an account that isn't an admin is refused before anything changes.
- **`.gitignore`** covers the virtual environment and Python caches, everything under `data/` (SAML-D, the Parquet copy, results; `data/.gitkeep` keeps the folder) and the Memgraph and Metabase volumes.

## v0.6: the Metabase dashboard is built from code

A fresh checkout opened Metabase on its setup screen: the dashboard lived only in the binary app database under `metabase/`, which the compose file's `MB_DB_FILE` doesn't point at, and nothing loaded the detector results into a database Metabase can query.

- `uv run aml export` writes every detector output and evaluation table into `data/aml_results.sqlite` (dropping tables whose CSV is gone).
- `uv run aml metabase` uses Metabase's REST API to finish first-run setup with the README's login (or log in), connect that file, and create the "AML detectors" dashboard: cycling funnel, smurfing duration vs average payment, scatter-gather episodes, deposit-send hours held, detector scorecard and recall by typology. Re-running updates the same connection, questions and dashboard instead of adding new ones. Uses only the standard library; supports the current dashboard API and the pre-0.47 one.
- `uv run aml run-all` now ends with the export; `--with-metabase` also builds the dashboard.
- Tested against a stand-in Metabase server: first run, re-run, wrong password, older Metabase, Metabase not running, and the CLI.

## v0.5.1: data folder permissions

- **`fetch-data` could fail with `Permission denied: 'data/SAML-D.csv'` after a full download.** The init step in `docker-compose.yml` ran `chown -R 101:103 ... /data && chmod -R 777 /data`; if the `chown` hit any error, the `&&` skipped the `chmod` and left `data/` owned by Memgraph's user and read-only for the user running the pipeline. The init step now leaves `data/`'s owner alone and only opens the folder up (`chmod -R a+rwX /data`), even if the `chown` of Memgraph's own folders fails.
- Every command that writes to `data/` checks it before starting, and says how to fix it, instead of failing after the work is done.
- `fetch-data` reuses a dataset already in kagglehub's cache (for example, one left there by a run that couldn't move it) instead of wiping the cache and downloading again, and no longer draws a spinner over kagglehub's progress bar.

## New typology: deposit-send (cash in, sent straight on)

Replaces the v0.4 behavioral-change detector. Its six "first-ever" signals fire constantly on ordinary accounts, so it produced a flood of alerts and stalled on the full dataset.

`uv run aml deposit-send` pairs each cash deposit with the first non-cash payment from the same account to a different account that sends on `--min-send-ratio` to `--max-send-ratio` (0.8 to 1.05) of the deposit within `--max-hold-hours` (72).

- Cash-deposit and cash payment types are read from the data (or set with `--deposit-types` / `--cash-types`); if no cash-deposit type exists, it stops and lists the types that do. `--cross-border-only` keeps payments into a different bank location.
- Only cash deposits are joined against later payments, on (account, time bucket) plus (account, next bucket), so the join stays small; the output table is built column-wise.
- Output: `data/deposit_send_suspects.csv`, one row per pair with hours held, share sent on, destination, and the account's total number of pairs.
- `aml evaluate` scores it like the other detectors and adds a holding-time sweep (`data/evaluation_deposit_send_sweep.csv`) against `--deposit-send-label` (default `Deposit-Send`).
- Tests cover decoys (sent too late, kept most of it, withdrawn as cash, paid out before the deposit) and check the join against brute force on random data.

## New typology: scatter-gather (layering through mules)

`uv run aml scatter-gather` finds a source account paying several intermediaries that each pass most of the money on to the same destination shortly afterwards, the classic mule-layering shape. A leg is a payment from the source to a mule, followed by a payment from that mule to the destination within `--hop-days` (default 7), passing on between `--min-forward-ratio` and `--max-forward-ratio` (0.5 to 1.05) of the amount. An episode needs `--min-mules` (3) distinct mules inside `--episode-days` (30).

- Runs in DuckDB as a self-join on (account, time bucket) plus (account, next bucket), so only payments close in time get paired; a busy account's incoming payments are never crossed with all of its outgoing ones.
- Output: `data/scatter_gather_suspects.csv`, one row per leg with hold time and forward ratio, plus each episode's mule count, totals and gather ratio.
- `aml evaluate` scores it like the other detectors and adds a mule-count sweep (`data/evaluation_scatter_gather_sweep.csv`).
- `queries/cypher/show_scatter_gather.cypher` draws an episode in Memgraph Lab.
- Tests check the bucketed join against brute force on random data, the episode windowing, and decoys that pass the money on too late, pass on too little, or pay the destination before being paid.

## Bug fixes (these change the results)

**Cycles that Memgraph listed backwards or out of order were dropped.** `cycles.get()` runs on the undirected graph and doesn't guarantee any node order. The old query walked `cycle_nodes[i] -> cycle_nodes[i + 1]` as if the list followed the money, so a directed cycle that came back reversed or shuffled failed the edge check and disappeared. Memgraph now only supplies each loop's accounts plus every transfer among them, and `aml/temporal.py` works out the directed ring(s) through those accounts itself.

**Real round trips were lost to the earliest-transfer shortcut.** For each hop the old query kept only the earliest transfer, then looked for a rotation in time order. Example: C→A on day 3, A→B on day 8 and B→C on day 9 is a valid cycle, but an earlier A→B on day 1 replaced the day-8 transfer and the cycle vanished. The new check tries every possible first transfer and then takes the earliest *eligible* transfer at each later hop. That is guaranteed to find every trip that exists, and the tests cross-check it against brute force on hundreds of random graphs.

**Smurfing required a receiver's whole history to fit inside 30 days.** The start and end times were taken over all of a receiver's transactions, so a mule account with any ordinary activity months before or after a burst could never be flagged. Every incoming payment now closes a rolling 30-day window; qualifying windows are merged into episodes, and `Sender_count`, `Total_amount` and `Duration_Days` describe the episode.

**The thresholds were exclusive.** `Sender_count > 10` meant exactly ten senders never counted, even though the parameter is a minimum. Both bars are now inclusive (`>=`).

**`test_cycle_existence.py` reported impossible chains.** Its final `FROM hop1, hop2, ...` had no join conditions, so it returned the cross product of all hops, including sequences that run backwards in time. Because of its `test_` name, pytest would also import it and scan the full 950 MB CSV. It is replaced by `aml verify-cycle`, where each hop joins to the previous one.

**Loading the graph timed out.** `MERGE` on 9.5M relationships with properties is slow, and it folded transactions with identical amount, time and type into a single relationship. One big `LOAD CSV` could then exceed Memgraph's default 600 s query timeout on slower machines. Relationships are now `CREATE`d from 8 chunk files (one statement each, optionally in parallel), and docker-compose raises the timeout.

**Failures looked like success.** Memgraph errors were caught and turned into an empty result, followed by "Pipeline Complete". Errors now stop the pipeline with a non-zero exit code.

## Additions

- `aml evaluate`: precision, lift and per-typology recall against SAML-D's labels, at transaction and account level, plus a precision/recall sweep over maximum cycle duration.
- New cycle options: `--max-window-days`, `--strict`, `--all-instances`, `--min-len` / `--max-len`.
- New output columns, appended after the original ones:
  - `detected_cycles.csv`: `Tx_ID, Ring_ID, Originator, Cycle_Start, Cycle_Duration_Hours, Amount_Retention, Valid_Instances`
  - `smurfing_suspects.csv`: `Episode_ID, Episode_Start, Episode_End, Tx_ID, Is_laundering`
- `aml prepare` writes a typed Parquet copy of the data with a stable `tx_id`, so DuckDB never re-parses the CSV and every detection can be traced to the exact transaction.
- A CLI (`uv run aml <command>`) instead of commenting code in and out. `python data_pattern_scanner.py` still runs the whole pipeline.
- Cypher for Memgraph Lab in `queries/cypher/`: bulk import, the loop search, and a query to visualise one detected ring.

## Engineering

- `pyproject.toml` + uv replace `requirements.in` / `requirements.txt`.
- Generated files (SAML-D, results, the Memgraph and Metabase volumes) are no longer tracked, and the original scripts that the package replaces (`test_cycle_existence.py`, `requirements.in`, `requirements.txt` and the old load and cycle Cypher scripts) are removed.
- pytest suite, and GitHub Actions CI with a second job that runs the Cypher against a real `memgraph/memgraph-mage` container.
- docker-compose: higher query timeout; image tags can be pinned through `MEMGRAPH_TAG`, `MEMGRAPH_LAB_TAG` and `METABASE_TAG`.

## Upgrading from the original script

- **Reload the graph** (`uv run aml load-graph --reload`). Relationships now carry `tx_id` and `ts` (epoch seconds), which `aml cycles` needs; `type` and `datetime` are kept for Lab. The extra `:Sender` / `:Receiver` labels are gone.
- `Timestamp` in `detected_cycles.csv` is now formatted `YYYY-MM-DD HH:MM:SS`.
- A receiver can have several smurfing episodes, and only the transactions inside an episode are listed. For per-burst charts in Metabase, group by `Episode_ID` rather than `Receiver_account`.
- Defaults keep the old intent: UK pounds both ways, 30 days, 10 senders, 100,000, cycles of at least 3 accounts and no time cap. New: cycles are capped at 20 accounts (`--max-len`).
