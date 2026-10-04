"""The Metabase dashboard, built from code instead of a hand-made app database.

``aml export`` copies every result into data/aml_results.sqlite, a file Metabase reads natively
(docker-compose mounts data/ into the Metabase container as /data). ``aml metabase`` then logs
in to Metabase *as your admin account* (which you create yourself on Metabase's Welcome
screen, or which ``--create-admin`` creates from your login on a fresh Metabase; no
credentials live in this repo), connects that file, and puts one question per chart
plus a dashboard into an "AML Forensics" collection. Non-admin users may only view that
collection and can't write their own queries, and an optional view-only login can be created
for sharing the dashboards. Everything is safe to re-run.
"""

from __future__ import annotations

import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from aml import outputs

RESULTS_DB = "aml_results.sqlite"
DATABASE_NAME = "AML results"
DASHBOARD_NAME = "AML detectors"
COLLECTION_NAME = "AML Forensics"
SITE_NAME = "AML Pattern Detection"
DEFAULT_URL = "http://localhost:3001"

def export_results(data_dir: Path) -> tuple[Path, list[str], list[outputs.OutputFile]]:
    """Write every result in aml.outputs that exists as a table of one SQLite file.

    Returns (file, tables written, files not found). Tables whose CSV is gone are dropped,
    so the dashboard never shows stale results.
    """
    loaded = outputs.load(data_dir, outputs.ALL)
    target = Path(data_dir) / RESULTS_DB
    con = sqlite3.connect(target)
    try:
        for output in outputs.ALL:
            if output.key in loaded.frames:
                frame = loaded.frames[output.key]
                frame.to_sql(output.table, con, if_exists="replace", index=False)
            else:
                con.execute(f'DROP TABLE IF EXISTS "{output.table}"')
        con.commit()
    finally:
        con.close()
    written = [o.table for o in outputs.ALL if o.key in loaded.frames]
    return target, written, loaded.missing


def result_tables(path: Path) -> set[str]:
    con = sqlite3.connect(path)
    try:
        return {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    finally:
        con.close()


@dataclass(frozen=True)
class Question:
    name: str
    table: str  # only created when the results database has this table
    sql: str
    display: str
    settings: dict = field(default_factory=dict)


QUESTIONS = (
    Question(
        "Cycling: average amount at each hop", "detected_cycles",
        "SELECT Hop_Number AS hop, ROUND(AVG(Amount), 2) AS avg_amount\n"
        "FROM detected_cycles\nGROUP BY Hop_Number\nORDER BY Hop_Number",
        "funnel", {"funnel.dimension": "hop", "funnel.metric": "avg_amount"},
    ),
    Question(
        "Smurfing: episode duration vs average payment", "smurfing_suspects",
        "SELECT Episode_ID, Receiver_account, MAX(Duration_Days) AS duration_days,\n"
        "       ROUND(AVG(Amount), 2) AS avg_amount, MAX(Sender_count) AS senders\n"
        "FROM smurfing_suspects\nGROUP BY Episode_ID, Receiver_account",
        "scatter", {"graph.dimensions": ["duration_days"], "graph.metrics": ["avg_amount"]},
    ),
    Question(
        "Scatter-gather: episodes by number of mules", "scatter_gather_suspects",
        "SELECT Episode_ID, Source_account, Destination_account, MAX(Mule_count) AS mules,\n"
        "       MAX(Total_scattered) AS scattered, MAX(Total_gathered) AS gathered,\n"
        "       MAX(Gather_Ratio) AS gather_ratio, MIN(Episode_Start) AS started,\n"
        "       MAX(Episode_Duration_Days) AS days\n"
        "FROM scatter_gather_suspects\n"
        "GROUP BY Episode_ID, Source_account, Destination_account\n"
        "ORDER BY mules DESC, scattered DESC",
        "table",
    ),
    Question(
        "Deposit-send: hours between cash deposit and payment", "deposit_send_suspects",
        "SELECT CAST(Hours_Held / 6 AS INTEGER) * 6 AS hours_held, COUNT(*) AS pairs\n"
        "FROM deposit_send_suspects\nGROUP BY 1\nORDER BY 1",
        "bar", {"graph.dimensions": ["hours_held"], "graph.metrics": ["pairs"]},
    ),
    Question(
        "Detector scorecard", "evaluation_summary",
        'SELECT detector, flagged_tx AS "flagged transactions",\n'
        '       ROUND(100 * precision_tx, 2) AS "precision % (transactions)",\n'
        '       ROUND(lift_tx, 1) AS "lift (transactions)",\n'
        '       flagged_accounts AS "flagged accounts",\n'
        '       ROUND(100 * precision_accounts, 2) AS "precision % (accounts)"\n'
        "FROM evaluation_summary",
        "table",
    ),
    Question(
        "Recall by laundering typology", "evaluation_by_typology",
        "SELECT * FROM evaluation_by_typology ORDER BY typology",
        "table",
    ),
)


class MetabaseError(RuntimeError):
    """Metabase answered with an error; the message names the call and Metabase's reply."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class Metabase:
    """Just enough of Metabase's REST API, using only the standard library."""

    def __init__(self, url: str, timeout: float = 60) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.url + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if self.session:
            request.add_header("X-Metabase-Session", self.session)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace").strip()[:400]
            raise MetabaseError(f"{method} {path} -> HTTP {exc.code}: {detail}", exc.code) from None
        return json.loads(raw) if raw.strip() else None

    def wait_until_up(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while True:
            try:
                if (self.call("GET", "/api/health") or {}).get("status") == "ok":
                    return
            except (MetabaseError, OSError):  # still starting, or not running at all
                pass
            if time.monotonic() >= deadline:
                raise MetabaseError(
                    f"Metabase at {self.url} didn't answer within {seconds:.0f}s. "
                    "Is it running (docker compose up -d)? It takes a minute or two to start."
                )
            time.sleep(min(3.0, max(0.1, deadline - time.monotonic())))


def _items(reply) -> list:
    """List endpoints answer with a bare list in older Metabase versions, {"data": [...]} now."""
    if isinstance(reply, dict):
        return reply.get("data") or []
    return reply or []


def _find(mb: Metabase, model: str, name: str) -> dict | None:
    query = urllib.parse.urlencode({"models": model, "q": name})
    for item in _items(mb.call("GET", f"/api/search?{query}")):
        if item.get("model") == model and item.get("name") == name and not item.get("archived"):
            return item
    return None


@dataclass(frozen=True)
class Login:
    email: str
    password: str = field(repr=False)  # never shown in reprs or logs


def create_admin(mb: Metabase, admin: Login, token: str) -> None:
    """Do what Metabase's Welcome screen does: make ``admin`` the first (admin) account."""
    mb.call("POST", "/api/setup", {
        "token": token,
        "user": {"email": admin.email, "password": admin.password,
                 "first_name": "AML", "last_name": "Admin", "site_name": SITE_NAME},
        "prefs": {"site_name": SITE_NAME, "site_locale": "en", "allow_tracking": False},
    })


def _needs_setup(props: dict) -> bool:
    return not props.get("has-user-setup", props.get("setup-token") is None)


def _not_set_up(mb: Metabase) -> MetabaseError:
    return MetabaseError(
        f"Metabase hasn't been set up yet. Open {mb.url}, create your admin account on "
        "the Welcome screen, then run this again with that login (or pass "
        "--create-admin to create it from that login now)."
    )


def check_ready(url: str, admin: Login, *, create_admin: bool = False) -> str | None:
    """Before a long run, fail now on what would otherwise only fail at its very end.

    Checks that Metabase is set up (or will be, with ``create_admin``) and that it accepts
    ``admin``. Returns a note instead if Metabase isn't answering yet: it may well be up by
    the time the dashboard is built.
    """
    mb = Metabase(url, timeout=10)
    try:
        props = mb.call("GET", "/api/session/properties") or {}
    except (MetabaseError, OSError):
        return (f"Metabase at {url} isn't answering yet, so your login can't be checked "
                "until the dashboard is built at the end")
    if _needs_setup(props):
        if not create_admin:
            raise _not_set_up(mb)
        return None  # setup_metabase creates the admin at the end
    log_in(mb, admin)
    return None


def log_in(mb: Metabase, admin: Login, *, create: bool = False) -> bool:
    """Log in as your admin account. Returns True if this call created it.

    The account is the one you created on Metabase's Welcome screen, or, with ``create``
    on a Metabase nobody has set up yet, ``admin`` itself, created through the setup API.
    """
    props = mb.call("GET", "/api/session/properties") or {}
    created = False
    if _needs_setup(props):
        if not create:
            raise _not_set_up(mb)
        if not props.get("setup-token"):
            raise MetabaseError("Metabase isn't set up but offered no setup token; "
                                f"create your admin account at {mb.url} instead")
        create_admin(mb, admin, props["setup-token"])
        created = True
    try:
        credentials = {"username": admin.email, "password": admin.password}
        reply = mb.call("POST", "/api/session", credentials)
    except MetabaseError as exc:
        if exc.status not in (400, 401):
            raise
        raise MetabaseError(
            f"Metabase rejected the login for {admin.email}; check your admin email and password",
            exc.status,
        ) from None
    mb.session = reply["id"]
    me = mb.call("GET", "/api/user/current") or {}
    if not me.get("is_superuser"):
        raise MetabaseError(f"{admin.email} isn't a Metabase admin; log in with your admin account")
    return created


def connect_results(mb: Metabase, path_in_container: str) -> int:
    """Connect the results SQLite file (once) and refresh Metabase's view of its tables."""
    details = {"db": path_in_container}
    for db in _items(mb.call("GET", "/api/database")):
        if db.get("name") == DATABASE_NAME:
            if (db.get("details") or {}).get("db") != path_in_container:
                mb.call("PUT", f"/api/database/{db['id']}",
                        {"name": DATABASE_NAME, "engine": "sqlite", "details": details})
            mb.call("POST", f"/api/database/{db['id']}/sync_schema")
            return db["id"]
    created = mb.call(
        "POST", "/api/database",
        {"engine": "sqlite", "name": DATABASE_NAME, "details": details, "is_full_sync": True},
    )
    return created["id"]


def ensure_collection(mb: Metabase) -> int:
    """The top-level collection holding the questions and dashboard."""
    for c in _items(mb.call("GET", "/api/collection")):
        if (c.get("name") == COLLECTION_NAME and isinstance(c.get("id"), int)
                and not c.get("archived") and c.get("personal_owner_id") is None):
            return c["id"]
    body = {"name": COLLECTION_NAME, "description": "Built by `uv run aml metabase`."}
    try:
        return mb.call("POST", "/api/collection", body)["id"]
    except MetabaseError as exc:
        if exc.status != 400:
            raise
        return mb.call("POST", "/api/collection", {**body, "color": "#509EE3"})["id"]  # < 0.40


def upsert_card(mb: Metabase, question: Question, database_id: int, collection_id: int) -> int:
    payload = {
        "name": question.name,
        "display": question.display,
        "visualization_settings": question.settings,
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "native": {"query": question.sql, "template-tags": {}},
            "database": database_id,
        },
    }
    existing = _find(mb, "card", question.name)
    if existing:
        mb.call("PUT", f"/api/card/{existing['id']}", payload)
        return existing["id"]
    return mb.call("POST", "/api/card", payload)["id"]


def upsert_dashboard(mb: Metabase, card_ids: list[int], collection_id: int) -> int:
    """The dashboard, two charts per row, holding exactly these questions."""
    existing = _find(mb, "dashboard", DASHBOARD_NAME)
    if existing is None:
        existing = mb.call("POST", "/api/dashboard", {
            "name": DASHBOARD_NAME,
            "collection_id": collection_id,
            "description": "Detector outputs and their scores against SAML-D's labels. "
                           "Rebuilt by `uv run aml export` + `uv run aml metabase`.",
        })
    dashboard_id = existing["id"]
    dashboard = mb.call("GET", f"/api/dashboard/{dashboard_id}") or {}
    placed = dashboard.get("dashcards", dashboard.get("ordered_cards")) or []
    current = {dc["card_id"]: dc["id"] for dc in placed if dc.get("card_id") is not None}
    layout = [
        {
            "id": current.get(card_id, -(i + 1)),
            "card_id": card_id,
            "row": (i // 2) * 8,
            "col": (i % 2) * 12,
            "size_x": 12,
            "size_y": 8,
            "series": [],
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for i, card_id in enumerate(card_ids)
    ]
    try:
        mb.call("PUT", f"/api/dashboard/{dashboard_id}", {
            "dashcards": layout,
            "tabs": dashboard.get("tabs") or [],
            "collection_id": collection_id,
        })
    except MetabaseError as modern:
        try:  # Metabase before 0.47 adds cards one at a time
            for dc in layout:
                if dc["id"] < 0:
                    mb.call("POST", f"/api/dashboard/{dashboard_id}/cards", {
                        "cardId": dc["card_id"], "row": dc["row"], "col": dc["col"],
                        "size_x": dc["size_x"], "size_y": dc["size_y"],
                    })
        except MetabaseError as legacy:
            raise MetabaseError(f"couldn't place the charts: {modern}; then: {legacy}") from None
    return dashboard_id


def restrict_viewers(mb: Metabase, collection_id: int, database_id: int) -> list[str]:
    """Non-admins (Metabase's "All Users" group) may only view the AML Forensics collection,
    and may not write their own queries against the results. Admins aren't affected."""
    groups = _items(mb.call("GET", "/api/permissions/group"))
    all_users = next((g["id"] for g in groups if g.get("name") == "All Users"), None)
    if all_users is None:
        return ["no 'All Users' group found; set viewer permissions in Admin > Permissions"]
    group = str(all_users)
    collections = mb.call("GET", "/api/collection/graph")
    mb.call("PUT", "/api/collection/graph", {
        "revision": collections["revision"],
        "groups": {group: {"root": "none", str(collection_id): "read"}},
    })
    data = mb.call("GET", "/api/permissions/graph")
    current = (data.get("groups") or {}).get(group, {}).get(str(database_id))
    if isinstance(current, dict) and "data" in current and "create-queries" not in current:
        return ["this Metabase predates 0.50, so non-admins can still write queries against "
                "AML results; turn that off in Admin > Permissions"]
    mb.call("PUT", "/api/permissions/graph", {
        "revision": data["revision"],
        "groups": {
            group: {str(database_id): {"view-data": "unrestricted", "create-queries": "no"}},
        },
    })
    return []


def ensure_viewer(mb: Metabase, viewer: Login) -> str:
    """Create the view-only login, or set its password if it exists. Never an admin."""
    query = urllib.parse.urlencode({"query": viewer.email})
    found = next(
        (u for u in _items(mb.call("GET", f"/api/user?{query}"))
         if str(u.get("email", "")).lower() == viewer.email.lower()),
        None,
    )
    if found is None:
        mb.call("POST", "/api/user", {
            "first_name": "Dashboard", "last_name": "Viewer",
            "email": viewer.email, "password": viewer.password,
        })
        return "created"
    if found.get("is_superuser"):
        raise MetabaseError(f"{viewer.email} is an admin; use another address for the viewer")
    mb.call("PUT", f"/api/user/{found['id']}/password", {"password": viewer.password})
    return "password updated"


@dataclass
class SetupResult:
    database_id: int
    collection_id: int
    card_ids: list[int]
    dashboard_id: int
    url: str
    viewer: str | None = None  # "created" / "password updated" when a viewer was given
    skipped: list[str] = field(default_factory=list)  # charts without results yet
    notes: list[str] = field(default_factory=list)
    created_admin: bool = False  # True when this run set Metabase up with your login


def setup_metabase(
    data_dir: Path,
    *,
    admin: Login,
    create_admin: bool = False,
    viewer: Login | None = None,
    url: str = DEFAULT_URL,
    container_data_dir: str = "/data",
    wait_seconds: float = 300,
) -> SetupResult:
    results = data_dir / RESULTS_DB
    if not results.is_file():
        raise FileNotFoundError(f"{results} not found - run `aml export` first")
    tables = result_tables(results)
    questions = [q for q in QUESTIONS if q.table in tables]
    skipped = [
        f"{q.name} (run `{outputs.BY_TABLE[q.table].command}`, then `aml export`)"
        for q in QUESTIONS
        if q.table not in tables
    ]
    if not questions:
        raise ValueError(f"{results} has no detector results - run detectors, then `aml export`")
    mb = Metabase(url)
    mb.wait_until_up(wait_seconds)
    created_admin = log_in(mb, admin, create=create_admin)
    collection_id = ensure_collection(mb)
    database_id = connect_results(mb, f"{container_data_dir.rstrip('/')}/{RESULTS_DB}")
    card_ids = [upsert_card(mb, q, database_id, collection_id) for q in questions]
    dashboard_id = upsert_dashboard(mb, card_ids, collection_id)
    notes = restrict_viewers(mb, collection_id, database_id)
    viewer_status = ensure_viewer(mb, viewer) if viewer else None
    url = f"{mb.url}/dashboard/{dashboard_id}"
    return SetupResult(database_id, collection_id, card_ids, dashboard_id, url,
                       viewer_status, skipped, notes, created_admin)
