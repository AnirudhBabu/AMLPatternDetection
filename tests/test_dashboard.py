import contextlib
import copy
import itertools
import json
import os
import socket
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pytest

import aml.cli
from aml.cli import main
from aml.dashboard import (
    QUESTIONS,
    Login,
    Metabase,
    MetabaseError,
    export_results,
    result_tables,
    setup_metabase,
)

ADMIN = Login("me@example.com", "admin-Pass-1")
VIEWER = Login("guest@example.com", "viewer-Pass-1")


def _public(user):
    return {k: v for k, v in user.items() if k != "password"}


class FakeMetabase:
    """A stand-in for the parts of Metabase's REST API that `aml metabase` uses."""

    def __init__(self, *, users=None, legacy=False, legacy_permissions=False):
        self._ids = itertools.count(10)
        # users: {email: (password, is_admin)}; none at all means nobody has set Metabase up
        self.users = {
            email: {"id": next(self._ids), "email": email, "password": pw, "is_superuser": admin}
            for email, (pw, admin) in (users or {}).items()
        }
        self.legacy = legacy  # pre-0.47: no dashcards PUT, cards added one by one
        self.legacy_permissions = legacy_permissions  # pre-0.50 permissions graph format
        self.sessions = {}
        self.databases, self.cards, self.dashboards, self.collections = {}, {}, {}, {}
        self.collection_graph = {"1": {"root": "write"}, "2": {"root": "write"}}
        self.data_graph = {"1": {}, "2": {}}
        self.syncs = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()

    def handle(self, method, path, query, body, session):
        if path == "/api/health":
            return 200, {"status": "ok"}
        if path == "/api/session/properties":
            return 200, {"has-user-setup": bool(self.users),
                         "setup-token": None if self.users else "setup-123"}
        if (method, path) == ("POST", "/api/setup"):
            if self.users or body.get("token") != "setup-123":
                return 403, "The /api/setup route can only be used to create the first user."
            user = body["user"]
            self.users[user["email"]] = {"id": next(self._ids), "email": user["email"],
                                         "password": user["password"], "is_superuser": True}
            self.site_name = body["prefs"]["site_name"]
            token = f"session-{next(self._ids)}"
            self.sessions[token] = user["email"]
            return 200, {"id": token}
        if (method, path) == ("POST", "/api/session"):
            user = self.users.get(body["username"])
            if user is None or user["password"] != body["password"]:
                return 401, {"errors": {"password": "did not match stored password"}}
            token = f"session-{next(self._ids)}"
            self.sessions[token] = user["email"]
            return 200, {"id": token}
        me = self.users.get(self.sessions.get(session))
        if me is None:
            return 401, "Unauthenticated"
        if path == "/api/user/current":
            return 200, _public(me)
        if not me["is_superuser"]:
            return 403, "You don't have permissions to do that."
        route = path.split("/")[2:]
        if route == ["user"]:
            if method == "GET":
                needle = query.get("query", [""])[0].lower()
                return 200, {"data": [_public(u) for u in self.users.values()
                                      if needle in u["email"].lower()]}
            if body["email"] in self.users:
                return 400, {"errors": {"email": "Email address already in use."}}
            user = {"id": next(self._ids), "email": body["email"], "password": body["password"],
                    "is_superuser": False}
            self.users[user["email"]] = user
            return 200, _public(user)
        if route[0] == "user" and route[2:] == ["password"]:
            user = next(u for u in self.users.values() if u["id"] == int(route[1]))
            user["password"] = body["password"]
            return 200, _public(user)
        if route == ["permissions", "group"]:
            return 200, [{"id": 1, "name": "All Users"}, {"id": 2, "name": "Administrators"}]
        if route == ["collection"]:
            if method == "GET":
                return 200, [{"id": "root", "name": "Our analytics"}, *self.collections.values()]
            collection = {"id": next(self._ids), "name": body["name"], "archived": False,
                          "personal_owner_id": None}
            self.collections[collection["id"]] = collection
            return 200, collection
        if route == ["collection", "graph"]:
            if method == "PUT":
                for group, perms in body["groups"].items():
                    self.collection_graph.setdefault(group, {}).update(perms)
            return 200, {"revision": 1, "groups": copy.deepcopy(self.collection_graph)}
        if route == ["permissions", "graph"]:
            if method == "PUT":
                for group, dbs in body["groups"].items():
                    self.data_graph.setdefault(group, {}).update(dbs)
            return 200, {"revision": 1, "groups": copy.deepcopy(self.data_graph)}
        if route == ["database"]:
            if method == "GET":
                return 200, {"data": list(self.databases.values()), "total": len(self.databases)}
            db = {"id": next(self._ids), **body}
            self.databases[db["id"]] = db
            self.data_graph["1"][str(db["id"])] = (
                {"data": {"native": "write", "schemas": "all"}} if self.legacy_permissions
                else {"view-data": "unrestricted", "create-queries": "query-builder-and-native"})
            return 200, db
        if route[0] == "database" and route[2:] == ["sync_schema"]:
            self.syncs += 1
            return 200, {"status": "ok"}
        if route[0] == "database" and method == "PUT":
            self.databases[int(route[1])].update(body)
            return 200, self.databases[int(route[1])]
        if route == ["search"]:
            pool = {"card": self.cards, "dashboard": self.dashboards}[query["models"][0]]
            needle = query["q"][0].lower()
            hits = [{"model": query["models"][0], "id": i, "name": o["name"], "archived": False}
                    for i, o in pool.items() if needle in o["name"].lower()]
            return 200, {"data": hits}
        if route[0] == "card":
            if method == "POST":
                card = {"id": next(self._ids), **body}
                self.cards[card["id"]] = card
                return 200, card
            self.cards[int(route[1])].update(body)
            return 200, self.cards[int(route[1])]
        if route == ["dashboard"]:
            dash = {"id": next(self._ids), "name": body["name"], "dashcards": [], "tabs": [],
                    "collection_id": body.get("collection_id")}
            self.dashboards[dash["id"]] = dash
            return 200, dash
        dash = self.dashboards[int(route[1])]
        if method == "GET":
            key = "ordered_cards" if self.legacy else "dashcards"
            return 200, {"id": dash["id"], "name": dash["name"], key: dash["dashcards"]}
        if method == "PUT" and not self.legacy:
            dash["dashcards"] = [{**dc, "id": dc["id"] if dc["id"] > 0 else next(self._ids)}
                                 for dc in body["dashcards"]]
            dash["collection_id"] = body.get("collection_id", dash["collection_id"])
            return 200, dash
        if method == "POST" and route[2:] == ["cards"] and self.legacy:
            dash["dashcards"].append({"id": next(self._ids), "card_id": body["cardId"],
                                      "row": body["row"], "col": body["col"]})
            return 200, dash["dashcards"][-1]
        return 404, {"message": f"no route for {method} {path}"}

    def _handler(fake):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _route(self, method):
                url = urlparse(self.path)
                size = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(size)) if size else None
                status, reply = fake.handle(method, url.path, parse_qs(url.query), body,
                                            self.headers.get("X-Metabase-Session"))
                raw = json.dumps(reply).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                self._route("GET")

            def do_POST(self):
                self._route("POST")

            def do_PUT(self):
                self._route("PUT")

        return Handler


def results(tmp: str) -> Path:
    data = Path(tmp) / "data"
    data.mkdir()
    pd.DataFrame({"Cycle_ID": [1, 1], "Hop_Number": [1, 2], "Amount": [100.0, 90.0],
                  "Tx_ID": [1, 2]}).to_csv(data / "detected_cycles.csv", index=False)
    pd.DataFrame({"detector": ["cycling"], "flagged_tx": [2], "precision_tx": [0.5],
                  "lift_tx": [3.0], "flagged_accounts": [2], "precision_accounts": [1.0],
                  "lift_accounts": [2.0]}).to_csv(data / "evaluation_summary.csv", index=False)
    return data


def admin_only():
    return {ADMIN.email: (ADMIN.password, True)}


def build(tmp, fake, **kwargs):
    data = results(tmp)
    export_results(data)
    return setup_metabase(data, admin=kwargs.pop("admin", ADMIN), url=fake.url,
                          wait_seconds=5, **kwargs)


@contextlib.contextmanager
def environment(**values):
    saved = {k: os.environ.get(k) for k in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_export_writes_one_table_per_result_and_drops_stale_ones():
    with tempfile.TemporaryDirectory() as tmp:
        data = results(tmp)
        path, written, missing = export_results(data)
        assert written == ["detected_cycles", "evaluation_summary"]
        assert [m.table for m in missing if m.always_written] == [
            "smurfing_suspects", "scatter_gather_suspects", "deposit_send_suspects",
            "evaluation_by_typology"]
        con = sqlite3.connect(path)
        assert con.execute("SELECT SUM(Amount) FROM detected_cycles").fetchone() == (190.0,)
        con.close()
        (data / "evaluation_summary.csv").unlink()
        export_results(data)
        assert result_tables(path) == {"detected_cycles"}


def test_waits_for_you_to_create_the_admin_account_yourself():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase() as fake:
        with pytest.raises(MetabaseError, match="hasn't been set up yet.*--create-admin"):
            build(tmp, fake)
        assert fake.users == {}  # no account was created on your behalf


def test_create_admin_sets_up_a_fresh_metabase_with_your_login():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase() as fake:
        result = build(tmp, fake, create_admin=True)
        assert result.created_admin and len(fake.dashboards) == 1
        assert list(fake.users) == [ADMIN.email] and fake.users[ADMIN.email]["is_superuser"]
        assert fake.users[ADMIN.email]["password"] == ADMIN.password
        assert fake.site_name == "AML Pattern Detection"


def test_create_admin_only_logs_in_once_metabase_is_set_up():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        result = build(tmp, fake, create_admin=True)
        assert not result.created_admin and list(fake.users) == [ADMIN.email]
        with pytest.raises(MetabaseError, match="rejected the login"):  # no second admin
            setup_metabase(Path(tmp) / "data", admin=Login(ADMIN.email, "wrong"),
                           create_admin=True, url=fake.url, wait_seconds=5)


def test_builds_the_dashboard_in_its_own_collection():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        result = build(tmp, fake)
        [collection] = fake.collections.values()
        assert collection["name"] == "AML Forensics" and result.collection_id == collection["id"]
        names = [fake.cards[i]["name"] for i in result.card_ids]
        assert names == ["Cycling: average amount at each hop", "Detector scorecard"]
        assert {fake.cards[i]["collection_id"] for i in result.card_ids} == {collection["id"]}
        dash = fake.dashboards[result.dashboard_id]
        assert dash["collection_id"] == collection["id"]
        assert [(d["card_id"], d["row"], d["col"]) for d in dash["dashcards"]] == [
            (result.card_ids[0], 0, 0), (result.card_ids[1], 0, 12)]
        assert len(result.skipped) == 4 and result.viewer is None


def test_non_admins_can_only_view_that_collection_and_cannot_write_queries():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        result = build(tmp, fake)
        assert fake.collection_graph["1"] == {"root": "none", str(result.collection_id): "read"}
        assert fake.data_graph["1"][str(result.database_id)] == {
            "view-data": "unrestricted", "create-queries": "no"}
        assert fake.collection_graph["2"] == {"root": "write"}  # admins untouched
        assert result.notes == []


def test_creates_a_view_only_login_and_reruns_only_reset_its_password():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        assert build(tmp, fake, viewer=VIEWER).viewer == "created"
        viewer = fake.users[VIEWER.email]
        assert (viewer["is_superuser"], viewer["password"]) == (False, VIEWER.password)

    with tempfile.TemporaryDirectory() as tmp:
        fake2_users = {**admin_only(), VIEWER.email: ("old-Pass-0", False)}
        with FakeMetabase(users=fake2_users) as fake:
            new = Login(VIEWER.email, "new-Pass-2")
            assert build(tmp, fake, viewer=new).viewer == "password updated"
            assert fake.users[VIEWER.email]["password"] == "new-Pass-2"
            assert len(fake.users) == 2


def test_never_turns_an_admin_into_the_viewer():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        with pytest.raises(MetabaseError, match="is an admin"):
            build(tmp, fake, viewer=Login(ADMIN.email, "x"))


def test_a_rejected_or_non_admin_login_changes_nothing():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        with pytest.raises(MetabaseError, match="rejected the login for me@example.com"):
            build(tmp, fake, admin=Login(ADMIN.email, "wrong"))
    users = {"user@example.com": ("user-Pass-1", False)}
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=users) as fake:
        with pytest.raises(MetabaseError, match="isn't a Metabase admin"):
            build(tmp, fake, admin=Login("user@example.com", "user-Pass-1"))
        assert fake.databases == {} and fake.cards == {}


def test_older_permission_format_is_left_alone_with_a_note():
    with tempfile.TemporaryDirectory() as tmp:
        with FakeMetabase(users=admin_only(), legacy_permissions=True) as fake:
            result = build(tmp, fake)
            assert "predates 0.50" in result.notes[0]
            assert "data" in fake.data_graph["1"][str(result.database_id)]


def test_rerunning_updates_instead_of_duplicating():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        data = results(tmp)
        export_results(data)
        first = setup_metabase(data, admin=ADMIN, url=fake.url, wait_seconds=5)
        second = setup_metabase(data, admin=ADMIN, url=fake.url, wait_seconds=5)
        assert (len(fake.databases), len(fake.cards), len(fake.dashboards)) == (1, 2, 1)
        assert len(fake.collections) == 1
        assert (second.card_ids, second.dashboard_id) == (first.card_ids, first.dashboard_id)
        assert fake.syncs == 1  # the second run re-syncs the existing connection


def test_falls_back_to_adding_cards_one_by_one_on_old_metabase():
    with tempfile.TemporaryDirectory() as tmp:
        with FakeMetabase(users=admin_only(), legacy=True) as fake:
            result = build(tmp, fake)
            placed = fake.dashboards[result.dashboard_id]["dashcards"]
            assert [d["card_id"] for d in placed] == result.card_ids


def test_gives_up_with_a_clear_message_when_metabase_is_down():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(MetabaseError, match="didn't answer"):
        Metabase(f"http://127.0.0.1:{port}").wait_until_up(0.5)


def test_cli_reads_passwords_from_the_environment():
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        data = results(tmp)
        args = ["--data-dir", str(data), "--metabase-url", fake.url, "--wait-seconds", "5"]
        with environment(MB_ADMIN_PASSWORD=ADMIN.password, MB_ADMIN_EMAIL=ADMIN.email,
                         MB_VIEWER_EMAIL=None, MB_VIEWER_PASSWORD=None):
            assert main(["export", "--data-dir", str(data)]) == 0
            assert main(["metabase", *args]) == 0
        assert len(fake.dashboards) == 1


def test_cli_asks_for_passwords_before_starting_and_needs_an_admin():
    asked = []
    answers = iter([ADMIN.password, VIEWER.password, VIEWER.password])

    def fake_getpass(prompt):
        asked.append((prompt, aml.cli._active_steps))  # no Step (spinner) may be running
        return next(answers)

    with tempfile.TemporaryDirectory() as tmp, FakeMetabase(users=admin_only()) as fake:
        data = results(tmp)
        export_results(data)
        args = ["metabase", "--data-dir", str(data), "--metabase-url", fake.url,
                "--wait-seconds", "5"]
        original = aml.cli.getpass.getpass
        aml.cli.getpass.getpass = fake_getpass
        try:
            with environment(MB_ADMIN_PASSWORD=None, MB_VIEWER_PASSWORD=None,
                             MB_ADMIN_EMAIL=None, MB_VIEWER_EMAIL=None):
                assert main(args) == 2  # no --admin-email: refused before anything happens
                assert asked == []
                full = [*args, "--admin-email", ADMIN.email, "--viewer-email", VIEWER.email]
                assert main(full) == 0
        finally:
            aml.cli.getpass.getpass = original
        assert [prompt.split(" ")[0] for prompt, _ in asked] == ["Metabase", "Password", "Same"]
        assert all(active == 0 for _, active in asked)
        assert fake.users[VIEWER.email]["is_superuser"] is False


def test_cli_create_admin_asks_for_the_new_password_twice():
    answers = iter([ADMIN.password, ADMIN.password])
    asked = []

    def fake_getpass(prompt):
        asked.append(prompt.split(" ")[0])
        return next(answers)

    with tempfile.TemporaryDirectory() as tmp, FakeMetabase() as fake:
        data = results(tmp)
        export_results(data)
        original = aml.cli.getpass.getpass
        aml.cli.getpass.getpass = fake_getpass
        try:
            with environment(MB_ADMIN_PASSWORD=None, MB_VIEWER_PASSWORD=None,
                             MB_ADMIN_EMAIL=None, MB_VIEWER_EMAIL=None):
                assert main(["metabase", "--data-dir", str(data), "--metabase-url", fake.url,
                             "--wait-seconds", "5", "--admin-email", ADMIN.email,
                             "--create-admin"]) == 0
        finally:
            aml.cli.getpass.getpass = original
        assert asked == ["Metabase", "Same"]
        assert fake.users[ADMIN.email]["is_superuser"] and len(fake.dashboards) == 1


PIPELINE = ("cmd_fetch_data", "cmd_prepare", "cmd_load_graph", "cmd_cycles", "cmd_smurfing",
            "cmd_scatter_gather", "cmd_deposit_send", "cmd_evaluate", "cmd_export",
            "cmd_metabase")


def run_all(monkeypatch, url, *extra, password=ADMIN.password):
    """`aml run-all --with-metabase` with every pipeline step replaced by a recorder."""
    ran = []
    for name in PIPELINE:
        monkeypatch.setattr(aml.cli, name, lambda args, name=name: ran.append(name))
    with environment(MB_ADMIN_PASSWORD=password, MB_ADMIN_EMAIL=ADMIN.email,
                     MB_VIEWER_EMAIL=None, MB_VIEWER_PASSWORD=None):
        code = main(["run-all", "--with-metabase", "--metabase-url", url, *extra])
    return code, ran


def test_run_all_stops_before_the_pipeline_when_metabase_isnt_set_up(monkeypatch):
    with FakeMetabase() as fake:
        code, ran = run_all(monkeypatch, fake.url)
        assert (code, ran) == (1, [])  # not after an hours-long run
        assert fake.users == {}


def test_run_all_stops_before_the_pipeline_on_a_wrong_password(monkeypatch):
    with FakeMetabase(users=admin_only()) as fake:
        assert run_all(monkeypatch, fake.url, password="wrong") == (1, [])


def test_run_all_with_create_admin_runs_on_a_fresh_metabase(monkeypatch):
    with FakeMetabase() as fake:
        code, ran = run_all(monkeypatch, fake.url, "--create-admin")
        assert (code, ran) == (0, list(PIPELINE))
        assert fake.users == {}  # created by cmd_metabase at the end, stubbed here


def test_run_all_goes_ahead_when_metabase_isnt_up_yet(monkeypatch, capsys):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    code, ran = run_all(monkeypatch, f"http://127.0.0.1:{port}")
    assert (code, ran) == (0, list(PIPELINE))
    assert "isn't answering yet" in capsys.readouterr().out


def test_run_all_with_metabase_builds_the_dashboard_end_to_end(monkeypatch):
    with tempfile.TemporaryDirectory() as tmp, FakeMetabase() as fake:
        data = results(tmp)
        for name in PIPELINE[:-2]:  # keep the real export and metabase steps
            monkeypatch.setattr(aml.cli, name, lambda args: None)
        with environment(MB_ADMIN_PASSWORD=ADMIN.password, MB_ADMIN_EMAIL=ADMIN.email,
                         MB_VIEWER_EMAIL=None, MB_VIEWER_PASSWORD=None):
            assert main(["run-all", "--with-metabase", "--create-admin", "--data-dir",
                         str(data), "--metabase-url", fake.url, "--wait-seconds", "5"]) == 0
        assert fake.users[ADMIN.email]["is_superuser"] and len(fake.dashboards) == 1


def test_queries_sql_holds_the_dashboard_questions():
    folder = Path(__file__).resolve().parent.parent / "queries" / "sql"
    expected = {f"{q.table}.sql": f"-- {q.name}\n{q.sql}\n" for q in QUESTIONS}
    actual = {p.name: p.read_text() for p in folder.glob("*.sql")}
    assert actual == expected, "queries/sql/ is out of date with aml.dashboard.QUESTIONS"
