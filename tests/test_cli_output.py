import contextlib
import io
import sys
import tempfile
import types

from aml.cli import Step, main


class FakeHalo:
    started = []

    def __init__(self, text, color, spinner):
        self.text, self.events = text, []
        FakeHalo.started.append(self)

    def start(self):
        self.events.append("start")

    def succeed(self, message):
        self.events.append(("succeed", message))

    def fail(self, message):
        self.events.append(("fail", message))

    def stop(self):
        self.events.append("stop")


class Terminal(io.StringIO):
    def isatty(self):
        return True


@contextlib.contextmanager
def console(stdout):
    saved = sys.stdout, sys.stderr, sys.modules.get("halo")
    sys.stdout, sys.stderr = stdout, io.StringIO()
    sys.modules["halo"] = types.SimpleNamespace(Halo=FakeHalo)
    FakeHalo.started = []
    try:
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = saved[0], saved[1]
        if saved[2] is None:
            sys.modules.pop("halo", None)
        else:
            sys.modules["halo"] = saved[2]


def test_only_one_spinner_draws_at_a_time():
    with console(Terminal()) as (out, _):
        with Step("outer") as outer:
            with Step("inner") as inner:
                inner.result = "inner done"
            outer.result = "outer done"
    assert [s.text for s in FakeHalo.started] == ["outer"]
    assert FakeHalo.started[0].events == ["start", ("succeed", "outer done")]
    assert out.getvalue() == "... inner\nok  inner done\n"


def test_plain_lines_when_output_is_not_a_terminal():
    with console(io.StringIO()) as (out, _):
        with Step("loading") as step:
            for n in range(100):
                step.update(f"loaded {n}")  # throttled: at most one line per 10 s
            step.result = "loaded everything"
    assert FakeHalo.started == []
    assert out.getvalue() == "... loading\n... loaded 0\nok  loaded everything\n"


def test_a_failed_step_reports_its_error_once():
    with tempfile.TemporaryDirectory() as tmp, console(io.StringIO()) as (_, err):
        assert main(["prepare", "--data-dir", tmp]) == 1  # no SAML-D.csv to prepare
    assert err.getvalue().count("SAML-D.csv not found") == 1
    assert err.getvalue().startswith("FAIL Converting SAML-D.csv")
