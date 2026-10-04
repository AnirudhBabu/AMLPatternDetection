import json
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from aml.cli import main
from aml.dashboard import Question, export_results
from aml.site import MAX_TABLE_ROWS, Chart, build_site, render_page

INJECTION = "</script><script>alert(1)</script>"
WHEN = datetime(2026, 10, 4, tzinfo=timezone.utc)


def results(tmp: str) -> Path:
    data = Path(tmp) / "data"
    data.mkdir()
    pd.DataFrame({"Cycle_ID": [1, 1, 1], "Hop_Number": [1, 2, 3],
                  "Amount": [100.0, 90.0, 85.5], "Tx_ID": [1, 2, 3]}
                 ).to_csv(data / "detected_cycles.csv", index=False)
    pd.DataFrame({"Episode_ID": [1, 1, 2], "Receiver_account": [7, 7, 8],
                  "Duration_Days": [3.0, 3.0, 9.5], "Amount": [9000.0, 9500.0, 100.0],
                  "Sender_count": [12, 12, 3], "Tx_ID": [4, 5, 6]}
                 ).to_csv(data / "smurfing_suspects.csv", index=False)
    typologies = [INJECTION] + [f"Typology_{i:03d}" for i in range(MAX_TABLE_ROWS + 5)]
    pd.DataFrame({"typology": typologies, "tx": 1, "accounts": 1}
                 ).to_csv(data / "evaluation_by_typology.csv", index=False)
    export_results(data)
    return data


def specs(page: str) -> dict:
    raw = re.search(r'<script id="specs" type="application/json">(.*?)</script>', page, re.S)
    return json.loads(raw.group(1))


def test_renders_the_dashboard_questions_into_one_page():
    with tempfile.TemporaryDirectory() as tmp:
        data = results(tmp)
        path, charts, skipped = build_site(data / "aml_results.sqlite", Path(tmp) / "docs",
                                           repo_url="https://github.com/me/aml", now=WHEN)
        page = path.read_text()
        assert path.name == "index.html" and charts == 3
        assert "Cycling: average amount at each hop" in page and "built 2026-10-04" in page
        assert '<a href="https://github.com/me/aml">' in page
        assert "https://cdn.plot.ly/plotly-2.35.2.min.js" in page
        figures = specs(page)
        funnel = figures["chart-0"]["data"][0]
        assert (funnel["type"], funnel["x"], funnel["y"]) == (
            "funnel", [100.0, 90.0, 85.5], ["hop 1", "hop 2", "hop 3"])
        scatter = figures["chart-1"]["data"][0]
        assert sorted(zip(scatter["x"], scatter["y"])) == [(3.0, 9250.0), (9.5, 100.0)]
        assert len(skipped) == 3 and "No results yet for:" in page


def test_tables_are_escaped_and_capped():
    with tempfile.TemporaryDirectory() as tmp:
        data = results(tmp)
        path, _, _ = build_site(data / "aml_results.sqlite", Path(tmp) / "docs", now=WHEN)
        page = path.read_text()
        assert "<script>alert(1)" not in page
        assert "&lt;/script&gt;&lt;script&gt;alert(1)&lt;/script&gt;" in page
        assert f"Showing the first {MAX_TABLE_ROWS} of {MAX_TABLE_ROWS + 6} rows." in page


def test_embedded_chart_data_cannot_close_its_script_tag():
    question = Question("labels", "t", "", "bar",
                        {"graph.dimensions": ["label"], "graph.metrics": ["n"]})
    page = render_page([Chart(question, ["label", "n"], [(INJECTION, 1)])], [], built="today")
    embedded = re.search(r'type="application/json">(.*?)</script>', page, re.S).group(1)
    assert "</" not in embedded and "<" not in embedded
    assert specs(page)["chart-0"]["data"][0]["x"] == [INJECTION]


def test_needs_the_exported_results():
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(FileNotFoundError, match="aml export"):
            build_site(Path(tmp) / "missing.sqlite", Path(tmp) / "docs")


def test_cli_writes_the_page():
    with tempfile.TemporaryDirectory() as tmp:
        data = results(tmp)
        out = Path(tmp) / "site"
        args = ["site", "--data-dir", str(data), "--out", str(out),
                "--repo-url", "https://github.com/me/aml"]
        assert main(args) == 0
        assert "github.com/me/aml" in (out / "index.html").read_text()
