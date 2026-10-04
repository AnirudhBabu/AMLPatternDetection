"""A public, static copy of the dashboard, for GitHub Pages.

Metabase runs on your machine, so nobody browsing the repo can reach it. ``aml site`` runs the
dashboard's own questions (the SQL in aml.dashboard.QUESTIONS) against the same exported
results and renders them into one HTML page, docs/index.html. GitHub Pages serves that page to
anyone, with no server, login or database behind it. It holds only aggregated results from
SAML-D, a public synthetic dataset.
"""

from __future__ import annotations

import html
import json
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from aml.dashboard import DASHBOARD_NAME, QUESTIONS, Question

PLOTLY_JS = "https://cdn.plot.ly/plotly-2.35.2.min.js"  # the CDN has no "latest" for v2+
MAX_TABLE_ROWS = 100
MAX_POINTS = 5_000


@dataclass
class Chart:
    question: Question
    columns: list[str]
    rows: list[tuple]

    def column(self, name: str, limit: int | None = None) -> list:
        i = self.columns.index(name)
        return [_json_safe(row[i]) for row in self.rows[:limit]]


def _json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None  # NaN/inf aren't valid JSON
    return value


def run_questions(results: Path) -> tuple[list[Chart], list[str]]:
    """Each dashboard question's rows (read-only), plus the names of questions without data."""
    if not results.is_file():
        raise FileNotFoundError(f"{results} not found - run `aml export` first")
    con = sqlite3.connect(f"{results.resolve().as_uri()}?mode=ro", uri=True)
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        charts, skipped = [], []
        for question in QUESTIONS:
            if question.table not in tables:
                skipped.append(question.name)
                continue
            cursor = con.execute(question.sql)
            charts.append(Chart(question, [d[0] for d in cursor.description], cursor.fetchall()))
        return charts, skipped
    finally:
        con.close()


def plot_spec(chart: Chart) -> dict | None:
    """A Plotly figure for chart questions; None for tables, which are rendered as HTML."""
    q = chart.question
    if q.display == "funnel":
        dim, metric = q.settings["funnel.dimension"], q.settings["funnel.metric"]
        return {
            "data": [{"type": "funnel", "x": chart.column(metric),
                      "y": [f"{dim} {v}" for v in chart.column(dim)]}],
            "layout": {"yaxis": {"title": {"text": dim}}},
        }
    if q.display in ("scatter", "bar"):
        x, y = q.settings["graph.dimensions"][0], q.settings["graph.metrics"][0]
        trace = {"type": "bar"} if q.display == "bar" else {"type": "scatter", "mode": "markers"}
        return {
            "data": [{**trace, "x": chart.column(x, MAX_POINTS), "y": chart.column(y, MAX_POINTS)}],
            "layout": {"xaxis": {"title": {"text": x}}, "yaxis": {"title": {"text": y}}},
        }
    return None


def _cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return str(value)


def table_html(chart: Chart) -> str:
    shown = chart.rows[:MAX_TABLE_ROWS]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in chart.columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(_cell(v))}</td>" for v in row) + "</tr>"
        for row in shown
    )
    out = (f'<div class="table"><table><thead><tr>{head}</tr></thead>'
           f'<tbody>{body}</tbody></table></div>')
    if len(chart.rows) > len(shown):
        out += f'<p class="note">Showing the first {len(shown):,} of {len(chart.rows):,} rows.</p>'
    return out


def _script_json(value) -> str:
    """JSON that can't end its <script> element early, whatever strings it contains."""
    text = json.dumps(value, separators=(",", ":"), allow_nan=False)
    return text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<script src="__PLOTLY__" charset="utf-8"></script>
<style>
  body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; margin: 0;
         color: #1f2933; background: #f5f7fa; }
  header, footer { padding: 24px 32px 8px; }
  header p, .note { color: #52606d; font-size: 14px; }
  main { display: grid; gap: 20px; padding: 16px 32px 32px;
         grid-template-columns: repeat(auto-fit, minmax(min(460px, 100%), 1fr)); }
  section { background: #fff; border-radius: 8px; padding: 16px; min-width: 0;
            box-shadow: 0 1px 3px rgba(0, 0, 0, .08); }
  h2 { font-size: 16px; margin: 0 0 12px; }
  .chart { height: 360px; }
  .table { overflow: auto; max-height: 360px; }
  table { border-collapse: collapse; font-size: 13px; width: 100%; }
  th, td { padding: 4px 8px; border-bottom: 1px solid #e4e7eb; text-align: right;
           white-space: nowrap; }
  th { position: sticky; top: 0; background: #fff; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <p>A static snapshot of the Metabase dashboard__SOURCE__, built __BUILT__ from SAML-D,
     a public synthetic transaction dataset.</p>
</header>
<main>
__SECTIONS__
</main>
<footer>__MISSING__</footer>
<script id="specs" type="application/json">__SPECS__</script>
<script>
  const specs = JSON.parse(document.getElementById("specs").textContent);
  const base = { margin: { t: 10, r: 10, b: 50, l: 70 } };
  for (const [id, spec] of Object.entries(specs)) {
    Plotly.newPlot(id, spec.data, Object.assign({}, base, spec.layout),
                   { responsive: true, displaylogo: false });
  }
</script>
</body>
</html>
"""


def render_page(
    charts: list[Chart], skipped: list[str], *, built: str, repo_url: str | None = None
) -> str:
    specs, sections = {}, []
    for i, chart in enumerate(charts):
        spec = plot_spec(chart)
        if spec is None:
            body = table_html(chart)
        else:
            specs[f"chart-{i}"] = spec
            body = f'<div class="chart" id="chart-{i}"></div>'
            if len(chart.rows) > MAX_POINTS:
                body += (f'<p class="note">Showing the first {MAX_POINTS:,} of '
                         f'{len(chart.rows):,} points.</p>')
        sections.append(f"<section><h2>{html.escape(chart.question.name)}</h2>{body}</section>")
    source = ""
    if repo_url:
        source = f' from <a href="{html.escape(repo_url, quote=True)}">{html.escape(repo_url)}</a>'
    missing = ""
    if skipped:
        missing = f'<p class="note">No results yet for: {html.escape(", ".join(skipped))}.</p>'
    replacements = {
        "__TITLE__": html.escape(DASHBOARD_NAME),
        "__PLOTLY__": PLOTLY_JS,
        "__SOURCE__": source,
        "__BUILT__": html.escape(built),
        "__SECTIONS__": "\n".join(sections),
        "__MISSING__": missing,
        "__SPECS__": _script_json(specs),
    }
    page = PAGE
    for token, value in replacements.items():
        page = page.replace(token, value)
    return page


def build_site(
    results: Path,
    out_dir: Path,
    *,
    repo_url: str | None = None,
    now: datetime | None = None,
) -> tuple[Path, int, list[str]]:
    """Write out_dir/index.html. Returns (file, number of charts, questions without data)."""
    charts, skipped = run_questions(results)
    if not charts:
        raise ValueError(f"{results} has no detector results - run detectors, then `aml export`")
    built = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "index.html"
    page = render_page(charts, skipped, built=built, repo_url=repo_url)
    target.write_text(page, encoding="utf-8")
    return target, len(charts), skipped
