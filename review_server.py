"""Flask web server for reviewing extraction results side-by-side with source PDFs."""

import argparse
import json
import sqlite3
from pathlib import Path

from flask import Flask, abort, render_template_string, send_file

from personnel_print import (
    build_output,
    get_company_name,
    load_api_officers,
    load_report_personnel,
)

app = Flask(__name__)

# Set via CLI args at startup
DB_PATH: str = ""
PERSONNEL_CACHE_DIR: str = ""

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Base template
# ---------------------------------------------------------------------------

BASE_CSS = """
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/water.css@2/out/water.min.css">
<style>
  body { max-width: 95%; margin: 0 auto; padding: 1rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #ddd; }
  tr:hover { background: #f5f5f5; }
  a { text-decoration: none; }
  .badge { display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px; font-size: 0.85em; }
  .badge-success { background: #d4edda; color: #155724; }
  .badge-error { background: #f8d7da; color: #721c24; }
  .badge-other { background: #e2e3e5; color: #383d41; }

  /* Side-by-side layout */
  .split { display: flex; gap: 1rem; height: calc(100vh - 8rem); }
  .split-left { flex: 1; min-width: 0; }
  .split-right { flex: 1; min-width: 0; overflow-y: auto; }
  .split-left iframe, .split-left embed { width: 100%; height: 100%; border: 1px solid #ccc; }

  /* Tabs */
  .tabs { display: flex; gap: 0; border-bottom: 2px solid #ccc; margin-bottom: 0.5rem; }
  .tab { padding: 0.5rem 1rem; cursor: pointer; border: 1px solid transparent;
         border-bottom: none; border-radius: 4px 4px 0 0; background: #f0f0f0; }
  .tab.active { background: white; border-color: #ccc; border-bottom: 1px solid white;
                 margin-bottom: -2px; font-weight: bold; }
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  pre { background: #f6f6f6; padding: 1rem; overflow-x: auto; font-size: 0.85em;
        max-height: 70vh; overflow-y: auto; white-space: pre-wrap; word-break: break-word; }
  .personnel-table { font-size: 0.85em; }
  .personnel-table th { position: sticky; top: 0; background: white; }
  .source-merged { color: #155724; }
  .source-report_only { color: #856404; }
  .source-api_only { color: #0c5460; }
  nav { margin-bottom: 1rem; }
  nav a { margin-right: 0.5rem; }
  .meta { color: #666; font-size: 0.9em; margin-bottom: 1rem; }
</style>
"""

TAB_JS = """
<script>
function switchTab(group, name) {
  document.querySelectorAll('.' + group + '-tab').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.' + group + '-content').forEach(el => el.classList.remove('active'));
  document.querySelector('[data-tab="' + name + '"]').classList.add('active');
  document.getElementById(group + '-' + name).classList.add('active');
}
</script>
"""


def status_badge(status):
    if status == "success":
        cls = "badge-success"
    elif status == "error":
        cls = "badge-error"
    else:
        cls = "badge-other"
    return f'<span class="badge {cls}">{status}</span>'


def fmt_bytes(n):
    if n is None:
        return "—"
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def fmt_json(raw):
    if not raw:
        return "<em>No data</em>"
    try:
        obj = json.loads(raw) if isinstance(raw, str) else raw
        return f"<pre>{json.dumps(obj, indent=2, ensure_ascii=False)}</pre>"
    except (json.JSONDecodeError, TypeError):
        return f"<pre>{raw}</pre>"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def run_list():
    db = get_db()
    try:
        rows = db.execute(
            "SELECT run_id, started_at, finished_at, model, input_xlsx_path, "
            "total_companies, processed, succeeded, failed "
            "FROM runs ORDER BY run_id DESC"
        ).fetchall()
    finally:
        db.close()

    table_rows = ""
    for r in rows:
        table_rows += f"""<tr>
            <td><a href="/run/{r['run_id']}">{r['run_id']}</a></td>
            <td>{r['started_at'] or '—'}</td>
            <td>{r['finished_at'] or '—'}</td>
            <td>{r['model'] or '—'}</td>
            <td>{r['total_companies']}</td>
            <td>{r['succeeded']}</td>
            <td>{r['failed']}</td>
            <td>{Path(r['input_xlsx_path']).name if r['input_xlsx_path'] else '—'}</td>
        </tr>"""

    return render_template_string(f"""<!DOCTYPE html>
<html><head><title>Extraction Runs</title>{BASE_CSS}</head>
<body>
<h1>Extraction Runs</h1>
<table>
<thead><tr>
  <th>Run ID</th><th>Started</th><th>Finished</th><th>Model</th>
  <th>Total</th><th>Succeeded</th><th>Failed</th><th>Input</th>
</tr></thead>
<tbody>{table_rows}</tbody>
</table>
</body></html>""")


@app.route("/run/<int:run_id>")
def run_detail(run_id):
    db = get_db()
    try:
        run = db.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not run:
            abort(404, "Run not found")

        reports = db.execute(
            "SELECT id, company_number, company_name, group_name, status, "
            "model_used, pdf_size_bytes "
            "FROM company_reports WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
    finally:
        db.close()

    table_rows = ""
    for r in reports:
        table_rows += f"""<tr>
            <td><a href="/company/{r['id']}">{r['company_number']}</a></td>
            <td>{r['company_name'] or '—'}</td>
            <td>{r['group_name'] or '—'}</td>
            <td>{status_badge(r['status'])}</td>
            <td>{r['model_used'] or '—'}</td>
            <td>{fmt_bytes(r['pdf_size_bytes'])}</td>
        </tr>"""

    return render_template_string(f"""<!DOCTYPE html>
<html><head><title>Run {run_id}</title>{BASE_CSS}</head>
<body>
<nav><a href="/">&larr; All Runs</a></nav>
<h1>Run {run_id}</h1>
<p class="meta">
  Model: {run['model']} &middot;
  Started: {run['started_at']} &middot;
  {run['succeeded']}/{run['total_companies']} succeeded
</p>
<table>
<thead><tr>
  <th>Company #</th><th>Name</th><th>Group</th><th>Status</th><th>Model</th><th>PDF Size</th>
</tr></thead>
<tbody>{table_rows}</tbody>
</table>
</body></html>""")


@app.route("/company/<int:report_id>")
def company_detail(report_id):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM company_reports WHERE id = ?", (report_id,)).fetchone()
        if not row:
            abort(404, "Report not found")
    finally:
        db.close()

    company_number = row["company_number"]
    company_name = row["company_name"] or company_number

    # PDF iframe
    has_pdf = bool(row["pdf_path"] and Path(row["pdf_path"]).exists())
    pdf_html = (
        f'<iframe src="/pdfview/{report_id}" style="width:100%;height:100%;border:none;"></iframe>'
        if has_pdf
        else "<p>No PDF available</p>"
    )

    # --- Tab: Extraction ---
    extraction_html = _render_extraction(row["extraction_json"])

    # --- Tab: Personnel (Merged) ---
    personnel_html = _render_personnel(company_number, Path(DB_PATH))

    # --- Tab: Profile ---
    profile_html = fmt_json(row["profile_json"])

    # --- Tab: Filing History ---
    filing_html = fmt_json(row["filing_history_json"])

    # --- Tab: Warnings ---
    warnings_html = fmt_json(row["warnings_json"])

    # --- Tab: Error ---
    error_html = f"<pre>{row['error_message']}</pre>" if row["error_message"] else "<em>No errors</em>"

    return render_template_string(f"""<!DOCTYPE html>
<html><head><title>{company_name}</title>{BASE_CSS}</head>
<body>
<nav>
  <a href="/">&larr; All Runs</a>
  <a href="/run/{row['run_id']}">&larr; Run {row['run_id']}</a>
</nav>
<h2>{company_name} <small>({company_number})</small></h2>
<p class="meta">
  Status: {status_badge(row['status'])} &middot;
  Model: {row['model_used'] or '—'} &middot;
  PDF: {fmt_bytes(row['pdf_size_bytes'])}
</p>

<div class="split">
  <div class="split-left">
    {pdf_html}
  </div>
  <div class="split-right">
    <div class="tabs">
      <div class="tab main-tab active" data-tab="extraction" onclick="switchTab('main','extraction')">Extraction</div>
      <div class="tab main-tab" data-tab="personnel" onclick="switchTab('main','personnel')">Personnel (Merged)</div>
      <div class="tab main-tab" data-tab="profile" onclick="switchTab('main','profile')">Profile</div>
      <div class="tab main-tab" data-tab="filing" onclick="switchTab('main','filing')">Filing History</div>
      <div class="tab main-tab" data-tab="warnings" onclick="switchTab('main','warnings')">Warnings</div>
      <div class="tab main-tab" data-tab="error" onclick="switchTab('main','error')">Error</div>
    </div>
    <div id="main-extraction" class="main-content tab-content active">{extraction_html}</div>
    <div id="main-personnel" class="main-content tab-content">{personnel_html}</div>
    <div id="main-profile" class="main-content tab-content">{profile_html}</div>
    <div id="main-filing" class="main-content tab-content">{filing_html}</div>
    <div id="main-warnings" class="main-content tab-content">{warnings_html}</div>
    <div id="main-error" class="main-content tab-content">{error_html}</div>
  </div>
</div>
{TAB_JS}
</body></html>""")


@app.route("/pdfview/<int:report_id>")
def pdf_viewer(report_id):
    """Render a minimal PDF.js viewer without sidebar."""
    return render_template_string("""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<style>
  * { margin: 0; padding: 0; }
  html, body { height: 100%; overflow: hidden; }
  #pdf-container { width: 100%; height: 100%; overflow-y: auto; background: #525659; }
  canvas { display: block; margin: 4px auto; box-shadow: 0 2px 8px rgba(0,0,0,0.3); }
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/4.2.67/pdf.min.mjs" type="module"></script>
</head>
<body>
<div id="pdf-container"></div>
<script type="module">
  const pdfjsLib = await import("https://cdnjs.cloudflare.com/ajax/libs/pdf.js/4.2.67/pdf.min.mjs");
  pdfjsLib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/4.2.67/pdf.worker.min.mjs";

  const pdf = await pdfjsLib.getDocument("/pdf/{{ report_id }}").promise;
  const container = document.getElementById("pdf-container");

  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const scale = (container.clientWidth - 16) / page.getViewport({scale: 1}).width;
    const viewport = page.getViewport({scale});
    const canvas = document.createElement("canvas");
    canvas.width = viewport.width;
    canvas.height = viewport.height;
    container.appendChild(canvas);
    await page.render({canvasContext: canvas.getContext("2d"), viewport}).promise;
  }
</script>
</body></html>""", report_id=report_id)


@app.route("/pdf/<int:report_id>")
def serve_pdf(report_id):
    db = get_db()
    try:
        row = db.execute("SELECT pdf_path FROM company_reports WHERE id = ?", (report_id,)).fetchone()
    finally:
        db.close()
    if not row or not row["pdf_path"]:
        abort(404, "No PDF path")
    pdf = Path(row["pdf_path"])
    if not pdf.exists():
        abort(404, f"PDF not found on disk: {pdf}")
    return send_file(str(pdf.resolve()), mimetype="application/pdf")


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _render_extraction(extraction_json_raw):
    if not extraction_json_raw:
        return "<em>No extraction data</em>"
    try:
        data = json.loads(extraction_json_raw)
    except (json.JSONDecodeError, TypeError):
        return f"<pre>{extraction_json_raw}</pre>"

    parts = []

    # Personnel table
    personnel = data.get("personnel_details")
    if personnel and isinstance(personnel, list):
        parts.append("<h3>Personnel Details</h3>")
        parts.append('<table class="personnel-table"><thead><tr>'
                     "<th>Name</th><th>First (Extracted)</th><th>First (Enriched)</th>"
                     "<th>Job Title</th><th>Standardised Title</th>"
                     "<th>Organisation</th><th>Email</th></tr></thead><tbody>")
        for p in personnel:
            first = p.get("first_name", "")
            last = p.get("last_name", "")
            extracted = p.get("first_name_extracted", "") or ""
            enriched = p.get("first_name_enriched", "") or ""
            title = p.get("job_title", "")
            std = p.get("standardised_job_title", "") or ""
            org = p.get("organisation_name", "") or ""
            email = p.get("email", "") or ""
            parts.append(f"<tr><td>{first} {last}</td>"
                         f"<td>{extracted}</td><td>{enriched}</td>"
                         f"<td>{title}</td><td>{std}</td>"
                         f"<td>{org}</td><td>{email}</td></tr>")
        parts.append("</tbody></table>")

    # Metadata
    meta = data.get("metadata")
    if meta:
        parts.append("<h3>Metadata</h3>")
        parts.append(fmt_json(meta))

    # Governance
    gov = data.get("governance")
    if gov:
        parts.append("<h3>Governance</h3>")
        trustees = gov.get("trustees")
        if trustees and isinstance(trustees, list):
            parts.append('<table class="personnel-table"><thead><tr>'
                         "<th>Name</th><th>Attended</th><th>Possible</th>"
                         "</tr></thead><tbody>")
            for t in trustees:
                parts.append(f"<tr><td>{t.get('name', '—')}</td>"
                             f"<td>{t.get('meetings_attended', '—')}</td>"
                             f"<td>{t.get('meetings_possible', '—')}</td></tr>")
            parts.append("</tbody></table>")
        else:
            parts.append(fmt_json(gov))

    # Staffing
    staffing = data.get("staffing_data")
    if staffing:
        parts.append("<h3>Staffing Data</h3>")
        parts.append(fmt_json(staffing))

    # Balance sheet
    bs = data.get("balance_sheet") or data.get("detailed_balance_sheet")
    if bs:
        parts.append("<h3>Balance Sheet</h3>")
        parts.append(fmt_json(bs))

    # SOFA
    sofa = data.get("statement_of_financial_activities")
    if sofa:
        parts.append("<h3>Statement of Financial Activities</h3>")
        parts.append(fmt_json(sofa))

    # Full JSON fallback
    parts.append("<h3>Full JSON</h3>")
    parts.append(fmt_json(data))

    return "\n".join(parts)


def _render_personnel(company_number, db_path):
    try:
        api_officers = load_api_officers(Path(PERSONNEL_CACHE_DIR), company_number)
        report_personnel = load_report_personnel(db_path, company_number)
        name = get_company_name(db_path, company_number)
        output = build_output(company_number, name, api_officers, report_personnel, include_unmatched=True)
    except Exception as e:
        return f"<em>Error loading personnel: {e}</em>"

    parts = []

    # Summary
    summary = output.get("summary", {})
    sources = output.get("sources", {})
    parts.append(f"""<p class="meta">
        API officers: {sources.get('api_cache', {}).get('officers_count', 0)} &middot;
        Report personnel: {sources.get('report_extraction', {}).get('personnel_count', 0)} &middot;
        Matched: {summary.get('matched', 0)} &middot;
        Report only: {summary.get('unmatched_report', 0)} &middot;
        API only: {summary.get('unmatched_api', 0)}
    </p>""")

    # Personnel table
    people = output.get("personnel", [])
    if people:
        parts.append('<table class="personnel-table"><thead><tr>'
                     "<th>Name</th><th>Job Title</th><th>Std Title</th>"
                     "<th>Role (API)</th><th>Appointed</th><th>Source</th>"
                     "</tr></thead><tbody>")
        for p in people:
            name_str = f"{p.get('first_name', '')} {p.get('middle_names', '')} {p.get('last_name', '')}".strip()
            source = p.get("source", "")
            source_cls = f"source-{source}"
            parts.append(
                f"<tr><td>{name_str}</td>"
                f"<td>{p.get('job_title', '') or '—'}</td>"
                f"<td>{p.get('standardised_job_title', '') or '—'}</td>"
                f"<td>{p.get('role', '') or '—'}</td>"
                f"<td>{p.get('appointed_on', '') or '—'}</td>"
                f'<td class="{source_cls}">{source}</td></tr>'
            )
        parts.append("</tbody></table>")

    # By standardised title
    by_title = summary.get("by_standardised_title", {})
    if by_title:
        parts.append("<h4>By Standardised Title</h4><ul>")
        for title, count in sorted(by_title.items()):
            parts.append(f"<li>{title}: {count}</li>")
        parts.append("</ul>")

    if not people:
        parts.append("<em>No personnel data available</em>")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    global DB_PATH, PERSONNEL_CACHE_DIR

    parser = argparse.ArgumentParser(description="Review extraction results")
    parser.add_argument(
        "--db-path",
        default="output/companies_extraction/companies_house_extractions.db",
        help="Path to SQLite database",
    )
    parser.add_argument("--port", type=int, default=5000, help="Port to serve on")
    parser.add_argument(
        "--personnel-cache-dir",
        default="output/personnel_cache",
        help="Directory containing cached personnel data",
    )
    args = parser.parse_args()

    DB_PATH = args.db_path
    PERSONNEL_CACHE_DIR = args.personnel_cache_dir

    if not Path(DB_PATH).exists():
        print(f"Error: database not found at {DB_PATH}")
        return

    print(f"Serving on http://localhost:{args.port}")
    print(f"Database: {DB_PATH}")
    print(f"Personnel cache: {PERSONNEL_CACHE_DIR}")
    app.run(host="0.0.0.0", port=args.port, debug=True)


if __name__ == "__main__":
    main()
