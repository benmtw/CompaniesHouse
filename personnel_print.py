"""CLI tool to print merged personnel data from report extractions and Companies House API.

Combines personnel data from two sources:
1. Full reports (PDFs) - LLM-extracted data stored in SQLite; often has initials for first names
2. Companies House API - Cached JSON files with full names

Merges the specificity of report job titles with the full names from the API,
and adds role flags (isCEO, isCFO, isCOO).
"""

import argparse
import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


# ── Role Detection Patterns ─────────────────────────────────────────────────────

CEO_PATTERNS = ["chief executive", "ceo", "chief exec", "managing director"]
CFO_PATTERNS = [
    "chief finance",
    "cfo",
    "finance director",
    "financial director",
    "director of finance",
    "chief financial",
    "finance and operations officer",
]
COO_PATTERNS = [
    "chief operating",
    "coo",
    "chief operations",
    "operations director",
    "director of operations",
]


# ── Small utilities ────────────────────────────────────────────────────────────


def _load_dotenv_file(env_path: Path) -> None:
    """Load environment variables from a .env file."""
    if not env_path.exists() or not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def normalize_company_number(raw_value: str) -> str | None:
    """Normalize a company number to standard 8-character format."""
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    alnum = "".join(ch for ch in text.upper() if ch.isalnum())
    if not alnum:
        return None
    if alnum.isdigit():
        if len(alnum) > 8:
            return None
        return alnum.zfill(8)
    if len(alnum) > 8:
        return None
    return alnum


# ── Role Detection ─────────────────────────────────────────────────────────────


def is_ceo(job_title: str) -> bool:
    """Check if a job title indicates a CEO role."""
    if not job_title:
        return False
    title_lower = job_title.lower()
    return any(pattern in title_lower for pattern in CEO_PATTERNS)


def is_cfo(job_title: str) -> bool:
    """Check if a job title indicates a CFO role."""
    if not job_title:
        return False
    title_lower = job_title.lower()
    return any(pattern in title_lower for pattern in CFO_PATTERNS)


def is_coo(job_title: str) -> bool:
    """Check if a job title indicates a COO role."""
    if not job_title:
        return False
    title_lower = job_title.lower()
    return any(pattern in title_lower for pattern in COO_PATTERNS)


# ── Name Matching ──────────────────────────────────────────────────────────────


def generate_match_key(first_name: str, last_name: str) -> str:
    """Generate a key for matching: "{first_initial}_{last_name_lower}"."""
    first_initial = first_name.strip()[0].lower() if first_name.strip() else ""
    last_normalized = last_name.strip().lower()
    return f"{first_initial}_{last_normalized}"


# ── Data Loading ───────────────────────────────────────────────────────────────


def load_api_officers(cache_dir: Path, company_number: str) -> list[dict[str, Any]] | None:
    """Load officers from the personnel cache JSON file."""
    cache_file = cache_dir / f"{company_number}.json"
    if not cache_file.is_file():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    officers = data.get("officers")
    if not isinstance(officers, list):
        return None
    return officers


def load_report_personnel(db_path: Path, company_number: str) -> list[dict[str, Any]] | None:
    """Load personnel from the most recent successful extraction in SQLite."""
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            """
            SELECT extraction_json
            FROM company_reports
            WHERE company_number = ?
              AND status = 'success'
              AND extraction_json IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (company_number,),
        )
        row = cursor.fetchone()
        conn.close()
    except sqlite3.Error:
        return None

    if not row or not row[0]:
        return None

    try:
        extraction = json.loads(row[0])
    except json.JSONDecodeError:
        return None

    personnel_details = extraction.get("personnel_details")
    if not isinstance(personnel_details, list):
        return None

    return personnel_details


def get_company_name(db_path: Path, company_number: str) -> str | None:
    """Get the company name from the most recent record in SQLite."""
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            """
            SELECT company_name
            FROM company_reports
            WHERE company_number = ?
              AND company_name IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (company_number,),
        )
        row = cursor.fetchone()
        conn.close()
    except sqlite3.Error:
        return None

    if not row or not row[0]:
        return None
    return row[0]


# ── Matching and Merging ───────────────────────────────────────────────────────


def match_personnel(
    report_list: list[dict[str, Any]],
    api_list: list[dict[str, Any]],
) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Match report personnel with API officers.

    Returns:
        - matched: list of (report_person, api_officer) tuples
        - unmatched_report: report personnel that couldn't be matched
        - unmatched_api: API officers that couldn't be matched
    """
    # Build lookup for API officers by match key
    api_by_key: dict[str, list[dict[str, Any]]] = {}
    for officer in api_list:
        key = generate_match_key(
            officer.get("first_name", ""),
            officer.get("last_name", ""),
        )
        if key not in api_by_key:
            api_by_key[key] = []
        api_by_key[key].append(officer)

    matched: list[tuple[dict[str, Any], dict[str, Any]]] = []
    unmatched_report: list[dict[str, Any]] = []
    used_api_indices: set[int] = set()

    for report_person in report_list:
        key = generate_match_key(
            report_person.get("first_name", ""),
            report_person.get("last_name", ""),
        )
        candidates = api_by_key.get(key, [])
        # Find first unused candidate
        matched_officer = None
        for idx, officer in enumerate(candidates):
            # Use id() to track which officer instances we've used
            officer_id = id(officer)
            if officer_id not in used_api_indices:
                matched_officer = officer
                used_api_indices.add(officer_id)
                break

        if matched_officer:
            matched.append((report_person, matched_officer))
        else:
            unmatched_report.append(report_person)

    # Find unmatched API officers
    unmatched_api: list[dict[str, Any]] = []
    for officer in api_list:
        if id(officer) not in used_api_indices:
            unmatched_api.append(officer)

    return matched, unmatched_report, unmatched_api


def merge_personnel_record(report_person: dict[str, Any], api_officer: dict[str, Any]) -> dict[str, Any]:
    """Merge a report person with an API officer record.

    Uses report's job_title (more specific) and API's full name fields.
    """
    job_title = report_person.get("job_title", "")

    return {
        "first_name": api_officer.get("first_name", ""),
        "middle_names": api_officer.get("middle_names", ""),
        "last_name": api_officer.get("last_name", ""),
        "job_title": job_title,
        "role": api_officer.get("role", ""),
        "appointed_on": api_officer.get("appointed_on", ""),
        "date_of_birth": api_officer.get("date_of_birth"),
        "correspondence_address": api_officer.get("correspondence_address"),
        "isCEO": is_ceo(job_title),
        "isCFO": is_cfo(job_title),
        "isCOO": is_coo(job_title),
        "source": "merged",
    }


def build_output(
    company_number: str,
    company_name: str | None,
    api_officers: list[dict[str, Any]] | None,
    report_personnel: list[dict[str, Any]] | None,
    include_unmatched: bool,
) -> dict[str, Any]:
    """Build the complete output structure."""
    sources = {
        "api_cache": {
            "available": api_officers is not None,
            "officers_count": len(api_officers) if api_officers else 0,
        },
        "report_extraction": {
            "available": report_personnel is not None,
            "personnel_count": len(report_personnel) if report_personnel else 0,
        },
    }

    personnel: list[dict[str, Any]] = []
    matched_count = 0
    unmatched_report_count = 0
    unmatched_api_count = 0
    ceo_count = 0
    cfo_count = 0
    coo_count = 0

    if api_officers and report_personnel:
        # Both sources available - merge
        matched, unmatched_report, unmatched_api = match_personnel(report_personnel, api_officers)

        for report_person, api_officer in matched:
            merged = merge_personnel_record(report_person, api_officer)
            personnel.append(merged)
            matched_count += 1
            if merged["isCEO"]:
                ceo_count += 1
            if merged["isCFO"]:
                cfo_count += 1
            if merged["isCOO"]:
                coo_count += 1

        if include_unmatched:
            for report_person in unmatched_report:
                job_title = report_person.get("job_title", "")
                record = {
                    "first_name": report_person.get("first_name", ""),
                    "middle_names": "",
                    "last_name": report_person.get("last_name", ""),
                    "job_title": job_title,
                    "role": "",
                    "appointed_on": "",
                    "date_of_birth": None,
                    "correspondence_address": None,
                    "isCEO": is_ceo(job_title),
                    "isCFO": is_cfo(job_title),
                    "isCOO": is_coo(job_title),
                    "source": "report_only",
                }
                personnel.append(record)
                if record["isCEO"]:
                    ceo_count += 1
                if record["isCFO"]:
                    cfo_count += 1
                if record["isCOO"]:
                    coo_count += 1

            for api_officer in unmatched_api:
                job_title = api_officer.get("role", "")  # API uses 'role' as generic title
                record = {
                    "first_name": api_officer.get("first_name", ""),
                    "middle_names": api_officer.get("middle_names", ""),
                    "last_name": api_officer.get("last_name", ""),
                    "job_title": job_title,
                    "role": api_officer.get("role", ""),
                    "appointed_on": api_officer.get("appointed_on", ""),
                    "date_of_birth": api_officer.get("date_of_birth"),
                    "correspondence_address": api_officer.get("correspondence_address"),
                    "isCEO": is_ceo(job_title),
                    "isCFO": is_cfo(job_title),
                    "isCOO": is_coo(job_title),
                    "source": "api_only",
                }
                personnel.append(record)
                if record["isCEO"]:
                    ceo_count += 1
                if record["isCFO"]:
                    cfo_count += 1
                if record["isCOO"]:
                    coo_count += 1

        unmatched_report_count = len(unmatched_report)
        unmatched_api_count = len(unmatched_api)

    elif report_personnel:
        # Only report data available
        for person in report_personnel:
            job_title = person.get("job_title", "")
            record = {
                "first_name": person.get("first_name", ""),
                "middle_names": "",
                "last_name": person.get("last_name", ""),
                "job_title": job_title,
                "role": "",
                "appointed_on": "",
                "date_of_birth": None,
                "correspondence_address": None,
                "isCEO": is_ceo(job_title),
                "isCFO": is_cfo(job_title),
                "isCOO": is_coo(job_title),
                "source": "report_only",
            }
            personnel.append(record)
            if record["isCEO"]:
                ceo_count += 1
            if record["isCFO"]:
                cfo_count += 1
            if record["isCOO"]:
                coo_count += 1

    elif api_officers:
        # Only API data available
        for officer in api_officers:
            job_title = officer.get("role", "")
            record = {
                "first_name": officer.get("first_name", ""),
                "middle_names": officer.get("middle_names", ""),
                "last_name": officer.get("last_name", ""),
                "job_title": job_title,
                "role": officer.get("role", ""),
                "appointed_on": officer.get("appointed_on", ""),
                "date_of_birth": officer.get("date_of_birth"),
                "correspondence_address": officer.get("correspondence_address"),
                "isCEO": is_ceo(job_title),
                "isCFO": is_cfo(job_title),
                "isCOO": is_coo(job_title),
                "source": "api_only",
            }
            personnel.append(record)
            if record["isCEO"]:
                ceo_count += 1
            if record["isCFO"]:
                cfo_count += 1
            if record["isCOO"]:
                coo_count += 1

    return {
        "company_number": company_number,
        "company_name": company_name,
        "sources": sources,
        "personnel": personnel,
        "summary": {
            "total_personnel": len(personnel),
            "matched": matched_count,
            "unmatched_report": unmatched_report_count,
            "unmatched_api": unmatched_api_count,
            "ceos": ceo_count,
            "cfos": cfo_count,
            "coos": coo_count,
        },
    }


# ── Output Formatting ──────────────────────────────────────────────────────────


def format_json(result: dict[str, Any]) -> str:
    """Format the result as pretty-printed JSON."""
    return json.dumps(result, indent=2, ensure_ascii=True)


def format_pretty(result: dict[str, Any]) -> str:
    """Format the result as human-readable text."""
    lines: list[str] = []
    lines.append(f"Company: {result.get('company_name', 'Unknown')} ({result.get('company_number', 'N/A')})")
    lines.append("")

    sources = result.get("sources", {})
    api_cache = sources.get("api_cache", {})
    report_extraction = sources.get("report_extraction", {})

    lines.append("Sources:")
    lines.append(f"  API Cache: {'Available' if api_cache.get('available') else 'Not available'} ({api_cache.get('officers_count', 0)} officers)")
    lines.append(f"  Report Extraction: {'Available' if report_extraction.get('available') else 'Not available'} ({report_extraction.get('personnel_count', 0)} personnel)")
    lines.append("")

    summary = result.get("summary", {})
    lines.append("Summary:")
    lines.append(f"  Total Personnel: {summary.get('total_personnel', 0)}")
    lines.append(f"  Matched: {summary.get('matched', 0)}")
    lines.append(f"  Unmatched from Report: {summary.get('unmatched_report', 0)}")
    lines.append(f"  Unmatched from API: {summary.get('unmatched_api', 0)}")
    lines.append(f"  CEOs: {summary.get('ceos', 0)}")
    lines.append(f"  CFOs: {summary.get('cfos', 0)}")
    lines.append(f"  COOs: {summary.get('coos', 0)}")
    lines.append("")

    personnel = result.get("personnel", [])
    if personnel:
        lines.append("Personnel:")
        for person in personnel:
            name_parts = []
            if person.get("first_name"):
                name_parts.append(person["first_name"])
            if person.get("middle_names"):
                name_parts.append(person["middle_names"])
            if person.get("last_name"):
                name_parts.append(person["last_name"])
            full_name = " ".join(name_parts) or "Unknown"

            job_title = person.get("job_title", "No title")
            role = person.get("role", "")
            source = person.get("source", "")

            flags = []
            if person.get("isCEO"):
                flags.append("CEO")
            if person.get("isCFO"):
                flags.append("CFO")
            if person.get("isCOO"):
                flags.append("COO")
            flag_str = f" [{', '.join(flags)}]" if flags else ""

            lines.append(f"  - {full_name}")
            lines.append(f"    Title: {job_title}{flag_str}")
            if role:
                lines.append(f"    Role: {role}")
            if person.get("appointed_on"):
                lines.append(f"    Appointed: {person['appointed_on']}")
            lines.append(f"    Source: {source}")
    else:
        lines.append("No personnel found.")

    return "\n".join(lines)


# ── CLI Interface ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        description="Print merged personnel data from report extractions and Companies House API cache.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python personnel_print.py 07318714
  python personnel_print.py 07318714 --format pretty
  python personnel_print.py 07318714 --include-unmatched
  python personnel_print.py 07318714 --db-path custom.db --cache-dir custom_cache
""",
    )

    parser.add_argument(
        "company_number",
        help="8-digit company number (e.g., '07318714')",
    )

    parser.add_argument(
        "--db-path",
        default="output/companies_extraction/companies_house_extractions.db",
        help="SQLite database path (default: %(default)s)",
    )

    parser.add_argument(
        "--cache-dir",
        default="output/personnel_cache",
        help="Personnel cache directory (default: %(default)s)",
    )

    parser.add_argument(
        "--format",
        choices=["json", "pretty"],
        default="json",
        help="Output format (default: %(default)s)",
    )

    parser.add_argument(
        "--include-unmatched",
        action="store_true",
        help="Include personnel that couldn't be matched between sources",
    )

    return parser


def main() -> int:
    """Main entry point."""
    _load_dotenv_file(Path(".env"))
    args = build_parser().parse_args()

    # Normalize company number
    company_number = normalize_company_number(args.company_number)
    if not company_number:
        print(f"Error: Invalid company number '{args.company_number}'", flush=True)
        return 1

    db_path = Path(args.db_path)
    cache_dir = Path(args.cache_dir)

    # Load data from both sources
    api_officers = load_api_officers(cache_dir, company_number)
    report_personnel = load_report_personnel(db_path, company_number)
    company_name = get_company_name(db_path, company_number)

    # Check if we have any data
    if api_officers is None and report_personnel is None:
        import sys
        print(f"Error: No data found for company {company_number}", file=sys.stderr)
        print(f"  - No cache file at: {cache_dir / f'{company_number}.json'}", file=sys.stderr)
        print(f"  - No extraction in database: {db_path}", file=sys.stderr)
        return 1

    # Build output
    result = build_output(
        company_number=company_number,
        company_name=company_name,
        api_officers=api_officers,
        report_personnel=report_personnel,
        include_unmatched=args.include_unmatched,
    )

    # Format and print
    if args.format == "json":
        print(format_json(result))
    else:
        print(format_pretty(result))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
