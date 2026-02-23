"""CLI tool to print merged personnel data from report extractions and Companies House API.

Combines personnel data from two sources:
1. Full reports (PDFs) - LLM-extracted data stored in SQLite; often has initials for first names
2. Companies House API - Cached JSON files with full names

Merges the specificity of report job titles with the full names from the API,
and classifies each person into a standardised role title.
"""

import argparse
import json
import os
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from nameparser import HumanName


# ── Standardised Job Titles ──────────────────────────────────────────────────────

STANDARDISED_TITLES: list[str] = [
    "Chief Executive Officer",
    "Chief Financial Officer / Director of Finance",
    "Director of Operations",
    "Director of People / HR",
    "Director of Education",
    "Director of Standards / School Improvement",
    "Director of Safeguarding",
    "Director of SEND / Inclusion",
    "Director of Governance / Company Secretary",
    "Director of IT / Digital",
    "Director of Estates / Property",
    "Director of Data & Assessment",
    "Director of Communications / Marketing",
    "Director of Procurement",
    "Director of Compliance / Risk",
]


# ── Role Detection Patterns ─────────────────────────────────────────────────────
# Maps each standardised title to a list of lowercase patterns to match against
# the raw job_title string.

ROLE_PATTERNS: dict[str, list[str]] = {
    "Chief Executive Officer": [
        "chief executive", "ceo", "chief exec", "managing director",
    ],
    "Chief Financial Officer / Director of Finance": [
        "chief finance", "cfo", "finance director", "financial director",
        "director of finance", "chief financial", "finance and operations officer",
    ],
    "Director of Operations": [
        "chief operating", "coo", "chief operations", "operations director",
        "director of operations",
    ],
    "Director of People / HR": [
        "director of people", "director of hr", "director of human resources",
        "hr director", "head of people", "head of hr", "chief people officer",
    ],
    "Director of Education": [
        "director of education", "chief education officer",
        "head of education", "education director",
    ],
    "Director of Standards / School Improvement": [
        "director of standards", "director of school improvement",
        "head of school improvement", "school improvement director",
    ],
    "Director of Safeguarding": [
        "director of safeguarding", "head of safeguarding",
        "safeguarding director", "safeguarding lead",
    ],
    "Director of SEND / Inclusion": [
        "director of send", "director of inclusion", "head of send",
        "head of inclusion", "send director", "inclusion director",
    ],
    "Director of Governance / Company Secretary": [
        "director of governance", "company secretary", "governance director",
        "head of governance", "clerk to the board",
    ],
    "Director of IT / Digital": [
        "director of it", "director of digital", "chief information officer",
        "cio", "chief technology officer", "cto", "head of it",
        "head of digital", "it director", "digital director",
    ],
    "Director of Estates / Property": [
        "director of estates", "director of property", "estates director",
        "head of estates", "property director", "head of property",
    ],
    "Director of Data & Assessment": [
        "director of data", "director of assessment", "head of data",
        "head of assessment", "data director",
    ],
    "Director of Communications / Marketing": [
        "director of communications", "director of marketing",
        "communications director", "marketing director",
        "head of communications", "head of marketing",
    ],
    "Director of Procurement": [
        "director of procurement", "procurement director",
        "head of procurement", "chief procurement officer",
    ],
    "Director of Compliance / Risk": [
        "director of compliance", "director of risk", "compliance director",
        "risk director", "head of compliance", "head of risk",
    ],
}


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


def match_standardised_title(job_title: str) -> str | None:
    """Match a raw job title to one of the standardised titles using pattern matching.

    Uses word-boundary matching so abbreviations like 'cto' don't match
    inside unrelated words like 'director'.

    Returns the standardised title string or None if no match is found.
    """
    if not job_title:
        return None
    title_lower = job_title.lower()
    for std_title, patterns in ROLE_PATTERNS.items():
        for pattern in patterns:
            if re.search(r"\b" + re.escape(pattern) + r"\b", title_lower):
                return std_title
    return None


def resolve_standardised_title(
    llm_title: str | None, job_title: str
) -> str | None:
    """Return the standardised title: prefer LLM value, fall back to local matching."""
    if llm_title and llm_title in STANDARDISED_TITLES:
        return llm_title
    return match_standardised_title(job_title)


# ── Name Parsing ──────────────────────────────────────────────────────────────


def _parse_name(first_name: str, middle_names: str, last_name: str) -> dict[str, str]:
    """Parse name fields with nameparser and return a dict of parsed components."""
    parts = [p for p in (first_name, middle_names, last_name) if p]
    full_name = " ".join(parts)
    hn = HumanName(full_name)
    return {
        "title": str(hn.title),
        "first": str(hn.first),
        "middle": str(hn.middle),
        "last": str(hn.last),
        "suffix": str(hn.suffix),
        "nickname": str(hn.nickname),
    }


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
    llm_std = report_person.get("standardised_job_title")
    std_title = resolve_standardised_title(llm_std, job_title)

    record: dict[str, Any] = {
        "first_name": api_officer.get("first_name", ""),
        "middle_names": api_officer.get("middle_names", ""),
        "last_name": api_officer.get("last_name", ""),
        "job_title": job_title,
        "standardised_job_title": std_title,
        "role": api_officer.get("role", ""),
        "appointed_on": api_officer.get("appointed_on", ""),
        "date_of_birth": api_officer.get("date_of_birth"),
        "correspondence_address": api_officer.get("correspondence_address"),
        "source": "merged",
    }
    record["parsed"] = _parse_name(
        record["first_name"], record["middle_names"], record["last_name"],
    )
    if report_person.get("organisation_name") is not None:
        record["organisation_name"] = report_person["organisation_name"]
    if report_person.get("organisation_type") is not None:
        record["organisation_type"] = report_person["organisation_type"]
    return record


def _build_report_only_record(person: dict[str, Any]) -> dict[str, Any]:
    """Build a personnel record from report-only data."""
    job_title = person.get("job_title", "")
    llm_std = person.get("standardised_job_title")
    std_title = resolve_standardised_title(llm_std, job_title)
    record: dict[str, Any] = {
        "first_name": person.get("first_name", ""),
        "middle_names": "",
        "last_name": person.get("last_name", ""),
        "job_title": job_title,
        "standardised_job_title": std_title,
        "role": "",
        "appointed_on": "",
        "date_of_birth": None,
        "correspondence_address": None,
        "source": "report_only",
    }
    record["parsed"] = _parse_name(
        record["first_name"], record["middle_names"], record["last_name"],
    )
    if person.get("organisation_name") is not None:
        record["organisation_name"] = person["organisation_name"]
    if person.get("organisation_type") is not None:
        record["organisation_type"] = person["organisation_type"]
    return record


def _build_api_only_record(officer: dict[str, Any]) -> dict[str, Any]:
    """Build a personnel record from API-only data."""
    job_title = officer.get("role", "")
    std_title = match_standardised_title(job_title)
    record: dict[str, Any] = {
        "first_name": officer.get("first_name", ""),
        "middle_names": officer.get("middle_names", ""),
        "last_name": officer.get("last_name", ""),
        "job_title": job_title,
        "standardised_job_title": std_title,
        "role": officer.get("role", ""),
        "appointed_on": officer.get("appointed_on", ""),
        "date_of_birth": officer.get("date_of_birth"),
        "correspondence_address": officer.get("correspondence_address"),
        "source": "api_only",
    }
    record["parsed"] = _parse_name(
        record["first_name"], record["middle_names"], record["last_name"],
    )
    return record


def _count_by_standardised_title(personnel: list[dict[str, Any]]) -> dict[str, int]:
    """Count personnel by standardised_job_title, only including titles with count > 0."""
    counts: dict[str, int] = {}
    for person in personnel:
        std = person.get("standardised_job_title")
        if std:
            counts[std] = counts.get(std, 0) + 1
    return counts


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

    if api_officers and report_personnel:
        matched, unmatched_report, unmatched_api = match_personnel(report_personnel, api_officers)

        for report_person, api_officer in matched:
            merged = merge_personnel_record(report_person, api_officer)
            personnel.append(merged)
            matched_count += 1

        if include_unmatched:
            for report_person in unmatched_report:
                personnel.append(_build_report_only_record(report_person))
            for api_officer in unmatched_api:
                personnel.append(_build_api_only_record(api_officer))

        unmatched_report_count = len(unmatched_report)
        unmatched_api_count = len(unmatched_api)

    elif report_personnel:
        for person in report_personnel:
            personnel.append(_build_report_only_record(person))

    elif api_officers:
        for officer in api_officers:
            personnel.append(_build_api_only_record(officer))

    title_counts = _count_by_standardised_title(personnel)

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
            "by_standardised_title": title_counts,
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
    title_counts = summary.get("by_standardised_title", {})
    if title_counts:
        lines.append("  By Standardised Title:")
        for title, count in sorted(title_counts.items()):
            lines.append(f"    {title}: {count}")
    lines.append("")

    personnel = result.get("personnel", [])
    if personnel:
        lines.append("Personnel:")
        for person in personnel:
            first = person.get("first_name", "")
            last = person.get("last_name", "")
            display_name = f"{first} {last}".strip() or "Unknown"

            job_title = person.get("job_title", "No title")
            role = person.get("role", "")
            source = person.get("source", "")
            std_title = person.get("standardised_job_title")
            middle = person.get("middle_names", "")

            std_str = f" [{std_title}]" if std_title else ""

            parsed = person.get("parsed", {})
            parsed_title = parsed.get("title", "")
            parsed_suffix = parsed.get("suffix", "")
            parsed_nickname = parsed.get("nickname", "")

            lines.append(f"  - {display_name}")
            if parsed_title:
                lines.append(f"    Honorific: {parsed_title}")
            if middle:
                lines.append(f"    Middle Names: {middle}")
            if parsed_suffix:
                lines.append(f"    Suffix: {parsed_suffix}")
            if parsed_nickname:
                lines.append(f"    Nickname: {parsed_nickname}")
            lines.append(f"    Title: {job_title}{std_str}")
            if person.get("organisation_name"):
                org_type = person.get("organisation_type", "")
                org_suffix = f" ({org_type})" if org_type else ""
                lines.append(f"    Organisation: {person['organisation_name']}{org_suffix}")
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
