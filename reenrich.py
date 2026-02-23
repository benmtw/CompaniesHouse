"""Re-enrich existing extraction data in SQLite and on-disk JSON files.

Reads extraction_json from company_reports, runs enrich_personnel_names()
(which caches Gemini calls to disk), applies name parsing to split
first/middle names, and updates both SQLite and the on-disk JSON files.
"""

import argparse
import json
import logging
import sqlite3
from pathlib import Path

from name_enrichment import enrich_personnel_names
from nameparser import HumanName

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


def parse_enriched_names(personnel: list[dict]) -> list[dict]:
    """Post-process personnel to split enriched first names from middle names.

    For personnel where first_name_enriched was set, parse the full name
    to ensure first_name contains only the first name and middle_names
    is populated correctly.
    """
    for person in personnel:
        first = (person.get("first_name") or "").strip()
        last = (person.get("last_name") or "").strip()
        if not first or not last:
            continue

        # Parse the current name to split first/middle properly
        parsed = HumanName(f"{first} {last}")
        parsed_first = str(parsed.first).strip()
        parsed_middle = str(parsed.middle).strip()

        if parsed_first and parsed_first != first:
            person["first_name"] = parsed_first
        if parsed_middle:
            existing_middle = (person.get("middle_names") or "").strip()
            if not existing_middle:
                person["middle_names"] = parsed_middle

    return personnel


def reenrich_row(row_id, company_name, extraction_json_raw, gemini_api_key, cache_dir):
    """Re-enrich a single row's extraction JSON. Returns updated JSON string or None if unchanged."""
    try:
        data = json.loads(extraction_json_raw)
    except (json.JSONDecodeError, TypeError):
        return None

    personnel = data.get("personnel_details")
    if not personnel or not isinstance(personnel, list):
        return None

    # Strip any old enrichment fields so we start clean
    for person in personnel:
        person.pop("first_name_extracted", None)
        person.pop("first_name_enriched", None)
        person.pop("middle_names", None)
        # Restore original first name if we have an extracted version
        # (means a previous enrichment changed it)
        # We don't have the original stored separately, so we leave first_name as-is

    # Run enrichment (caches Gemini calls to disk)
    personnel = enrich_personnel_names(
        personnel=personnel,
        company_name=company_name or "",
        gemini_api_key=gemini_api_key,
        cache_dir=cache_dir,
    )

    # Post-process: split any multi-word first names into first + middle
    personnel = parse_enriched_names(personnel)

    data["personnel_details"] = personnel
    return json.dumps(data, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Re-enrich extraction data in SQLite and JSON files")
    parser.add_argument(
        "--db-path",
        default="output/companies_extraction/companies_house_extractions.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--enrichment-cache-dir",
        default="output/enrichment_cache",
        help="Directory for enrichment cache files",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        help="Only process rows from this run (default: all runs)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"Error: database not found at {db_path}")
        return 1

    # Load Gemini API key from environment
    import os
    from name_enrichment import DEFAULT_ENRICHMENT_CACHE_DIR

    # Load .env
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip().strip("'").strip('"')

    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not gemini_key:
        print("Error: GEMINI_API_KEY not set in environment or .env")
        return 1

    cache_dir = args.enrichment_cache_dir

    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row

    where = "status = 'success' AND extraction_json IS NOT NULL"
    params = ()
    if args.run_id is not None:
        where += " AND run_id = ?"
        params = (args.run_id,)

    rows = conn.execute(
        f"SELECT id, company_number, company_name, extraction_json, extraction_json_path "
        f"FROM company_reports WHERE {where} ORDER BY id",
        params,
    ).fetchall()

    print(f"Found {len(rows)} rows to process")

    updated = 0
    skipped = 0
    errors = 0

    for row in rows:
        row_id = row["id"]
        company_number = row["company_number"]
        company_name = row["company_name"] or ""
        extraction_json_raw = row["extraction_json"]
        json_path = row["extraction_json_path"]

        prefix = f"[{row_id}] {company_number} ({company_name})"

        try:
            new_json = reenrich_row(row_id, company_name, extraction_json_raw, gemini_key, cache_dir)
        except Exception as e:
            print(f"{prefix} ERROR: {e}")
            errors += 1
            continue

        if new_json is None:
            skipped += 1
            continue

        if args.dry_run:
            # Count how many would change
            old_data = json.loads(extraction_json_raw)
            new_data = json.loads(new_json)
            old_pd = old_data.get("personnel_details", [])
            new_pd = new_data.get("personnel_details", [])
            changes = []
            for old_p, new_p in zip(old_pd, new_pd):
                if old_p.get("first_name") != new_p.get("first_name"):
                    changes.append(f"  {old_p.get('first_name')} -> {new_p.get('first_name')} {new_p.get('last_name')}")
            if changes:
                print(f"{prefix} would update:")
                for c in changes:
                    print(c)
            continue

        # Update SQLite
        conn.execute(
            "UPDATE company_reports SET extraction_json = ? WHERE id = ?",
            (new_json, row_id),
        )

        # Update on-disk JSON file if it exists
        if json_path:
            json_file = Path(json_path)
            if json_file.exists():
                json_file.write_text(new_json, encoding="utf-8")

        updated += 1
        print(f"{prefix} updated")

    if not args.dry_run:
        conn.commit()

    conn.close()

    print(f"\nDone: {updated} updated, {skipped} skipped, {errors} errors")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
