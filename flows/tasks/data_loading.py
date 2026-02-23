"""Tasks for batch preparation, run initialization, and run finalization."""

import json
import random
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import task

from document_extraction_models import ExtractionType


DEFAULT_DB_NAME = "companies_house_extractions.db"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


@task(name="load-and-prepare-batch", retries=0)
def load_and_prepare_batch(
    input_xlsx: str,
    start_index: int,
    max_companies: int,
    random_sample_size: int,
    random_seed: int,
) -> list[dict]:
    """Read XLSX, deduplicate company numbers, apply slicing/sampling."""
    from batch_extract_companies import normalize_company_number, read_xlsx_rows

    input_path = Path(input_xlsx)
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Input xlsx file not found: {input_path}")

    rows = read_xlsx_rows(input_path)
    seen: set[str] = set()
    batch: list[dict[str, Any]] = []
    for source_index, row in enumerate(rows, start=2):
        normalized = normalize_company_number(row.get("Companies House Number", ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        batch.append(
            {
                "source_row_index": source_index,
                "group_uid": row.get("Group UID"),
                "group_id": row.get("Group ID"),
                "group_name": row.get("Group Name"),
                "company_number": normalized,
            }
        )

    if start_index > 0:
        batch = batch[start_index:]
    if max_companies > 0:
        batch = batch[:max_companies]
    if random_sample_size > 0 and len(batch) > random_sample_size:
        rng = random.Random(None if random_seed == 0 else random_seed)
        batch = rng.sample(batch, random_sample_size)

    return batch


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            input_xlsx_path TEXT NOT NULL,
            output_run_dir TEXT NOT NULL,
            model TEXT NOT NULL,
            extraction_types_json TEXT NOT NULL,
            total_companies INTEGER NOT NULL DEFAULT 0,
            processed INTEGER NOT NULL DEFAULT 0,
            succeeded INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            source_row_index INTEGER NOT NULL,
            group_uid TEXT,
            group_id TEXT,
            group_name TEXT,
            company_number TEXT NOT NULL,
            company_name TEXT,
            status TEXT NOT NULL,
            document_id TEXT,
            pdf_path TEXT,
            profile_json_path TEXT,
            filing_history_json_path TEXT,
            extraction_json_path TEXT,
            warnings_json_path TEXT,
            profile_json TEXT,
            filing_history_json TEXT,
            extraction_json TEXT,
            warnings_json TEXT,
            model_used TEXT,
            pdf_size_bytes INTEGER,
            approx_llm_tokens INTEGER,
            error_message TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )
        """
    )
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(company_reports)").fetchall()
    }
    if "model_used" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN model_used TEXT")
    if "pdf_size_bytes" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN pdf_size_bytes INTEGER")
    if "approx_llm_tokens" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN approx_llm_tokens INTEGER")
    conn.commit()


@task(name="initialize-run", retries=0)
def initialize_run(
    input_xlsx_path: str,
    output_root: str,
    model: str,
    extraction_types: list[ExtractionType],
    db_path: str = "",
) -> dict:
    """Create directories, SQLite tables, and insert run row.

    Returns dict with run_id, output_run_dir, db_path.
    """
    output_root_path = Path(output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_run_dir = output_root_path / f"run_{run_stamp}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    resolved_db_path = Path(db_path) if db_path else output_root_path / DEFAULT_DB_NAME
    _ensure_parent(resolved_db_path)
    conn = sqlite3.connect(resolved_db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    _create_tables(conn)

    cursor = conn.execute(
        """
        INSERT INTO runs (
            started_at,
            input_xlsx_path,
            output_run_dir,
            model,
            extraction_types_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            _utc_now(),
            input_xlsx_path,
            str(output_run_dir),
            model,
            json.dumps([e.value for e in extraction_types]),
        ),
    )
    conn.commit()
    run_id = int(cursor.lastrowid)
    conn.close()

    return {
        "run_id": run_id,
        "output_run_dir": str(output_run_dir),
        "db_path": str(resolved_db_path),
    }


@task(name="finalize-run", retries=0)
def finalize_run(
    db_path: str,
    run_id: int,
    total_companies: int,
    processed: int,
    succeeded: int,
    failed: int,
) -> None:
    """Update run totals and mark as finished."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        UPDATE runs
        SET
            finished_at = ?,
            total_companies = ?,
            processed = ?,
            succeeded = ?,
            failed = ?
        WHERE run_id = ?
        """,
        (_utc_now(), total_companies, processed, succeeded, failed, run_id),
    )
    conn.commit()
    conn.close()
