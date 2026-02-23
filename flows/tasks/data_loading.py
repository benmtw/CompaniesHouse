"""Tasks for batch preparation, run initialization, and run finalization."""

import json
import random
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import task

from document_extraction_models import ExtractionType
from shared import (
    DEFAULT_DB_NAME,
    create_tables,
    ensure_parent,
    finalize_run as _finalize_run_core,
    insert_run,
    utc_now_iso,
)


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
    ensure_parent(resolved_db_path)
    conn = sqlite3.connect(resolved_db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    create_tables(conn)

    run_id = insert_run(
        conn=conn,
        input_xlsx_path=input_xlsx_path,
        output_run_dir=str(output_run_dir),
        model=model,
        extraction_types=extraction_types,
    )
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
    _finalize_run_core(
        conn=conn,
        run_id=run_id,
        total_companies=total_companies,
        processed=processed,
        succeeded=succeeded,
        failed=failed,
    )
    conn.close()
