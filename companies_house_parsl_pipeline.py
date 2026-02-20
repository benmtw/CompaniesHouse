"""
Parsl-based Companies House extraction pipeline.

Replacement for companies_house_full_reports_extraction_pipeline.py using Parsl
for parallel execution with dataflow patterns.

NOTE: Parsl requires fork-based multiprocessing and is NOT compatible with Windows.
      Use WSL2, Linux, or macOS to run this pipeline. On Windows, use the legacy
      pipeline: companies_house_full_reports_extraction_pipeline_legacy.py

Usage (Unix/WSL2/macOS):
    python companies_house_parsl_pipeline.py --company-numbers 00000006 --mode all
    python companies_house_parsl_pipeline.py --input-xlsx Trusts.xlsx --mode download
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import random
import sqlite3
import sys
import time
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Windows compatibility check
if platform.system() == "Windows":
    print(
        "ERROR: Parsl pipeline is not compatible with Windows due to fork-based multiprocessing.\n"
        "Options:\n"
        "  1. Use WSL2 (Windows Subsystem for Linux)\n"
        "  2. Use the legacy pipeline: companies_house_full_reports_extraction_pipeline_legacy.py\n"
        "  3. Run on Linux or macOS",
        file=sys.stderr,
    )
    sys.exit(1)

import parsl
from parsl_pipeline_apps import process_company
from parsl_pipeline_config import create_pipeline_config
from pipeline_shared import (
    add_common_extraction_cli_args,
    extraction_types_for_schema_profile,
    load_dotenv_file,
    normalize_company_number,
    parse_fallback_models,
    read_xlsx_rows,
    utc_now,
    utc_now_precise,
    write_json,
)


DEFAULT_OUTPUT_ROOT = "output/parsl_extraction_pipeline"
DEFAULT_DB_NAME = "parsl_extraction_pipeline.db"
DEFAULT_CH_WORKERS = 2
DEFAULT_OR_WORKERS = 4
DEFAULT_CH_CACHE_DIR = "output/ch_document_cache"


def _read_shared_throttle_request_count(state_path: Path) -> int:
    if not state_path.is_file():
        return 0
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 0
    return int(payload.get("request_count", 0))


def _init_shared_throttle_state(state_path: Path, lock_path: Path, min_interval_seconds: float) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch(exist_ok=True)
    state_payload = {
        "enabled": min_interval_seconds > 0,
        "min_interval_seconds": min_interval_seconds,
        "last_request_ts": 0.0,
        "request_count": 0,
    }
    state_path.write_text(json.dumps(state_payload), encoding="utf-8")


def _utc_now() -> str:
    return utc_now()


def _utc_now_precise() -> str:
    return utc_now_precise()


def _append_jsonl_locked(path: Path, payload: dict[str, Any], lock: threading.Lock) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=True)
    with lock:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(serialized + "\n")


def _parse_company_numbers_csv(raw: str) -> list[str]:
    seen: set[str] = set()
    parsed: list[str] = []
    for part in str(raw or "").split(","):
        normalized = normalize_company_number(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        parsed.append(normalized)
    return parsed


def _validate_input_xor(input_xlsx: str, company_numbers: str) -> None:
    has_xlsx = bool(str(input_xlsx or "").strip())
    has_company_numbers = bool(str(company_numbers or "").strip())
    if has_xlsx == has_company_numbers:
        raise ValueError(
            "Exactly one input source is required: provide --input-xlsx OR --company-numbers"
        )


def _validate_worker_settings(ch_workers: int, or_workers: int) -> None:
    if ch_workers < 1:
        raise ValueError("ch_workers must be >= 1")
    if or_workers < 1:
        raise ValueError("or_workers must be >= 1")


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            mode TEXT NOT NULL,
            input_source_type TEXT NOT NULL,
            input_source_value TEXT NOT NULL,
            output_run_dir TEXT NOT NULL,
            model TEXT,
            fallback_models_json TEXT,
            schema_profile TEXT,
            ch_workers INTEGER NOT NULL,
            or_workers INTEGER NOT NULL,
            executor_type TEXT NOT NULL,
            ch_min_request_interval_seconds REAL NOT NULL,
            filing_history_items_per_page INTEGER NOT NULL,
            retries_on_invalid_json INTEGER NOT NULL,
            openrouter_timeout_seconds REAL NOT NULL,
            parsl_monitoring INTEGER NOT NULL DEFAULT 0,
            total_jobs INTEGER NOT NULL DEFAULT 0,
            download_succeeded INTEGER NOT NULL DEFAULT 0,
            download_failed INTEGER NOT NULL DEFAULT 0,
            extract_succeeded INTEGER NOT NULL DEFAULT 0,
            extract_failed INTEGER NOT NULL DEFAULT 0,
            final_succeeded INTEGER NOT NULL DEFAULT 0,
            final_failed INTEGER NOT NULL DEFAULT 0,
            companies_house_request_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            job_index INTEGER NOT NULL,
            source_row_index INTEGER,
            group_uid TEXT,
            group_id TEXT,
            group_name TEXT,
            company_number TEXT NOT NULL,
            company_name TEXT,
            download_status TEXT NOT NULL,
            download_attempts INTEGER NOT NULL DEFAULT 0,
            download_started_at TEXT,
            download_ended_at TEXT,
            download_error TEXT,
            document_id TEXT,
            pdf_path TEXT,
            cache_hit INTEGER,
            pdf_size_bytes INTEGER,
            extract_status TEXT NOT NULL,
            extract_attempts INTEGER NOT NULL DEFAULT 0,
            extract_started_at TEXT,
            extract_ended_at TEXT,
            extract_error TEXT,
            model_used TEXT,
            extraction_json_path TEXT,
            warnings_json_path TEXT,
            run_report_json_path TEXT,
            final_status TEXT NOT NULL,
            final_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES pipeline_runs(run_id),
            UNIQUE(run_id, job_index)
        )
        """
    )
    conn.commit()


def _insert_run(conn: sqlite3.Connection, payload: dict[str, Any]) -> int:
    cursor = conn.execute(
        """
        INSERT INTO pipeline_runs (
            started_at,
            mode,
            input_source_type,
            input_source_value,
            output_run_dir,
            model,
            fallback_models_json,
            schema_profile,
            ch_workers,
            or_workers,
            executor_type,
            ch_min_request_interval_seconds,
            filing_history_items_per_page,
            retries_on_invalid_json,
            openrouter_timeout_seconds,
            parsl_monitoring,
            total_jobs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _utc_now(),
            payload["mode"],
            payload["input_source_type"],
            payload["input_source_value"],
            payload["output_run_dir"],
            payload.get("model"),
            payload.get("fallback_models_json"),
            payload.get("schema_profile"),
            payload["ch_workers"],
            payload["or_workers"],
            payload["executor_type"],
            payload["ch_min_request_interval_seconds"],
            payload["filing_history_items_per_page"],
            payload["retries_on_invalid_json"],
            payload["openrouter_timeout_seconds"],
            payload["parsl_monitoring"],
            payload["total_jobs"],
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _insert_job(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
    now = _utc_now()
    conn.execute(
        """
        INSERT INTO pipeline_jobs (
            run_id,
            job_index,
            source_row_index,
            group_uid,
            group_id,
            group_name,
            company_number,
            download_status,
            extract_status,
            final_status,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["run_id"],
            payload["job_index"],
            payload.get("source_row_index"),
            payload.get("group_uid"),
            payload.get("group_id"),
            payload.get("group_name"),
            payload["company_number"],
            payload["download_status"],
            payload["extract_status"],
            payload["final_status"],
            now,
            now,
        ),
    )
    conn.commit()


def _update_job_state_from_result(
    conn: sqlite3.Connection,
    run_id: int,
    result: dict[str, Any],
    mode: str,
) -> None:
    """Update job state based on a completed download or extraction result."""
    job_index = result["job_index"]
    company_number = result["company_number"]
    now_precise = _utc_now_precise()

    # Update download state
    download_status = "success" if result.get("success") else "failed"
    download_updates: dict[str, Any] = {
        "download_status": download_status,
        "download_ended_at": now_precise,
        "company_name": result.get("company_name"),
        "document_id": result.get("document_id"),
        "pdf_path": result.get("pdf_path"),
        "cache_hit": 1 if result.get("cache_hit") else 0,
        "pdf_size_bytes": result.get("pdf_size_bytes"),
        "download_error": result.get("error"),
    }
    _update_job_fields(conn, run_id, job_index, download_updates)

    # Update extract state
    if mode in ("all", "extract"):
        extract_success = result.get("extract_success")
        if extract_success is None:
            # Download-only mode or download failed
            extract_status = "skipped" if mode == "download" or not result.get("success") else "pending"
            extract_updates: dict[str, Any] = {
                "extract_status": extract_status,
            }
        else:
            extract_status = "success" if extract_success else "failed"
            extract_updates = {
                "extract_status": extract_status,
                "extract_ended_at": now_precise,
                "model_used": result.get("model_used"),
                "extraction_json_path": result.get("extraction_json_path"),
                "warnings_json_path": result.get("warnings_json_path"),
                "extract_error": result.get("extract_error"),
            }
        _update_job_fields(conn, run_id, job_index, extract_updates)

    # Update final state
    if mode == "download":
        final_status = "success" if result.get("success") else "failed"
        final_error = result.get("error")
    else:
        final_status = "success" if result.get("extract_success") else "failed"
        final_error = result.get("extract_error") or result.get("error")
    _update_final_state(conn, run_id, job_index, final_status, final_error)


def _update_job_fields(
    conn: sqlite3.Connection,
    run_id: int,
    job_index: int,
    updates: dict[str, Any],
) -> None:
    if not updates:
        return
    now = _utc_now()
    assignments = ["updated_at = ?"]
    params: list[Any] = [now]
    for key, value in updates.items():
        if value is None and key.endswith("_at"):
            continue  # Skip null timestamps
        assignments.append(f"{key} = ?")
        params.append(value)
    params.extend([run_id, job_index])
    conn.execute(
        f"""
        UPDATE pipeline_jobs
        SET {', '.join(assignments)}
        WHERE run_id = ? AND job_index = ?
        """,
        tuple(params),
    )
    conn.commit()


def _update_final_state(
    conn: sqlite3.Connection,
    run_id: int,
    job_index: int,
    final_status: str,
    final_error: str | None,
) -> None:
    conn.execute(
        """
        UPDATE pipeline_jobs
        SET final_status = ?, final_error = ?, updated_at = ?
        WHERE run_id = ? AND job_index = ?
        """,
        (final_status, final_error, _utc_now(), run_id, job_index),
    )
    conn.commit()


def _load_jobs_for_summary(conn: sqlite3.Connection, run_id: int) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """
        SELECT
            job_index,
            source_row_index,
            group_uid,
            group_id,
            group_name,
            company_number,
            company_name,
            download_status,
            download_attempts,
            document_id,
            pdf_path,
            cache_hit,
            pdf_size_bytes,
            extract_status,
            extract_attempts,
            model_used,
            extraction_json_path,
            warnings_json_path,
            run_report_json_path,
            final_status,
            final_error,
            download_error,
            extract_error
        FROM pipeline_jobs
        WHERE run_id = ?
        ORDER BY job_index ASC
        """,
        (run_id,),
    )
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _finalize_run(conn: sqlite3.Connection, run_id: int, ch_request_count: int) -> None:
    counts = conn.execute(
        """
        SELECT
            SUM(CASE WHEN download_status = 'success' THEN 1 ELSE 0 END),
            SUM(CASE WHEN download_status = 'failed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN extract_status = 'success' THEN 1 ELSE 0 END),
            SUM(CASE WHEN extract_status = 'failed' THEN 1 ELSE 0 END),
            SUM(CASE WHEN final_status = 'success' THEN 1 ELSE 0 END),
            SUM(CASE WHEN final_status = 'failed' THEN 1 ELSE 0 END)
        FROM pipeline_jobs
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    download_succeeded = int(counts[0] or 0)
    download_failed = int(counts[1] or 0)
    extract_succeeded = int(counts[2] or 0)
    extract_failed = int(counts[3] or 0)
    final_succeeded = int(counts[4] or 0)
    final_failed = int(counts[5] or 0)
    conn.execute(
        """
        UPDATE pipeline_runs
        SET
            finished_at = ?,
            download_succeeded = ?,
            download_failed = ?,
            extract_succeeded = ?,
            extract_failed = ?,
            final_succeeded = ?,
            final_failed = ?,
            companies_house_request_count = ?
        WHERE run_id = ?
        """,
        (
            _utc_now(),
            download_succeeded,
            download_failed,
            extract_succeeded,
            extract_failed,
            final_succeeded,
            final_failed,
            ch_request_count,
            run_id,
        ),
    )
    conn.commit()


def _read_run_row(conn: sqlite3.Connection, run_id: int) -> dict[str, Any]:
    cursor = conn.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,))
    row = cursor.fetchone()
    if row is None:
        return {}
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row, strict=False))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Parsl-based extraction pipeline: Companies House download workers and "
            "OpenRouter extraction workers with dataflow handoff and SQLite state."
        )
    )
    parser.add_argument("--input-xlsx", default="", help="Path to Trusts.xlsx input")
    parser.add_argument(
        "--company-numbers",
        default="",
        help="Comma-separated company numbers (XOR with --input-xlsx)",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "download", "extract"],
        default="all",
        help="Pipeline mode: all, download-only, or extract-only",
    )
    parser.add_argument("--ch-workers", type=int, default=DEFAULT_CH_WORKERS)
    parser.add_argument("--or-workers", type=int, default=DEFAULT_OR_WORKERS)
    parser.add_argument(
        "--executor-type",
        choices=["thread", "htex"],
        default="thread",
        help="Parsl executor type: thread (local) or htex (distributed)",
    )
    parser.add_argument(
        "--parsl-monitoring",
        action="store_true",
        help="Enable Parsl monitoring hub for visualization",
    )
    add_common_extraction_cli_args(
        parser,
        output_root_default=DEFAULT_OUTPUT_ROOT,
        db_help="SQLite file path (defaults to <output-root>/parsl_extraction_pipeline.db)",
    )
    return parser


def _build_batch_from_input(args: argparse.Namespace) -> tuple[list[dict[str, Any]], str, str]:
    _validate_input_xor(args.input_xlsx, args.company_numbers)
    batch: list[dict[str, Any]] = []

    if args.input_xlsx:
        input_xlsx = Path(args.input_xlsx)
        if not input_xlsx.exists() or not input_xlsx.is_file():
            raise FileNotFoundError(f"Input xlsx file not found: {input_xlsx}")
        if input_xlsx.suffix.lower() != ".xlsx":
            raise ValueError("Only .xlsx is supported for --input-xlsx")
        rows = read_xlsx_rows(input_xlsx)
        seen: set[str] = set()
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
        source_type = "xlsx"
        source_value = str(input_xlsx)
    else:
        numbers = _parse_company_numbers_csv(args.company_numbers)
        for index, company_number in enumerate(numbers, start=1):
            batch.append(
                {
                    "source_row_index": None,
                    "group_uid": None,
                    "group_id": None,
                    "group_name": None,
                    "company_number": company_number,
                }
            )
        source_type = "company_numbers"
        source_value = args.company_numbers

    if args.start_index > 0:
        batch = batch[args.start_index :]
    if args.max_companies > 0:
        batch = batch[: args.max_companies]
    if args.random_sample_size > 0 and len(batch) > args.random_sample_size:
        rng = random.Random(None if args.random_seed == 0 else args.random_seed)
        batch = rng.sample(batch, args.random_sample_size)

    return batch, source_type, source_value


def _require_mode_credentials(args: argparse.Namespace, ch_api_key: str, openrouter_api_key: str) -> None:
    if args.mode in {"all", "download"} and not ch_api_key:
        raise ValueError("Missing CH_API_KEY. Required for mode all/download.")
    if args.mode in {"all", "extract"}:
        if not openrouter_api_key:
            raise ValueError("Missing OPENROUTER_API_KEY. Required for mode all/extract.")
        if not str(args.model or "").strip():
            raise ValueError(
                "Missing model. Set OPENROUTER_MODEL in .env or pass --model explicitly."
            )


def main() -> int:
    load_dotenv_file(Path(".env"))
    args = _build_parser().parse_args()

    # Validate arguments
    if args.start_index < 0:
        raise ValueError("start_index must be >= 0")
    if args.max_companies < 0:
        raise ValueError("max_companies must be >= 0")
    if args.random_sample_size < 0:
        raise ValueError("random_sample_size must be >= 0")
    if args.filing_history_items_per_page <= 0:
        raise ValueError("filing_history_items_per_page must be > 0")
    if args.retries_on_invalid_json < 0:
        raise ValueError("retries_on_invalid_json must be >= 0")
    if args.openrouter_timeout_seconds <= 0:
        raise ValueError("openrouter_timeout_seconds must be > 0")
    if args.ch_min_request_interval_seconds < 0:
        raise ValueError("ch_min_request_interval_seconds must be >= 0")

    _validate_worker_settings(args.ch_workers, args.or_workers)

    ch_api_key = os.getenv("CH_API_KEY", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    _require_mode_credentials(args, ch_api_key, openrouter_api_key)

    # Build batch
    batch, source_type, source_value = _build_batch_from_input(args)
    for index, item in enumerate(batch, start=1):
        item["job_index"] = index

    # Set up directories
    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_root / f"run_{run_stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    events_path = run_dir / "events.jsonl"
    events_lock = threading.Lock()

    db_path = Path(args.db_path) if args.db_path else output_root / DEFAULT_DB_NAME
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    _create_tables(conn)

    # Insert run record
    run_id = _insert_run(
        conn,
        {
            "mode": args.mode,
            "input_source_type": source_type,
            "input_source_value": source_value,
            "output_run_dir": str(run_dir),
            "model": args.model,
            "fallback_models_json": json.dumps(parse_fallback_models(args.fallback_models)),
            "schema_profile": args.schema_profile,
            "ch_workers": args.ch_workers,
            "or_workers": args.or_workers,
            "executor_type": args.executor_type,
            "ch_min_request_interval_seconds": args.ch_min_request_interval_seconds,
            "filing_history_items_per_page": args.filing_history_items_per_page,
            "retries_on_invalid_json": args.retries_on_invalid_json,
            "openrouter_timeout_seconds": args.openrouter_timeout_seconds,
            "parsl_monitoring": 1 if args.parsl_monitoring else 0,
            "total_jobs": len(batch),
        },
    )

    def emit_event(event_type: str, **fields: Any) -> None:
        payload: dict[str, Any] = {
            "ts_utc": _utc_now_precise(),
            "run_id": run_id,
            "event": event_type,
        }
        payload.update(fields)
        _append_jsonl_locked(events_path, payload, events_lock)

    print(f"[run {run_id}] output_dir={run_dir}")
    print(f"[run {run_id}] db_path={db_path}")
    print(f"[run {run_id}] mode={args.mode}")
    print(f"[run {run_id}] jobs={len(batch)}")
    print(f"[run {run_id}] ch_workers={args.ch_workers} or_workers={args.or_workers}")
    print(f"[run {run_id}] executor_type={args.executor_type}")
    if args.parsl_monitoring:
        print(f"[run {run_id}] parsl_monitoring=enabled")

    emit_event(
        "run_started",
        mode=args.mode,
        input_source_type=source_type,
        input_source_value=source_value,
        total_jobs=len(batch),
        ch_workers=args.ch_workers,
        or_workers=args.or_workers,
        executor_type=args.executor_type,
        ch_min_request_interval_seconds=args.ch_min_request_interval_seconds,
        model=args.model,
        schema_profile=args.schema_profile,
    )

    # Insert job records
    for item in batch:
        initial_download_status = "pending" if args.mode in {"all", "download"} else "skipped"
        initial_extract_status = "pending" if args.mode in {"all", "extract"} else "skipped"
        _insert_job(
            conn,
            {
                "run_id": run_id,
                "job_index": item["job_index"],
                "source_row_index": item.get("source_row_index"),
                "group_uid": item.get("group_uid"),
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "company_number": item["company_number"],
                "download_status": initial_download_status,
                "extract_status": initial_extract_status,
                "final_status": "pending",
            },
        )

    # Initialize cross-process throttle state for HTEX worker processes.
    throttle_state_path = run_dir / "ch_throttle_state.json"
    throttle_lock_path = run_dir / "ch_throttle_state.lock"
    _init_shared_throttle_state(
        state_path=throttle_state_path,
        lock_path=throttle_lock_path,
        min_interval_seconds=args.ch_min_request_interval_seconds,
    )

    # Build model candidates
    model_candidates = [args.model] + parse_fallback_models(args.fallback_models)
    deduped_models: list[str] = []
    seen_models: set[str] = set()
    for model in model_candidates:
        if model and model not in seen_models:
            deduped_models.append(model)
            seen_models.add(model)

    extraction_types = extraction_types_for_schema_profile(args.schema_profile)

    # Build configs for apps
    download_config = {
        "run_dir": str(run_dir),
        "ch_api_key": ch_api_key,
        "filing_history_items_per_page": args.filing_history_items_per_page,
        "cache_dir": DEFAULT_CH_CACHE_DIR,
        "throttle_config": {
            "min_interval_seconds": args.ch_min_request_interval_seconds,
            "shared_state_path": str(throttle_state_path),
            "shared_lock_path": str(throttle_lock_path),
        },
    }

    extract_config = {
        "run_dir": str(run_dir),
        "openrouter_api_key": openrouter_api_key,
        "model_candidates": deduped_models,
        "extraction_types": [t.value for t in extraction_types],
        "retries_on_invalid_json": args.retries_on_invalid_json,
        "openrouter_timeout_seconds": args.openrouter_timeout_seconds,
        "write_openrouter_debug_artifacts": args.write_openrouter_debug_artifacts,
        "cache_dir": DEFAULT_CH_CACHE_DIR,
        "ch_api_key": ch_api_key if args.mode == "extract" else None,
        "filing_history_items_per_page": args.filing_history_items_per_page,
        "throttle_config": {
            "min_interval_seconds": args.ch_min_request_interval_seconds,
            "shared_state_path": str(throttle_state_path),
            "shared_lock_path": str(throttle_lock_path),
        },
    }

    # Create and load Parsl config
    config = create_pipeline_config(
        ch_workers=args.ch_workers,
        or_workers=args.or_workers,
        executor_type=args.executor_type,
        monitoring_enabled=args.parsl_monitoring,
        run_dir=run_dir / "parsl_logs",
    )

    parsl.load(config)

    try:
        # Launch all tasks
        futures = []
        for item in batch:
            future = process_company(
                company_number=item["company_number"],
                job_index=item["job_index"],
                mode=args.mode,
                download_config=download_config,
                extract_config=extract_config,
            )
            futures.append(future)
            emit_event(
                "job_submitted",
                job_index=item["job_index"],
                company_number=item["company_number"],
            )

        # Wait for all results and update state
        for future in futures:
            result = future.result()
            _update_job_state_from_result(conn, run_id, result, args.mode)

            # Emit completion event
            if args.mode == "download":
                event_type = "job_download_end"
                event_data = {
                    "job_index": result["job_index"],
                    "company_number": result["company_number"],
                    "status": "success" if result.get("success") else "failed",
                    "document_id": result.get("document_id"),
                    "cache_hit": result.get("cache_hit"),
                    "pdf_path": result.get("pdf_path"),
                }
            else:
                event_type = "job_extract_end"
                event_data = {
                    "job_index": result["job_index"],
                    "company_number": result["company_number"],
                    "status": "success" if result.get("extract_success") else "failed",
                    "model_used": result.get("model_used"),
                    "document_id": result.get("document_id"),
                }
            emit_event(event_type, **event_data)

    finally:
        parsl.clear()

    # Get final CH request count from cross-process state file.
    ch_request_count = _read_shared_throttle_request_count(throttle_state_path)

    # Finalize run
    time.sleep(0.05)
    _finalize_run(conn, run_id, ch_request_count)

    run_row = _read_run_row(conn, run_id)
    company_rows = _load_jobs_for_summary(conn, run_id)

    # Write summary JSON
    if args.write_summary_json or args.summary_json_path:
        summary_path = Path(args.summary_json_path) if args.summary_json_path else run_dir / "summary.json"
        summary_payload = {
            "run_type": "companies_house_parsl_pipeline",
            "run_id": run_id,
            "timestamp_utc": _utc_now(),
            "mode": args.mode,
            "input_source_type": source_type,
            "input_source_value": source_value,
            "output_run_dir": str(run_dir),
            "db_path": str(db_path),
            "model": args.model,
            "schema_profile": args.schema_profile,
            "ch_workers": args.ch_workers,
            "or_workers": args.or_workers,
            "executor_type": args.executor_type,
            "parsl_monitoring": args.parsl_monitoring,
            "ch_min_request_interval_seconds": args.ch_min_request_interval_seconds,
            "companies_house_request_count": run_row.get("companies_house_request_count"),
            "total_jobs": run_row.get("total_jobs"),
            "download_succeeded": run_row.get("download_succeeded"),
            "download_failed": run_row.get("download_failed"),
            "extract_succeeded": run_row.get("extract_succeeded"),
            "extract_failed": run_row.get("extract_failed"),
            "final_succeeded": run_row.get("final_succeeded"),
            "final_failed": run_row.get("final_failed"),
            "jobs": company_rows,
        }
        write_json(summary_path, summary_payload)
        emit_event("run_summary_written", summary_json_path=str(summary_path))
        print(f"[run {run_id}] summary_json_path={summary_path}")

    emit_event(
        "run_completed",
        download_succeeded=run_row.get("download_succeeded"),
        download_failed=run_row.get("download_failed"),
        extract_succeeded=run_row.get("extract_succeeded"),
        extract_failed=run_row.get("extract_failed"),
        final_succeeded=run_row.get("final_succeeded"),
        final_failed=run_row.get("final_failed"),
        companies_house_request_count=run_row.get("companies_house_request_count"),
    )

    conn.close()

    print(
        f"[run {run_id}] complete final_succeeded={run_row.get('final_succeeded')} "
        f"final_failed={run_row.get('final_failed')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
