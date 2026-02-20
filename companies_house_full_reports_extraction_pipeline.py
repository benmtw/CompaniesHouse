import argparse
import json
import os
import queue
import random
import shutil
import sqlite3
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from batch_extract_trusts import (
    _derive_annual_report_from_component_sections,
    _extract_with_model_fallback,
    _extraction_types_for_schema_profile,
    _latest_full_accounts_document_id_from_filing_history,
    _load_dotenv_file,
    _parse_fallback_models,
    normalize_company_number,
    read_xlsx_rows,
    write_json,
)
from companies_house_client import CompaniesHouseClient


DEFAULT_OUTPUT_ROOT = "output/full_reports_extraction_pipeline"
DEFAULT_DB_NAME = "full_reports_extraction_pipeline.db"
DEFAULT_CH_WORKERS = 2
DEFAULT_OR_WORKERS = 4
DEFAULT_MAX_PENDING_EXTRACTIONS = 100
DEFAULT_CH_CACHE_DIR = "output/ch_document_cache"
MAX_ERROR_TRACEBACK_CHARS = 4000


@dataclass
class GlobalCHThrottle:
    min_interval_seconds: float
    lock: threading.Lock
    last_request_ts: float = 0.0
    request_count: int = 0

    @property
    def enabled(self) -> bool:
        return self.min_interval_seconds > 0


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _utc_now_precise() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


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


def _validate_worker_settings(ch_workers: int, or_workers: int, max_pending_extractions: int) -> None:
    if ch_workers < 1:
        raise ValueError("ch_workers must be >= 1")
    if or_workers < 1:
        raise ValueError("or_workers must be >= 1")
    if max_pending_extractions < 1:
        raise ValueError("max_pending_extractions must be >= 1")


def _install_global_throttle_on_client(
    client: CompaniesHouseClient,
    shared_throttle: GlobalCHThrottle,
) -> None:
    if not shared_throttle.enabled:
        return
    original_request = client.session.request

    def throttled_request(*args: Any, **kwargs: Any) -> Any:
        with shared_throttle.lock:
            now = time.monotonic()
            elapsed = now - shared_throttle.last_request_ts
            wait = shared_throttle.min_interval_seconds - elapsed
            if wait > 0:
                time.sleep(wait)
            shared_throttle.last_request_ts = time.monotonic()
            shared_throttle.request_count += 1
        return original_request(*args, **kwargs)

    client.session.request = throttled_request


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
            max_pending_extractions INTEGER NOT NULL,
            ch_min_request_interval_seconds REAL NOT NULL,
            filing_history_items_per_page INTEGER NOT NULL,
            retries_on_invalid_json INTEGER NOT NULL,
            openrouter_timeout_seconds REAL NOT NULL,
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
            max_pending_extractions,
            ch_min_request_interval_seconds,
            filing_history_items_per_page,
            retries_on_invalid_json,
            openrouter_timeout_seconds,
            total_jobs
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            payload["max_pending_extractions"],
            payload["ch_min_request_interval_seconds"],
            payload["filing_history_items_per_page"],
            payload["retries_on_invalid_json"],
            payload["openrouter_timeout_seconds"],
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


def _update_download_state(
    conn: sqlite3.Connection,
    run_id: int,
    job_index: int,
    status: str,
    updates: dict[str, Any] | None = None,
) -> None:
    updates = updates or {}
    assignments = ["download_status = ?", "updated_at = ?"]
    params: list[Any] = [status, _utc_now()]
    for key, value in updates.items():
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


def _update_extract_state(
    conn: sqlite3.Connection,
    run_id: int,
    job_index: int,
    status: str,
    updates: dict[str, Any] | None = None,
) -> None:
    updates = updates or {}
    assignments = ["extract_status = ?", "updated_at = ?"]
    params: list[Any] = [status, _utc_now()]
    for key, value in updates.items():
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


def _finalize_run(conn: sqlite3.Connection, run_id: int, shared_throttle: GlobalCHThrottle) -> None:
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
            shared_throttle.request_count,
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
            "Decoupled full-reports pipeline: Companies House download workers and "
            "OpenRouter extraction workers with bounded queue handoff and SQLite state."
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
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--db-path",
        default="",
        help="SQLite file path (defaults to <output-root>/full_reports_extraction_pipeline.db)",
    )
    parser.add_argument("--ch-workers", type=int, default=DEFAULT_CH_WORKERS)
    parser.add_argument("--or-workers", type=int, default=DEFAULT_OR_WORKERS)
    parser.add_argument(
        "--max-pending-extractions",
        type=int,
        default=DEFAULT_MAX_PENDING_EXTRACTIONS,
    )
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--max-companies", type=int, default=0)
    parser.add_argument("--random-sample-size", type=int, default=0)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--filing-history-items-per-page", type=int, default=100)
    parser.add_argument(
        "--ch-min-request-interval-seconds",
        type=float,
        default=float(os.getenv("CH_MIN_REQUEST_INTERVAL_SECONDS", "2.0")),
    )
    parser.add_argument("--model", default=os.getenv("OPENROUTER_MODEL", "").strip())
    parser.add_argument(
        "--fallback-models",
        default=os.getenv("OPENROUTER_FALLBACK_MODELS", ""),
    )
    parser.add_argument(
        "--schema-profile",
        choices=["compact_single_call", "full_legacy", "light_core", "personnel_only"],
        default=os.getenv("BATCH_SCHEMA_PROFILE", "compact_single_call"),
    )
    parser.add_argument("--retries-on-invalid-json", type=int, default=2)
    parser.add_argument(
        "--openrouter-timeout-seconds",
        type=float,
        default=float(os.getenv("OPENROUTER_TIMEOUT_SECONDS", "180")),
    )
    parser.add_argument("--write-openrouter-debug-artifacts", action="store_true")
    parser.add_argument("--write-summary-json", action="store_true")
    parser.add_argument("--summary-json-path", default="")
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


def _resolve_existing_pdf(output_root: Path, company_number: str) -> Path | None:
    pattern = f"**/{company_number}/documents/{company_number}_latest_full_accounts_*.pdf"
    candidates = [path for path in output_root.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def _resolve_cached_pdf_for_company(
    company_number: str,
    cache_dir: Path,
    accept: str = "application/pdf",
) -> tuple[Path | None, str | None]:
    index_path = cache_dir / "cache_index.jsonl"
    if not index_path.is_file():
        return None, None

    accept_normalized = accept.strip().lower()
    candidates: list[tuple[Path, str | None]] = []
    with index_path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            if str(payload.get("company_number") or "").strip() != company_number:
                continue
            if str(payload.get("accept") or "").strip().lower() != accept_normalized:
                continue

            cache_path_raw = str(payload.get("cache_path") or "").strip()
            if not cache_path_raw:
                continue
            document_id = str(payload.get("document_id") or "").strip() or None
            candidates.append((Path(cache_path_raw), document_id))

    for cache_path, document_id in reversed(candidates):
        if cache_path.is_file() and cache_path.stat().st_size > 0:
            return cache_path, document_id
    return None, None


def _materialize_cached_pdf_into_run(
    cache_path: Path,
    run_dir: Path,
    company_number: str,
    document_id: str | None,
) -> tuple[Path, str | None]:
    doc_id = str(document_id or "").strip() or None
    resolved_doc_id = doc_id or "cache_unknown_docid"
    target = (
        run_dir
        / company_number
        / "documents"
        / f"{company_number}_latest_full_accounts_{resolved_doc_id}.pdf"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists() or target.stat().st_size == 0:
        shutil.copy2(cache_path, target)
    return target, doc_id


def _enqueue_with_backpressure(
    extraction_queue: queue.Queue[Any],
    payload: dict[str, Any],
    shutdown_event: threading.Event,
    timeout_seconds: float = 0.1,
) -> bool:
    while not shutdown_event.is_set():
        try:
            extraction_queue.put(payload, timeout=timeout_seconds)
            return True
        except queue.Full:
            continue
    return False


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


class _DBWriter:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._queue: queue.Queue[Any] = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            fn, args = item
            try:
                fn(*args)
            finally:
                self._queue.task_done()

    def call(self, fn: Any, *args: Any) -> None:
        self._queue.put((fn, args))

    def drain(self) -> None:
        self._queue.join()

    def stop(self) -> None:
        self.drain()
        self._queue.put(None)
        self._queue.join()
        self._thread.join()


def _download_stage_worker(
    run_id: int,
    item: dict[str, Any],
    args: argparse.Namespace,
    run_dir: Path,
    ch_api_key: str,
    shared_throttle: GlobalCHThrottle,
    extraction_queue: queue.Queue[Any],
    shutdown_event: threading.Event,
    emit_event: Any,
    db_writer: _DBWriter,
) -> None:
    job_index = item["job_index"]
    company_number = item["company_number"]
    company_dir = run_dir / company_number
    api_dir = company_dir / "api"
    doc_dir = company_dir / "documents"
    api_dir.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    db_writer.call(
        _update_download_state,
        db_writer.conn,
        run_id,
        job_index,
        "running",
        {
            "download_attempts": 1,
            "download_started_at": _utc_now_precise(),
        },
    )
    emit_event(
        "job_download_start",
        job_index=job_index,
        company_number=company_number,
    )

    client = CompaniesHouseClient(api_key=ch_api_key)
    _install_global_throttle_on_client(client, shared_throttle)

    try:
        profile = client.get_company_profile(company_number)
        filing_page = client.get_filing_history(
            company_number=company_number,
            items_per_page=args.filing_history_items_per_page,
            start_index=0,
        )
        filings = filing_page.get("items") or []
        write_json(api_dir / "profile.json", profile)
        write_json(api_dir / "filing_history.json", filings)

        document_id = _latest_full_accounts_document_id_from_filing_history(filings)
        if not document_id:
            raise ValueError("No full accounts document found")

        pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
        client.download_document(
            document_id=document_id,
            output_path=str(pdf_path),
            accept="application/pdf",
            company_number=company_number,
        )
        pdf_size_bytes = pdf_path.stat().st_size

        db_writer.call(
            _update_download_state,
            db_writer.conn,
            run_id,
            job_index,
            "success",
            {
                "download_ended_at": _utc_now_precise(),
                "company_name": profile.get("company_name"),
                "document_id": document_id,
                "pdf_path": str(pdf_path),
                "cache_hit": 1 if client.last_download_cache_hit else 0,
                "pdf_size_bytes": pdf_size_bytes,
                "download_error": None,
            },
        )

        emit_event(
            "job_download_end",
            job_index=job_index,
            company_number=company_number,
            status="success",
            document_id=document_id,
            cache_hit=client.last_download_cache_hit,
            pdf_path=str(pdf_path),
        )

        if args.mode == "all":
            token = {
                "job_index": job_index,
                "company_number": company_number,
                "pdf_path": str(pdf_path),
                "document_id": document_id,
            }
            queued = _enqueue_with_backpressure(extraction_queue, token, shutdown_event)
            if queued:
                emit_event(
                    "job_enqueued",
                    job_index=job_index,
                    company_number=company_number,
                    stage="extract",
                )
    except Exception as exc:
        error_text = str(exc)
        db_writer.call(
            _update_download_state,
            db_writer.conn,
            run_id,
            job_index,
            "failed",
            {
                "download_ended_at": _utc_now_precise(),
                "download_error": error_text,
            },
        )
        db_writer.call(
            _update_extract_state,
            db_writer.conn,
            run_id,
            job_index,
            "skipped",
            {
                "extract_error": "download stage failed",
                "extract_ended_at": _utc_now_precise(),
            },
        )
        db_writer.call(
            _update_final_state,
            db_writer.conn,
            run_id,
            job_index,
            "failed",
            error_text,
        )
        emit_event(
            "job_failed",
            job_index=job_index,
            company_number=company_number,
            stage="download",
            error=error_text,
        )


def _download_company_fallback_for_extract(
    company_number: str,
    run_dir: Path,
    args: argparse.Namespace,
    ch_client: CompaniesHouseClient,
) -> tuple[str, str, int]:
    company_dir = run_dir / company_number
    api_dir = company_dir / "api"
    doc_dir = company_dir / "documents"
    api_dir.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    profile = ch_client.get_company_profile(company_number)
    filing_page = ch_client.get_filing_history(
        company_number=company_number,
        items_per_page=args.filing_history_items_per_page,
        start_index=0,
    )
    filings = filing_page.get("items") or []
    write_json(api_dir / "profile.json", profile)
    write_json(api_dir / "filing_history.json", filings)

    document_id = _latest_full_accounts_document_id_from_filing_history(filings)
    if not document_id:
        raise ValueError("No full accounts document found")

    pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
    ch_client.download_document(
        document_id=document_id,
        output_path=str(pdf_path),
        accept="application/pdf",
        company_number=company_number,
    )
    return str(pdf_path), document_id, int(pdf_path.stat().st_size)


def _extract_stage_worker_loop(
    run_id: int,
    args: argparse.Namespace,
    run_dir: Path,
    output_root: Path,
    extraction_queue: queue.Queue[Any],
    ch_api_key: str,
    openrouter_api_key: str,
    shared_throttle: GlobalCHThrottle,
    emit_event: Any,
    db_writer: _DBWriter,
) -> None:
    model_candidates = [args.model] + _parse_fallback_models(args.fallback_models)
    deduped_models: list[str] = []
    seen_models: set[str] = set()
    for model in model_candidates:
        if model and model not in seen_models:
            deduped_models.append(model)
            seen_models.add(model)

    extraction_types = _extraction_types_for_schema_profile(args.schema_profile)
    cache_dir = Path(DEFAULT_CH_CACHE_DIR)

    ch_client: CompaniesHouseClient | None = None
    if ch_api_key:
        ch_client = CompaniesHouseClient(api_key=ch_api_key)
        _install_global_throttle_on_client(ch_client, shared_throttle)

    while True:
        token = extraction_queue.get()
        if token is None:
            return

        job_index = int(token["job_index"])
        company_number = str(token["company_number"])
        document_id = token.get("document_id")
        pdf_path = token.get("pdf_path")

        db_writer.call(
            _update_extract_state,
            db_writer.conn,
            run_id,
            job_index,
            "running",
            {
                "extract_attempts": 1,
                "extract_started_at": _utc_now_precise(),
            },
        )
        emit_event(
            "job_extract_start",
            job_index=job_index,
            company_number=company_number,
        )

        try:
            resolved_pdf_path: Path | None = None
            if pdf_path and Path(pdf_path).is_file():
                resolved_pdf_path = Path(pdf_path)
            if resolved_pdf_path is None:
                resolved_pdf_path = _resolve_existing_pdf(output_root, company_number)
            if resolved_pdf_path is None:
                cached_path, cached_document_id = _resolve_cached_pdf_for_company(
                    company_number=company_number,
                    cache_dir=cache_dir,
                    accept="application/pdf",
                )
                if cached_path is not None:
                    materialized_path, materialized_document_id = _materialize_cached_pdf_into_run(
                        cache_path=cached_path,
                        run_dir=run_dir,
                        company_number=company_number,
                        document_id=cached_document_id,
                    )
                    resolved_pdf_path = materialized_path
                    if materialized_document_id:
                        document_id = materialized_document_id
                    db_writer.call(
                        _update_download_state,
                        db_writer.conn,
                        run_id,
                        job_index,
                        "success",
                        {
                            "download_ended_at": _utc_now_precise(),
                            "document_id": document_id,
                            "pdf_path": str(materialized_path),
                            "cache_hit": 1,
                            "pdf_size_bytes": int(materialized_path.stat().st_size),
                            "download_error": None,
                        },
                    )
                    emit_event(
                        "job_download_end",
                        job_index=job_index,
                        company_number=company_number,
                        status="success",
                        document_id=document_id,
                        cache_hit=True,
                        pdf_path=str(materialized_path),
                        source="cache_index",
                    )
            if resolved_pdf_path is None:
                if ch_client is None:
                    raise ValueError(
                        "Missing local PDF and CH_API_KEY not set for extract-mode fallback"
                    )
                fetched_pdf_path, fetched_document_id, fetched_size = _download_company_fallback_for_extract(
                    company_number=company_number,
                    run_dir=run_dir,
                    args=args,
                    ch_client=ch_client,
                )
                resolved_pdf_path = Path(fetched_pdf_path)
                document_id = fetched_document_id
                db_writer.call(
                    _update_download_state,
                    db_writer.conn,
                    run_id,
                    job_index,
                    "success",
                    {
                        "download_ended_at": _utc_now_precise(),
                        "document_id": fetched_document_id,
                        "pdf_path": fetched_pdf_path,
                        "cache_hit": 1 if ch_client.last_download_cache_hit else 0,
                        "pdf_size_bytes": fetched_size,
                        "download_error": None,
                    },
                )

            extraction_dir = run_dir / company_number / "extraction"
            extraction_dir.mkdir(parents=True, exist_ok=True)
            openrouter_debug_dir = (
                extraction_dir / "openrouter_debug"
                if args.write_openrouter_debug_artifacts
                else None
            )
            extraction_payload, warnings_payload, model_used = _extract_with_model_fallback(
                api_key=openrouter_api_key,
                model_candidates=deduped_models,
                document_path=str(resolved_pdf_path),
                extraction_types=extraction_types,
                retries_on_invalid_json=args.retries_on_invalid_json,
                openrouter_timeout_seconds=args.openrouter_timeout_seconds,
                openrouter_debug_dir=openrouter_debug_dir,
            )
            if extraction_payload.get("academy_trust_annual_report") is None:
                derived = _derive_annual_report_from_component_sections(extraction_payload)
                if derived is not None:
                    extraction_payload["academy_trust_annual_report"] = derived

            extraction_path = extraction_dir / "extraction_result.json"
            warnings_path = extraction_dir / "validation_warnings.json"
            run_report_path = extraction_dir / "run_report.json"
            run_report = {
                "run_id": run_id,
                "job_index": job_index,
                "company_number": company_number,
                "document_id": document_id,
                "pdf_path": str(resolved_pdf_path),
                "model": args.model,
                "model_used": model_used,
                "schema_profile": args.schema_profile,
                "requested_types": [t.value for t in extraction_types],
                "extraction_result": extraction_payload,
            }
            write_json(extraction_path, extraction_payload)
            write_json(warnings_path, warnings_payload)
            write_json(run_report_path, run_report)

            db_writer.call(
                _update_extract_state,
                db_writer.conn,
                run_id,
                job_index,
                "success",
                {
                    "extract_ended_at": _utc_now_precise(),
                    "extract_error": None,
                    "model_used": model_used,
                    "extraction_json_path": str(extraction_path),
                    "warnings_json_path": str(warnings_path),
                    "run_report_json_path": str(run_report_path),
                },
            )
            db_writer.call(
                _update_final_state,
                db_writer.conn,
                run_id,
                job_index,
                "success",
                None,
            )
            emit_event(
                "job_extract_end",
                job_index=job_index,
                company_number=company_number,
                status="success",
                model_used=model_used,
                document_id=document_id,
            )
        except Exception as exc:
            error_text = "".join(
                traceback.format_exception(exc.__class__, exc, exc.__traceback__)
            )[:MAX_ERROR_TRACEBACK_CHARS]
            db_writer.call(
                _update_extract_state,
                db_writer.conn,
                run_id,
                job_index,
                "failed",
                {
                    "extract_ended_at": _utc_now_precise(),
                    "extract_error": error_text,
                },
            )
            db_writer.call(
                _update_final_state,
                db_writer.conn,
                run_id,
                job_index,
                "failed",
                str(exc),
            )
            emit_event(
                "job_failed",
                job_index=job_index,
                company_number=company_number,
                stage="extract",
                error=str(exc),
            )


def main() -> int:
    _load_dotenv_file(Path(".env"))
    args = _build_parser().parse_args()

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

    _validate_worker_settings(args.ch_workers, args.or_workers, args.max_pending_extractions)

    ch_api_key = os.getenv("CH_API_KEY", "").strip()
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    _require_mode_credentials(args, ch_api_key, openrouter_api_key)

    batch, source_type, source_value = _build_batch_from_input(args)
    for index, item in enumerate(batch, start=1):
        item["job_index"] = index

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

    run_id = _insert_run(
        conn,
        {
            "mode": args.mode,
            "input_source_type": source_type,
            "input_source_value": source_value,
            "output_run_dir": str(run_dir),
            "model": args.model,
            "fallback_models_json": json.dumps(_parse_fallback_models(args.fallback_models)),
            "schema_profile": args.schema_profile,
            "ch_workers": args.ch_workers,
            "or_workers": args.or_workers,
            "max_pending_extractions": args.max_pending_extractions,
            "ch_min_request_interval_seconds": args.ch_min_request_interval_seconds,
            "filing_history_items_per_page": args.filing_history_items_per_page,
            "retries_on_invalid_json": args.retries_on_invalid_json,
            "openrouter_timeout_seconds": args.openrouter_timeout_seconds,
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
    print(f"[run {run_id}] max_pending_extractions={args.max_pending_extractions}")

    emit_event(
        "run_started",
        mode=args.mode,
        input_source_type=source_type,
        input_source_value=source_value,
        total_jobs=len(batch),
        ch_workers=args.ch_workers,
        or_workers=args.or_workers,
        max_pending_extractions=args.max_pending_extractions,
        ch_min_request_interval_seconds=args.ch_min_request_interval_seconds,
        model=args.model,
        schema_profile=args.schema_profile,
    )

    db_writer = _DBWriter(conn)
    db_writer.start()
    for item in batch:
        initial_download_status = "pending" if args.mode in {"all", "download"} else "skipped"
        db_writer.call(
            _insert_job,
            db_writer.conn,
            {
                "run_id": run_id,
                "job_index": item["job_index"],
                "source_row_index": item.get("source_row_index"),
                "group_uid": item.get("group_uid"),
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "company_number": item["company_number"],
                "download_status": initial_download_status,
                "extract_status": "pending" if args.mode in {"all", "extract"} else "skipped",
                "final_status": "pending",
            },
        )

    shared_throttle = GlobalCHThrottle(
        min_interval_seconds=args.ch_min_request_interval_seconds,
        lock=threading.Lock(),
    )
    extraction_queue: queue.Queue[Any] = queue.Queue(maxsize=args.max_pending_extractions)
    shutdown_event = threading.Event()

    extract_threads: list[threading.Thread] = []
    if args.mode in {"all", "extract"}:
        for _ in range(args.or_workers):
            thread = threading.Thread(
                target=_extract_stage_worker_loop,
                kwargs={
                    "run_id": run_id,
                    "args": args,
                    "run_dir": run_dir,
                    "output_root": output_root,
                    "extraction_queue": extraction_queue,
                    "ch_api_key": ch_api_key,
                    "openrouter_api_key": openrouter_api_key,
                    "shared_throttle": shared_throttle,
                    "emit_event": emit_event,
                    "db_writer": db_writer,
                },
                daemon=True,
            )
            thread.start()
            extract_threads.append(thread)

    if args.mode in {"all", "download"}:
        with ThreadPoolExecutor(max_workers=args.ch_workers) as executor:
            futures = [
                executor.submit(
                    _download_stage_worker,
                    run_id,
                    item,
                    args,
                    run_dir,
                    ch_api_key,
                    shared_throttle,
                    extraction_queue,
                    shutdown_event,
                    emit_event,
                    db_writer,
                )
                for item in batch
            ]
            for future in futures:
                future.result()

    if args.mode == "download":
        for item in batch:
            db_writer.call(
                _update_extract_state,
                db_writer.conn,
                run_id,
                item["job_index"],
                "skipped",
                {
                    "extract_error": "mode=download",
                    "extract_ended_at": _utc_now_precise(),
                },
            )
        db_writer.drain()
        conn.execute(
            """
            UPDATE pipeline_jobs
            SET
                final_status = CASE
                    WHEN download_status = 'success' THEN 'success'
                    ELSE 'failed'
                END,
                final_error = CASE
                    WHEN download_status = 'success' THEN NULL
                    ELSE COALESCE(download_error, 'download failed')
                END,
                updated_at = ?
            WHERE run_id = ?
            """,
            (_utc_now(), run_id),
        )
        conn.commit()

    if args.mode == "extract":
        for item in batch:
            token = {
                "job_index": item["job_index"],
                "company_number": item["company_number"],
                "pdf_path": None,
                "document_id": None,
            }
            _enqueue_with_backpressure(extraction_queue, token, shutdown_event)
            emit_event(
                "job_enqueued",
                job_index=item["job_index"],
                company_number=item["company_number"],
                stage="extract",
            )

    if args.mode in {"all", "extract"}:
        for _ in extract_threads:
            extraction_queue.put(None)
        for thread in extract_threads:
            thread.join()

    shutdown_event.set()
    db_writer.drain()
    time.sleep(0.05)
    _finalize_run(conn, run_id, shared_throttle)

    run_row = _read_run_row(conn, run_id)
    company_rows = _load_jobs_for_summary(conn, run_id)

    if args.write_summary_json or args.summary_json_path:
        summary_path = Path(args.summary_json_path) if args.summary_json_path else run_dir / "summary.json"
        summary_payload = {
            "run_type": "companies_house_full_reports_extraction_pipeline",
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
            "max_pending_extractions": args.max_pending_extractions,
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

    db_writer.stop()
    conn.close()

    print(
        f"[run {run_id}] complete final_succeeded={run_row.get('final_succeeded')} "
        f"final_failed={run_row.get('final_failed')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
