"""Shared utilities used by both the CLI batch script and Prefect flows.

Consolidates duplicated helper functions that were previously defined in
``batch_extract_companies.py``, ``flows/batch_extract.py``, and the
``flows/tasks/`` modules.
"""

import json
import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from document_extraction_models import ExtractionType
from openrouter_document_extractor import DocumentExtractionError, OpenRouterDocumentExtractor


# ── Constants ──────────────────────────────────────────────────────────────────

DEFAULT_INPUT_XLSX = "SourceData/allgroupslinksdata20260217/Trusts.xlsx"
DEFAULT_OUTPUT_ROOT = "output/companies_extraction"
DEFAULT_DB_NAME = "companies_house_extractions.db"
MAX_ERROR_TRACEBACK_CHARS = 4000


# ── Small utilities ────────────────────────────────────────────────────────────


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string (no microseconds)."""
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def ensure_parent(path: Path) -> None:
    """Create parent directories for *path* if they don't exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    """Write *payload* as pretty-printed JSON to *path*."""
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def deduplicate_ordered(items: list[str]) -> list[str]:
    """Return *items* in original order with duplicates removed."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def parse_fallback_models(raw_models: str) -> list[str]:
    """Parse a comma-separated model list, deduplicating and stripping whitespace."""
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_models.split(","):
        model = part.strip()
        if not model or model in seen:
            continue
        seen.add(model)
        out.append(model)
    return out


def estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes: int) -> int:
    """Coarse heuristic: ~4 bytes per token for reporting purposes."""
    if pdf_size_bytes <= 0:
        return 0
    return int(math.ceil(pdf_size_bytes / 4.0))


# ── LLM error detection ───────────────────────────────────────────────────────


def is_file_not_supported_error(exc: Exception) -> bool:
    """Check if the LLM provider rejected file content types."""
    message = str(exc).lower()
    return (
        "does not support file content types" in message
        or "invalid value: 'file'" in message
        or "messages[1].content[1].type" in message
    )


def is_invalid_json_error(exc: Exception) -> bool:
    """Check if the LLM response was malformed JSON."""
    return "response was not valid json" in str(exc).lower()


def is_schema_depth_error(exc: Exception) -> bool:
    """Check if the schema exceeded the provider's nesting-depth limit."""
    message = str(exc).lower()
    return "maximum allowed nesting depth" in message or "schema-depth" in message


# ── Filing helpers ─────────────────────────────────────────────────────────────


def is_full_accounts_filing(item: dict[str, Any]) -> bool:
    """Return True if a filing-history item represents full accounts."""
    filing_type = str(item.get("type") or "").upper()
    description = str(item.get("description") or "").lower()
    if filing_type != "AA":
        return False
    return "accounts-with-accounts-type-full" in description or "full" in description


def latest_full_accounts_document_id(
    filings: list[dict[str, Any]],
) -> str | None:
    """Find the document_id for the most recent full-accounts filing, or None."""
    candidates: list[dict[str, Any]] = []
    for item in filings:
        if not is_full_accounts_filing(item):
            continue
        links = item.get("links") or {}
        metadata_url = str(links.get("document_metadata") or "").strip()
        if not metadata_url:
            continue
        candidates.append(item)

    if not candidates:
        return None

    latest_item = sorted(
        candidates, key=lambda i: (str(i.get("date") or "")), reverse=True
    )[0]
    metadata_url = str(
        (latest_item.get("links") or {}).get("document_metadata") or ""
    ).strip()
    return CompaniesHouseClient._extract_document_id(metadata_url)


# ── Extraction configuration ──────────────────────────────────────────────────


def extraction_types_for_schema_profile(
    schema_profile: str,
    company_type: CompanyType = CompanyType.GENERIC,
) -> list[ExtractionType]:
    """Map a schema profile name to the list of ExtractionTypes to request."""
    annual_report_type = (
        ExtractionType.AcademyTrustAnnualReport
        if company_type == CompanyType.ACADEMY_TRUST
        else ExtractionType.AnnualReport
    )
    if schema_profile == "full_legacy":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
            ExtractionType.StatementOfFinancialActivities,
            ExtractionType.DetailedBalanceSheet,
            ExtractionType.StaffingData,
            annual_report_type,
        ]
    if schema_profile == "compact_single_call":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
            ExtractionType.StatementOfFinancialActivities,
            ExtractionType.DetailedBalanceSheet,
            ExtractionType.StaffingData,
        ]
    if schema_profile == "light_core":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
        ]
    raise ValueError(f"Unsupported schema_profile: {schema_profile}")


def derive_annual_report_from_component_sections(
    extraction_payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Build a synthetic annual-report dict from individual extracted sections."""
    metadata = extraction_payload.get("metadata")
    governance = extraction_payload.get("governance")
    sofa = extraction_payload.get("statement_of_financial_activities")
    balance_sheet = extraction_payload.get("detailed_balance_sheet")
    staffing_data = extraction_payload.get("staffing_data")
    if (
        metadata is None
        and governance is None
        and sofa is None
        and balance_sheet is None
        and staffing_data is None
    ):
        return None
    return {
        "metadata": metadata,
        "governance": governance,
        "statement_of_financial_activities": sofa,
        "balance_sheet": balance_sheet,
        "staffing_data": staffing_data,
    }


def extract_with_model_fallback(
    api_key: str,
    model_candidates: list[str],
    document_path: str,
    extraction_types: list[ExtractionType],
    retries_on_invalid_json: int = 2,
    company_type: CompanyType = CompanyType.GENERIC,
) -> tuple[dict[str, Any], list[str], str]:
    """Try each model in order, with JSON-retry logic per model.

    Returns ``(extraction_payload, warnings, model_used)``.
    """
    errors: list[str] = []
    for model in model_candidates:
        attempts = retries_on_invalid_json + 1
        for attempt in range(1, attempts + 1):
            extractor = OpenRouterDocumentExtractor(
                api_key=api_key, model=model, company_type=company_type
            )
            try:
                result = extractor.extract(
                    document_path=document_path,
                    extraction_types=extraction_types,
                )
                return result.model_dump(mode="json"), result.validation_warnings or [], model
            except DocumentExtractionError as exc:
                if is_file_not_supported_error(exc):
                    errors.append(f"{model}: {exc}")
                    break
                if is_invalid_json_error(exc) and attempt < attempts:
                    errors.append(f"{model} attempt {attempt}/{attempts}: {exc}")
                    continue
                raise
    if errors:
        raise DocumentExtractionError(
            "All configured models failed file-input support checks: "
            + " | ".join(errors)
        )
    raise DocumentExtractionError("No models configured for extraction")


# ── Database operations ────────────────────────────────────────────────────────


def create_tables(conn: sqlite3.Connection) -> None:
    """Create the ``runs`` and ``company_reports`` tables if they don't exist."""
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


def insert_run(
    conn: sqlite3.Connection,
    input_xlsx_path: str,
    output_run_dir: str,
    model: str,
    extraction_types: list[ExtractionType],
) -> int:
    """Insert a new run record and return its ``run_id``."""
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
            utc_now_iso(),
            input_xlsx_path,
            output_run_dir,
            model,
            json.dumps([e.value for e in extraction_types]),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    total_companies: int,
    processed: int,
    succeeded: int,
    failed: int,
) -> None:
    """Mark a run as finished and record its counters."""
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
        (utc_now_iso(), total_companies, processed, succeeded, failed, run_id),
    )
    conn.commit()


def insert_company_row(
    conn: sqlite3.Connection,
    payload: dict[str, Any],
) -> None:
    """Insert a single company_reports row from a flat payload dict."""
    conn.execute(
        """
        INSERT INTO company_reports (
            run_id,
            source_row_index,
            group_uid,
            group_id,
            group_name,
            company_number,
            company_name,
            status,
            document_id,
            pdf_path,
            profile_json_path,
            filing_history_json_path,
            extraction_json_path,
            warnings_json_path,
            profile_json,
            filing_history_json,
            extraction_json,
            warnings_json,
            model_used,
            pdf_size_bytes,
            approx_llm_tokens,
            error_message,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["run_id"],
            payload["source_row_index"],
            payload.get("group_uid"),
            payload.get("group_id"),
            payload.get("group_name"),
            payload["company_number"],
            payload.get("company_name"),
            payload["status"],
            payload.get("document_id"),
            payload.get("pdf_path"),
            payload.get("profile_json_path"),
            payload.get("filing_history_json_path"),
            payload.get("extraction_json_path"),
            payload.get("warnings_json_path"),
            payload.get("profile_json"),
            payload.get("filing_history_json"),
            payload.get("extraction_json"),
            payload.get("warnings_json"),
            payload.get("model_used"),
            payload.get("pdf_size_bytes"),
            payload.get("approx_llm_tokens"),
            payload.get("error_message"),
            utc_now_iso(),
        ),
    )
    conn.commit()
