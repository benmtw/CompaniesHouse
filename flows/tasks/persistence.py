"""Tasks for persisting extraction results to JSON files and SQLite."""

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import task


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


@task(name="save-results", retries=0)
def save_results(
    run_id: int,
    item: dict,
    company_number: str,
    profile: dict,
    filing_history: list[dict],
    extraction_payload: dict,
    warnings_payload: list[str],
    model_used: str,
    model_requested: str,
    pdf_path: str,
    pdf_size_bytes: int,
    approx_llm_tokens: int,
    schema_profile: str,
    extraction_types: list,
    had_schema_depth_error: bool,
    output_run_dir: str,
    db_path: str,
) -> dict:
    """Write JSON files and insert SQLite row for a successful company."""
    company_dir = Path(output_run_dir) / company_number
    api_dir = company_dir / "api"
    extraction_dir = company_dir / "extraction"
    api_dir.mkdir(parents=True, exist_ok=True)
    extraction_dir.mkdir(parents=True, exist_ok=True)

    profile_path = api_dir / "profile.json"
    filing_history_path = api_dir / "filing_history.json"
    extraction_path = extraction_dir / "extraction_result.json"
    warnings_path = extraction_dir / "validation_warnings.json"
    run_report_path = extraction_dir / "run_report.json"

    document_id = Path(pdf_path).stem.split("_")[-1] if pdf_path else None

    run_report = {
        "run_id": run_id,
        "source_row_index": item["source_row_index"],
        "group_uid": item.get("group_uid"),
        "group_id": item.get("group_id"),
        "group_name": item.get("group_name"),
        "company_number": company_number,
        "company_name": profile.get("company_name"),
        "document_id": document_id,
        "pdf_path": pdf_path,
        "pdf_size_bytes": pdf_size_bytes,
        "approx_llm_tokens": approx_llm_tokens,
        "model": model_requested,
        "model_used": model_used,
        "schema_profile": schema_profile,
        "requested_types": [
            t.value if hasattr(t, "value") else str(t) for t in extraction_types
        ],
        "schema_profile_fallback_applied": had_schema_depth_error,
        "extraction_result": extraction_payload,
    }

    write_json(profile_path, profile)
    write_json(filing_history_path, filing_history)
    write_json(extraction_path, extraction_payload)
    write_json(warnings_path, warnings_payload)
    write_json(run_report_path, run_report)

    conn = sqlite3.connect(db_path, timeout=10)
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
            run_id,
            item["source_row_index"],
            item.get("group_uid"),
            item.get("group_id"),
            item.get("group_name"),
            company_number,
            profile.get("company_name"),
            "success",
            document_id,
            pdf_path,
            str(profile_path),
            str(filing_history_path),
            str(extraction_path),
            str(warnings_path),
            json.dumps(profile),
            json.dumps(filing_history),
            json.dumps(extraction_payload),
            json.dumps(warnings_payload),
            model_used,
            pdf_size_bytes,
            approx_llm_tokens,
            None,
            _utc_now(),
        ),
    )
    conn.commit()
    conn.close()

    return {
        "company_number": company_number,
        "company_name": profile.get("company_name"),
        "status": "success",
        "document_id": document_id,
        "pdf_path": pdf_path,
        "model_used": model_used,
    }
