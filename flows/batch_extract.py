"""Prefect flows for batch extraction of Companies House filings.

Phase 1: Sequential processing with Prefect observability.
Keeps existing rate limiting and SQLite persistence unchanged.
"""

import json
import math
import os
import sqlite3
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from prefect import flow

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from document_extraction_models import ExtractionType
from flows.tasks.companies_house import (
    download_document,
    fetch_company_profile,
    fetch_filing_history,
)
from flows.tasks.data_loading import finalize_run, initialize_run, load_and_prepare_batch
from flows.tasks.extraction import extract_document, find_latest_full_accounts
from flows.tasks.persistence import save_results, write_json


DEFAULT_INPUT_XLSX = "SourceData/allgroupslinksdata20260217/Trusts.xlsx"
DEFAULT_OUTPUT_ROOT = "output/companies_extraction"
MAX_ERROR_TRACEBACK_CHARS = 4000


def _extraction_types_for_schema_profile(
    schema_profile: str,
    company_type: CompanyType = CompanyType.GENERIC,
) -> list[ExtractionType]:
    from batch_extract_companies import _extraction_types_for_schema_profile as _orig

    return _orig(schema_profile, company_type)


def _derive_annual_report_from_component_sections(
    extraction_payload: dict[str, Any],
) -> dict[str, Any] | None:
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


def _estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes: int) -> int:
    if pdf_size_bytes <= 0:
        return 0
    return int(math.ceil(pdf_size_bytes / 4.0))


def _parse_fallback_models(raw_models: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_models.split(","):
        model = part.strip()
        if not model or model in seen:
            continue
        seen.add(model)
        out.append(model)
    return out


def _install_companies_house_request_throttle(
    client: CompaniesHouseClient, min_interval_seconds: float
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "enabled": min_interval_seconds > 0,
        "min_interval_seconds": min_interval_seconds,
        "request_count": 0,
    }
    if min_interval_seconds <= 0:
        return state

    original_request = client.session.request
    request_state = {"last_request_ts": 0.0}

    def throttled_request(*args: Any, **kwargs: Any) -> Any:
        now = time.monotonic()
        elapsed = now - request_state["last_request_ts"]
        wait = min_interval_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        request_state["last_request_ts"] = time.monotonic()
        state["request_count"] += 1
        return original_request(*args, **kwargs)

    client.session.request = throttled_request
    return state


def _insert_failed_company_row(
    db_path: str,
    run_id: int,
    item: dict[str, Any],
    company_number: str,
    error_message: str,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO company_reports (
            run_id, source_row_index, group_uid, group_id, group_name,
            company_number, company_name, status,
            document_id, pdf_path, profile_json_path, filing_history_json_path,
            extraction_json_path, warnings_json_path,
            profile_json, filing_history_json, extraction_json, warnings_json,
            model_used, pdf_size_bytes, approx_llm_tokens,
            error_message, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            item["source_row_index"],
            item.get("group_uid"),
            item.get("group_id"),
            item.get("group_name"),
            company_number,
            None,
            "failed",
            None, None, None, None, None, None,
            None, None, None, None,
            None, None, None,
            error_message,
            datetime.now(UTC).replace(microsecond=0).isoformat(),
        ),
    )
    conn.commit()
    conn.close()


@flow(
    name="process-company",
    retries=0,
    log_prints=True,
)
def process_company_flow(
    client: CompaniesHouseClient,
    openrouter_api_key: str,
    model_candidates: list[str],
    model_requested: str,
    extraction_types: list[ExtractionType],
    company_type: CompanyType,
    item: dict,
    run_id: int,
    output_run_dir: str,
    db_path: str,
    schema_profile: str,
    retries_on_invalid_json: int,
    filing_history_items_per_page: int,
) -> dict:
    """Process a single company: fetch, download, extract, save."""
    company_number = item["company_number"]

    profile = fetch_company_profile(client, company_number)
    filings = fetch_filing_history(client, company_number, filing_history_items_per_page)
    document_id = find_latest_full_accounts(filings)

    doc_dir = Path(output_run_dir) / company_number / "documents"
    doc_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = str(
        doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
    )
    downloaded_path = download_document(client, document_id, pdf_path)

    pdf_size_bytes = Path(downloaded_path).stat().st_size
    approx_llm_tokens = _estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes)

    extraction_payload, warnings, model_used = extract_document(
        openrouter_api_key=openrouter_api_key,
        model_candidates=model_candidates,
        document_path=downloaded_path,
        extraction_types=extraction_types,
        retries_on_invalid_json=retries_on_invalid_json,
        schema_profile=schema_profile,
        company_type=company_type,
    )

    annual_report_key = (
        "academy_trust_annual_report"
        if company_type == CompanyType.ACADEMY_TRUST
        else "annual_report"
    )
    if extraction_payload.get(annual_report_key) is None:
        derived = _derive_annual_report_from_component_sections(extraction_payload)
        if derived is not None:
            extraction_payload[annual_report_key] = derived

    result = save_results(
        run_id=run_id,
        item=item,
        company_number=company_number,
        profile=profile,
        filing_history=filings,
        extraction_payload=extraction_payload,
        warnings_payload=warnings,
        model_used=model_used,
        model_requested=model_requested,
        pdf_path=pdf_path,
        pdf_size_bytes=pdf_size_bytes,
        approx_llm_tokens=approx_llm_tokens,
        schema_profile=schema_profile,
        extraction_types=extraction_types,
        had_schema_depth_error=False,
        output_run_dir=output_run_dir,
        db_path=db_path,
    )
    return result


@flow(
    name="batch-extract-companies",
    log_prints=True,
)
def batch_extract_companies_flow(
    input_xlsx: str = DEFAULT_INPUT_XLSX,
    output_root: str = DEFAULT_OUTPUT_ROOT,
    model: str = "",
    max_companies: int = 0,
    start_index: int = 0,
    schema_profile: str = "compact_single_call",
    company_type: str = "generic",
    db_path: str = "",
    fallback_models: str = "",
    ch_min_request_interval_seconds: float = 2.0,
    write_summary_json: bool = False,
    summary_json_path: str = "",
    filing_history_items_per_page: int = 100,
    retries_on_invalid_json: int = 2,
    random_sample_size: int = 0,
    random_seed: int = 0,
) -> dict:
    """Top-level Prefect flow for batch extraction of Companies House filings.

    Orchestrates: load batch -> initialize run -> process each company -> finalize.
    """
    # Load .env for API keys if not already in environment
    from batch_extract_companies import _load_dotenv_file

    _load_dotenv_file(Path(".env"))

    # Resolve API keys
    ch_api_key = os.getenv("CH_API_KEY")
    if not ch_api_key:
        raise ValueError("Missing CH_API_KEY. Set in environment or .env.")
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
    if not openrouter_api_key:
        raise ValueError("Missing OPENROUTER_API_KEY. Set in environment or .env.")

    resolved_model = model or os.getenv("OPENROUTER_MODEL", "").strip()
    if not resolved_model:
        raise ValueError(
            "Missing model. Set OPENROUTER_MODEL in .env or pass model parameter."
        )

    ct = CompanyType(company_type)
    extraction_types = _extraction_types_for_schema_profile(schema_profile, ct)

    # Build model candidates list
    model_candidates = [resolved_model] + _parse_fallback_models(fallback_models)
    deduped_models: list[str] = []
    seen_models: set[str] = set()
    for m in model_candidates:
        if m in seen_models:
            continue
        seen_models.add(m)
        deduped_models.append(m)
    model_candidates = deduped_models

    # Load and prepare batch
    batch = load_and_prepare_batch(
        input_xlsx=input_xlsx,
        start_index=start_index,
        max_companies=max_companies,
        random_sample_size=random_sample_size,
        random_seed=random_seed,
    )
    total_companies = len(batch)

    # Initialize run
    run_context = initialize_run(
        input_xlsx_path=input_xlsx,
        output_root=output_root,
        model=resolved_model,
        extraction_types=extraction_types,
        db_path=db_path,
    )
    run_id = run_context["run_id"]
    output_run_dir = run_context["output_run_dir"]
    resolved_db_path = run_context["db_path"]

    print(f"[run {run_id}] output_dir={output_run_dir}")
    print(f"[run {run_id}] db_path={resolved_db_path}")
    print(f"[run {run_id}] companies_to_process={total_companies}")
    print(f"[run {run_id}] models={model_candidates}")
    print(f"[run {run_id}] schema_profile={schema_profile}")
    print(f"[run {run_id}] company_type={ct.value}")

    # Create CH client with throttle
    client = CompaniesHouseClient(api_key=ch_api_key)
    throttle_state = _install_companies_house_request_throttle(
        client=client,
        min_interval_seconds=ch_min_request_interval_seconds,
    )

    # Process companies sequentially (Phase 1)
    processed = 0
    succeeded = 0
    failed = 0
    company_summaries: list[dict[str, Any]] = []

    for item in batch:
        processed += 1
        company_number = item["company_number"]
        prefix = f"[run {run_id}] [{processed}/{total_companies}] {company_number}"
        print(f"{prefix} start")

        try:
            result = process_company_flow(
                client=client,
                openrouter_api_key=openrouter_api_key,
                model_candidates=model_candidates,
                model_requested=resolved_model,
                extraction_types=extraction_types,
                company_type=ct,
                item=item,
                run_id=run_id,
                output_run_dir=output_run_dir,
                db_path=resolved_db_path,
                schema_profile=schema_profile,
                retries_on_invalid_json=retries_on_invalid_json,
                filing_history_items_per_page=filing_history_items_per_page,
            )
            succeeded += 1
            company_summaries.append(
                {
                    "source_row_index": item["source_row_index"],
                    "group_uid": item.get("group_uid"),
                    "group_id": item.get("group_id"),
                    "group_name": item.get("group_name"),
                    "company_number": company_number,
                    "status": "success",
                    "company_name": result.get("company_name"),
                    "document_id": result.get("document_id"),
                    "pdf_path": result.get("pdf_path"),
                    "model_used": result.get("model_used"),
                    "error": None,
                }
            )
            print(
                f"{prefix} success document_id={result.get('document_id')} "
                f"model={result.get('model_used')}"
            )
        except Exception as exc:
            failed += 1
            error_message = "".join(
                traceback.format_exception(exc.__class__, exc, exc.__traceback__)
            )[:MAX_ERROR_TRACEBACK_CHARS]
            _insert_failed_company_row(
                db_path=resolved_db_path,
                run_id=run_id,
                item=item,
                company_number=company_number,
                error_message=error_message,
            )
            company_summaries.append(
                {
                    "source_row_index": item["source_row_index"],
                    "group_uid": item.get("group_uid"),
                    "group_id": item.get("group_id"),
                    "group_name": item.get("group_name"),
                    "company_number": company_number,
                    "status": "failed",
                    "error": str(exc),
                }
            )
            print(f"{prefix} failed error={exc}")

    # Finalize run
    finalize_run(
        db_path=resolved_db_path,
        run_id=run_id,
        total_companies=total_companies,
        processed=processed,
        succeeded=succeeded,
        failed=failed,
    )

    # Write summary JSON if requested
    if write_summary_json or summary_json_path:
        summary_path = (
            Path(summary_json_path)
            if summary_json_path
            else Path(output_run_dir) / "summary.json"
        )
        summary_payload = {
            "run_type": "batch_extract_companies",
            "company_type": ct.value,
            "run_id": run_id,
            "timestamp_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "input_xlsx_path": input_xlsx,
            "output_run_dir": output_run_dir,
            "model": resolved_model,
            "model_candidates": model_candidates,
            "schema_profile": schema_profile,
            "requested_types": [t.value for t in extraction_types],
            "companies_house_min_request_interval_seconds": ch_min_request_interval_seconds,
            "estimated_companies_house_rate_requests_per_second": (
                (1.0 / ch_min_request_interval_seconds)
                if ch_min_request_interval_seconds > 0
                else None
            ),
            "companies_house_request_count": throttle_state["request_count"],
            "total_companies": total_companies,
            "processed": processed,
            "succeeded": succeeded,
            "failed": failed,
            "companies": company_summaries,
        }
        write_json(summary_path, summary_payload)
        print(f"[run {run_id}] summary_json_path={summary_path}")

    print(
        f"[run {run_id}] complete processed={processed} "
        f"succeeded={succeeded} failed={failed}"
    )

    return {
        "run_id": run_id,
        "total_companies": total_companies,
        "processed": processed,
        "succeeded": succeeded,
        "failed": failed,
    }
