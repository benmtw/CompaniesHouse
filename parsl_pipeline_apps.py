"""
Parsl app definitions for Companies House extraction pipeline.

Defines @python_app for download and extract stages, and @join_app for mode switching.

NOTE: Parsl requires fork-based multiprocessing and is NOT compatible with Windows.
"""

from __future__ import annotations

import platform
import sys
import traceback
from typing import Any

# Windows compatibility check
if platform.system() == "Windows":
    raise ImportError(
        "Parsl is not compatible with Windows due to fork-based multiprocessing. "
        "Use WSL2, Linux, or macOS. Alternatively, use the legacy pipeline: "
        "companies_house_full_reports_extraction_pipeline_legacy.py"
    )

from parsl import join_app, python_app

MAX_ERROR_TRACEBACK_CHARS = 4000


@python_app(executors=["download_executor"], cache=False, max_retries=3)
def download_company_document(
    company_number: str,
    job_index: int,
    run_dir: str,
    ch_api_key: str,
    filing_history_items_per_page: int,
    cache_dir: str,
    throttle_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Download company profile, filing history, and latest full accounts PDF.

    Args:
        company_number: 8-digit Companies House company number
        job_index: Index of this job in the batch (1-based)
        run_dir: Root directory for this pipeline run
        ch_api_key: Companies House API key
        filing_history_items_per_page: Number of filing history items to fetch
        cache_dir: Directory for document caching
        throttle_config: Dict with min_interval_seconds for rate limiting

    Returns:
        DownloadResult dict with keys:
        - success: bool
        - company_number: str
        - job_index: int
        - pdf_path: str | None
        - document_id: str | None
        - company_name: str | None
        - cache_hit: bool
        - pdf_size_bytes: int | None
        - error: str | None
    """
    from pathlib import Path

    from companies_house_client import CompaniesHouseClient
    from pipeline_shared import (
        install_request_throttle,
        latest_full_accounts_document_id_from_filing_history,
        write_json,
    )

    client = CompaniesHouseClient(api_key=ch_api_key, cache_dir=cache_dir)
    install_request_throttle(
        client=client,
        min_interval_seconds=throttle_config.get("min_interval_seconds", 2.0),
        shared_state_path=str(throttle_config.get("shared_state_path") or "").strip() or None,
        shared_lock_path=str(throttle_config.get("shared_lock_path") or "").strip() or None,
    )

    run_dir_path = Path(run_dir)
    company_dir = run_dir_path / company_number
    api_dir = company_dir / "api"
    doc_dir = company_dir / "documents"
    api_dir.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    try:
        profile = client.get_company_profile(company_number)
        filing_page = client.get_filing_history(
            company_number=company_number,
            items_per_page=filing_history_items_per_page,
            start_index=0,
        )
        filings = filing_page.get("items") or []

        write_json(api_dir / "profile.json", profile)
        write_json(api_dir / "filing_history.json", filings)

        document_id = latest_full_accounts_document_id_from_filing_history(filings)
        if not document_id:
            return {
                "success": False,
                "company_number": company_number,
                "job_index": job_index,
                "pdf_path": None,
                "document_id": None,
                "company_name": profile.get("company_name"),
                "cache_hit": False,
                "pdf_size_bytes": None,
                "error": "No full accounts document found",
            }

        pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
        client.download_document(
            document_id=document_id,
            output_path=str(pdf_path),
            accept="application/pdf",
            company_number=company_number,
        )

        return {
            "success": True,
            "company_number": company_number,
            "job_index": job_index,
            "pdf_path": str(pdf_path),
            "document_id": document_id,
            "company_name": profile.get("company_name"),
            "cache_hit": bool(client.last_download_cache_hit),
            "pdf_size_bytes": pdf_path.stat().st_size,
            "error": None,
        }
    except Exception as exc:
        return {
            "success": False,
            "company_number": company_number,
            "job_index": job_index,
            "pdf_path": None,
            "document_id": None,
            "company_name": None,
            "cache_hit": False,
            "pdf_size_bytes": None,
            "error": str(exc),
        }


@python_app(executors=["extract_executor"], cache=False)
def extract_document(
    download_result: dict[str, Any],
    run_dir: str,
    openrouter_api_key: str,
    model_candidates: list[str],
    extraction_types: list[str],
    retries_on_invalid_json: int,
    openrouter_timeout_seconds: float,
    write_openrouter_debug_artifacts: bool,
    cache_dir: str,
    ch_api_key: str | None,
    filing_history_items_per_page: int,
    throttle_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Extract structured data from a downloaded PDF using OpenRouter LLM.

    Args:
        download_result: Result from download_company_document (can be Future - Parsl resolves)
        run_dir: Root directory for this pipeline run
        openrouter_api_key: OpenRouter API key
        model_candidates: List of model names to try in fallback order
        extraction_types: List of ExtractionType values to extract
        retries_on_invalid_json: Number of retries on invalid JSON response
        openrouter_timeout_seconds: Timeout for OpenRouter API calls
        write_openrouter_debug_artifacts: Whether to write debug artifacts
        cache_dir: Directory for document caching
        ch_api_key: Companies House API key (for extract-only fallback downloads)
        filing_history_items_per_page: Items per page for filing history fetch
        throttle_config: Throttle config for fallback downloads

    Returns:
        Combined result dict with download fields plus:
        - extract_success: bool
        - extraction_payload: dict | None
        - model_used: str | None
        - extraction_json_path: str | None
        - warnings_json_path: str | None
        - extract_error: str | None
    """
    from pathlib import Path
    import shutil

    from companies_house_client import CompaniesHouseClient
    from document_extraction_models import ExtractionType
    from pipeline_shared import (
        extract_with_model_fallback,
        derive_annual_report_from_component_sections,
        write_json,
        resolve_cached_pdf_for_company,
        install_request_throttle,
        latest_full_accounts_document_id_from_filing_history,
    )

    # If download failed, propagate the failure
    if not download_result.get("success"):
        return {
            **download_result,
            "extract_success": False,
            "extraction_payload": None,
            "model_used": None,
            "extract_error": download_result.get("error", "Download failed"),
            "extraction_json_path": None,
            "warnings_json_path": None,
        }

    company_number = download_result["company_number"]
    job_index = download_result["job_index"]
    pdf_path = download_result.get("pdf_path")
    document_id = download_result.get("document_id")

    run_dir_path = Path(run_dir)
    cache_dir_path = Path(cache_dir)
    resolved_pdf_path: Path | None = None

    # Resolve PDF path
    if pdf_path and Path(pdf_path).is_file():
        resolved_pdf_path = Path(pdf_path)

    if resolved_pdf_path is None:
        # Try cache index lookup
        cached_path, cached_doc_id = resolve_cached_pdf_for_company(
            company_number=company_number,
            cache_dir=cache_dir_path,
            accept="application/pdf",
        )
        if cached_path:
            # Materialize cached PDF into run directory
            doc_id = cached_doc_id or "cache_unknown_docid"
            target = (
                run_dir_path
                / company_number
                / "documents"
                / f"{company_number}_latest_full_accounts_{doc_id}.pdf"
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists() or target.stat().st_size == 0:
                shutil.copy2(cached_path, target)
            resolved_pdf_path = target
            if cached_doc_id:
                document_id = cached_doc_id

    if resolved_pdf_path is None and ch_api_key:
        # Fall back to downloading
        client = CompaniesHouseClient(api_key=ch_api_key, cache_dir=cache_dir)
        if throttle_config:
            install_request_throttle(
                client=client,
                min_interval_seconds=throttle_config.get("min_interval_seconds", 2.0),
                shared_state_path=str(throttle_config.get("shared_state_path") or "").strip() or None,
                shared_lock_path=str(throttle_config.get("shared_lock_path") or "").strip() or None,
            )

        company_dir = run_dir_path / company_number
        api_dir = company_dir / "api"
        doc_dir = company_dir / "documents"
        api_dir.mkdir(parents=True, exist_ok=True)
        doc_dir.mkdir(parents=True, exist_ok=True)

        try:
            profile = client.get_company_profile(company_number)
            filing_page = client.get_filing_history(
                company_number=company_number,
                items_per_page=filing_history_items_per_page,
                start_index=0,
            )
            filings = filing_page.get("items") or []
            write_json(api_dir / "profile.json", profile)
            write_json(api_dir / "filing_history.json", filings)

            doc_id = latest_full_accounts_document_id_from_filing_history(filings)
            if doc_id:
                pdf_target = doc_dir / f"{company_number}_latest_full_accounts_{doc_id}.pdf"
                client.download_document(
                    document_id=doc_id,
                    output_path=str(pdf_target),
                    accept="application/pdf",
                    company_number=company_number,
                )
                resolved_pdf_path = pdf_target
                document_id = doc_id
        except Exception:
            pass  # Will be caught by the PDF not found check below

    if resolved_pdf_path is None:
        return {
            **download_result,
            "extract_success": False,
            "extraction_payload": None,
            "model_used": None,
            "extract_error": "PDF not found and cache lookup failed",
            "extraction_json_path": None,
            "warnings_json_path": None,
        }

    # Convert extraction type strings to enums
    extraction_type_enums = [ExtractionType(t) for t in extraction_types]

    extraction_dir = run_dir_path / company_number / "extraction"
    extraction_dir.mkdir(parents=True, exist_ok=True)

    openrouter_debug_dir = (
        extraction_dir / "openrouter_debug"
        if write_openrouter_debug_artifacts
        else None
    )

    try:
        extraction_payload, warnings_payload, model_used = extract_with_model_fallback(
            api_key=openrouter_api_key,
            model_candidates=model_candidates,
            document_path=str(resolved_pdf_path),
            extraction_types=extraction_type_enums,
            retries_on_invalid_json=retries_on_invalid_json,
            openrouter_timeout_seconds=openrouter_timeout_seconds,
            openrouter_debug_dir=openrouter_debug_dir,
        )

        if extraction_payload.get("academy_trust_annual_report") is None:
            derived = derive_annual_report_from_component_sections(extraction_payload)
            if derived is not None:
                extraction_payload["academy_trust_annual_report"] = derived

        extraction_path = extraction_dir / "extraction_result.json"
        warnings_path = extraction_dir / "validation_warnings.json"
        write_json(extraction_path, extraction_payload)
        write_json(warnings_path, warnings_payload)

        return {
            **download_result,
            "extract_success": True,
            "extraction_payload": extraction_payload,
            "model_used": model_used,
            "extraction_json_path": str(extraction_path),
            "warnings_json_path": str(warnings_path),
            "extract_error": None,
        }

    except Exception as exc:
        error_text = "".join(
            traceback.format_exception(exc.__class__, exc, exc.__traceback__)
        )[:MAX_ERROR_TRACEBACK_CHARS]
        return {
            **download_result,
            "extract_success": False,
            "extraction_payload": None,
            "model_used": None,
            "extract_error": error_text,
            "extraction_json_path": None,
            "warnings_json_path": None,
        }


@join_app
def process_company(
    company_number: str,
    job_index: int,
    mode: str,
    download_config: dict[str, Any],
    extract_config: dict[str, Any],
) -> Any:
    """
    Join app that conditionally chains download -> extract based on mode.

    Args:
        company_number: 8-digit Companies House company number
        job_index: Index of this job in the batch (1-based)
        mode: Pipeline mode - 'all', 'download', or 'extract'
        download_config: Dict of kwargs for download_company_document
        extract_config: Dict of kwargs for extract_document

    Returns:
        For 'download' mode: download_company_document result
        For 'extract' mode: extract_document result (with synthetic download result)
        For 'all' mode: extract_document result (chained from download)
    """
    if mode == "download":
        return download_company_document(
            company_number=company_number,
            job_index=job_index,
            **download_config,
        )

    if mode == "extract":
        # Synthetic result triggers cache lookup in extract_app
        synthetic_result = {
            "success": True,
            "company_number": company_number,
            "job_index": job_index,
            "pdf_path": None,
            "document_id": None,
            "company_name": None,
            "cache_hit": False,
            "pdf_size_bytes": None,
            "error": None,
        }
        return extract_document(
            download_result=synthetic_result,
            **extract_config,
        )

    # mode == 'all': chain download -> extract
    download_future = download_company_document(
        company_number=company_number,
        job_index=job_index,
        **download_config,
    )
    return extract_document(
        download_result=download_future,  # Parsl resolves Future automatically
        **extract_config,
    )


__all__ = [
    "download_company_document",
    "extract_document",
    "process_company",
]
