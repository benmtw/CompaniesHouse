"""Tasks for document extraction via LLM."""

from typing import Any

from prefect import task

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from document_extraction_models import ExtractionType
from openrouter_document_extractor import DocumentExtractionError, OpenRouterDocumentExtractor


def _is_file_not_supported_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not support file content types" in message
        or "invalid value: 'file'" in message
        or "messages[1].content[1].type" in message
    )


def _is_invalid_json_error(exc: Exception) -> bool:
    return "response was not valid json" in str(exc).lower()


def _is_schema_depth_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "maximum allowed nesting depth" in message or "schema-depth" in message


def _is_full_accounts_filing(item: dict[str, Any]) -> bool:
    filing_type = str(item.get("type") or "").upper()
    description = str(item.get("description") or "").lower()
    if filing_type != "AA":
        return False
    return "accounts-with-accounts-type-full" in description or "full" in description


def _extract_with_model_fallback(
    api_key: str,
    model_candidates: list[str],
    document_path: str,
    extraction_types: list[ExtractionType],
    retries_on_invalid_json: int = 2,
    company_type: CompanyType = CompanyType.GENERIC,
) -> tuple[dict[str, Any], list[str], str]:
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
                if _is_file_not_supported_error(exc):
                    errors.append(f"{model}: {exc}")
                    break
                if _is_invalid_json_error(exc) and attempt < attempts:
                    errors.append(f"{model} attempt {attempt}/{attempts}: {exc}")
                    continue
                raise
    if errors:
        raise DocumentExtractionError(
            "All configured models failed file-input support checks: "
            + " | ".join(errors)
        )
    raise DocumentExtractionError("No models configured for extraction")


@task(name="find-latest-full-accounts", retries=0)
def find_latest_full_accounts(filings: list[dict]) -> str:
    """Identify the document_id for the most recent full accounts filing.

    Raises ValueError if none found.
    """
    candidates: list[dict[str, Any]] = []
    for item in filings:
        if not _is_full_accounts_filing(item):
            continue
        links = item.get("links") or {}
        metadata_url = str(links.get("document_metadata") or "").strip()
        if not metadata_url:
            continue
        candidates.append(item)

    if not candidates:
        raise ValueError("No full accounts document found")

    latest_item = sorted(
        candidates, key=lambda i: (str(i.get("date") or "")), reverse=True
    )[0]
    metadata_url = str(
        (latest_item.get("links") or {}).get("document_metadata") or ""
    ).strip()
    document_id = CompaniesHouseClient._extract_document_id(metadata_url)
    if not document_id:
        raise ValueError("Could not extract document_id from metadata URL")
    return document_id


@task(
    name="extract-document",
    retries=1,
    retry_delay_seconds=30,
    tags=["openrouter-llm"],
    timeout_seconds=300,
)
def extract_document(
    openrouter_api_key: str,
    model_candidates: list[str],
    document_path: str,
    extraction_types: list[ExtractionType],
    retries_on_invalid_json: int = 2,
    schema_profile: str = "compact_single_call",
    company_type: CompanyType = CompanyType.GENERIC,
) -> tuple[dict, list[str], str]:
    """Run LLM extraction with model fallback + schema fallback.

    Returns (extraction_payload, warnings, model_used).
    """
    from batch_extract_companies import _extraction_types_for_schema_profile

    try:
        payload, warnings, model_used = _extract_with_model_fallback(
            api_key=openrouter_api_key,
            model_candidates=model_candidates,
            document_path=document_path,
            extraction_types=extraction_types,
            retries_on_invalid_json=retries_on_invalid_json,
            company_type=company_type,
        )
        return payload, warnings, model_used
    except DocumentExtractionError as exc:
        if schema_profile == "compact_single_call" and _is_schema_depth_error(exc):
            fallback_types = _extraction_types_for_schema_profile(
                "light_core", company_type
            )
            payload, warnings, model_used = _extract_with_model_fallback(
                api_key=openrouter_api_key,
                model_candidates=model_candidates,
                document_path=document_path,
                extraction_types=fallback_types,
                retries_on_invalid_json=retries_on_invalid_json,
                company_type=company_type,
            )
            return payload, warnings, model_used
        raise
