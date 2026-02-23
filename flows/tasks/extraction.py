"""Tasks for document extraction via LLM."""

from typing import Any

from prefect import task
from prefect.concurrency.sync import rate_limit

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from document_extraction_models import ExtractionType
from openrouter_document_extractor import DocumentExtractionError
from name_enrichment import enrich_personnel_names
from shared import (
    extract_with_model_fallback,
    extraction_types_for_schema_profile,
    get_cached_extraction,
    is_schema_depth_error,
    latest_full_accounts_document_id,
)


@task(name="find-latest-full-accounts", retries=0)
def find_latest_full_accounts(filings: list[dict]) -> str:
    """Identify the document_id for the most recent full accounts filing.

    Raises ValueError if none found.
    """
    document_id = latest_full_accounts_document_id(filings)
    if not document_id:
        raise ValueError("No full accounts document found")
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
    document_id: str,
    db_path: str,
    extraction_types: list[ExtractionType],
    retries_on_invalid_json: int = 2,
    schema_profile: str = "compact_single_call",
    company_type: CompanyType = CompanyType.GENERIC,
    enrich_names: bool = True,
    company_name: str = "",
) -> tuple[dict, list[str], str, bool]:
    """Run LLM extraction with caching and model fallback + schema fallback.

    Returns (extraction_payload, warnings, model_used, cache_hit).
    Cache hit is True if the result was retrieved from a previous successful extraction.
    """
    # Check cache first - documents never change, so cached extractions are always valid
    cached = get_cached_extraction(db_path, document_id)
    if cached is not None:
        return (
            cached["extraction_payload"],
            cached["warnings"],
            cached["model_used"],
            True,  # cache_hit
        )

    rate_limit("openrouter-llm")
    try:
        payload, warnings, model_used = extract_with_model_fallback(
            api_key=openrouter_api_key,
            model_candidates=model_candidates,
            document_path=document_path,
            extraction_types=extraction_types,
            retries_on_invalid_json=retries_on_invalid_json,
            company_type=company_type,
        )
    except DocumentExtractionError as exc:
        if schema_profile == "compact_single_call" and is_schema_depth_error(exc):
            fallback_types = extraction_types_for_schema_profile(
                "light_core", company_type
            )
            payload, warnings, model_used = extract_with_model_fallback(
                api_key=openrouter_api_key,
                model_candidates=model_candidates,
                document_path=document_path,
                extraction_types=fallback_types,
                retries_on_invalid_json=retries_on_invalid_json,
                company_type=company_type,
            )
        else:
            raise

    if enrich_names and company_name:
        pd_list = payload.get("personnel_details")
        if pd_list:
            payload["personnel_details"] = enrich_personnel_names(
                personnel=pd_list,
                company_name=company_name,
            )

    return payload, warnings, model_used, False  # cache_hit
