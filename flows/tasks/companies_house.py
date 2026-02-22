"""Tasks for interacting with the Companies House API."""

from prefect import task

from companies_house_client import CompaniesHouseClient


@task(
    name="fetch-company-profile",
    retries=2,
    retry_delay_seconds=15,
    tags=["companies-house-api"],
)
def fetch_company_profile(client: CompaniesHouseClient, company_number: str) -> dict:
    """GET /company/{number} -- Companies House profile."""
    return client.get_company_profile(company_number)


@task(
    name="fetch-filing-history",
    retries=2,
    retry_delay_seconds=15,
    tags=["companies-house-api"],
)
def fetch_filing_history(
    client: CompaniesHouseClient,
    company_number: str,
    items_per_page: int = 100,
) -> list[dict]:
    """GET /company/{number}/filing-history."""
    page = client.get_filing_history(
        company_number=company_number,
        items_per_page=items_per_page,
        start_index=0,
    )
    return page.get("items") or []


@task(
    name="download-document",
    retries=2,
    retry_delay_seconds=15,
    tags=["companies-house-api"],
)
def download_document(
    client: CompaniesHouseClient,
    document_id: str,
    output_path: str,
) -> str:
    """Download a PDF filing document to disk."""
    return client.download_document(
        document_id=document_id,
        output_path=output_path,
        accept="application/pdf",
    )
