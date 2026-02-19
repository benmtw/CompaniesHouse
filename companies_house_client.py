import base64
import contextlib
import json
import os
import re
import shutil
import time
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from document_extraction_models import (
    AcademyTrustAnnualReport,
    BalanceSheetEntry,
    DetailedBalanceSheet,
    ExtractionResult,
    ExtractionType,
    Governance,
    Metadata,
    PersonnelDetail,
    StaffingData,
    StatementOfFinancialActivities,
)
from openrouter_document_extractor import DocumentExtractionError, OpenRouterDocumentExtractor


class CompaniesHouseApiError(Exception):
    """Raised when a Companies House API request fails."""

    def __init__(
        self,
        status_code: int | None,
        url: str,
        message: str,
        response_body: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.message = message
        self.response_body = response_body
        super().__init__(self.__str__())

    def __str__(self) -> str:
        parts = [f"CompaniesHouseApiError: {self.message}", f"url={self.url}"]
        if self.status_code is not None:
            parts.append(f"status_code={self.status_code}")
        if self.response_body:
            parts.append(f"response_body={self.response_body[:300]}")
        return " | ".join(parts)


class FilingDocumentType(Enum):
    """User-friendly filing document categories for filtering."""

    CONFIRMATION_STATEMENT = "Confirmation statement"
    FULL_ACCOUNTS = "Full accounts"
    APPOINTMENT = "Appointment"
    TERMINATION_OF_APPOINTMENT = "Termination of appointment"
    DIRECTOR_DETAILS_CHANGED = "Director's details changed"
    CURRENT_ACCOUNTING_PERIOD_SHORTENED = "Current accounting period shortened"
    REGISTERED_OFFICE_ADDRESS_CHANGED = "Registered office address changed"
    INCORPORATION = "Incorporation"


class CompaniesHouseClient:
    """Convenience client for Companies House Public Data and Document APIs."""

    _FRIENDLY_TYPE_CODE_SETS: dict[FilingDocumentType, set[str]] = {
        FilingDocumentType.APPOINTMENT: {"AP01", "AP03"},
        FilingDocumentType.TERMINATION_OF_APPOINTMENT: {"TM01", "TM02"},
        FilingDocumentType.DIRECTOR_DETAILS_CHANGED: {"CH01"},
        FilingDocumentType.CURRENT_ACCOUNTING_PERIOD_SHORTENED: {"AA01"},
        FilingDocumentType.REGISTERED_OFFICE_ADDRESS_CHANGED: {"AD01"},
        FilingDocumentType.INCORPORATION: {"NEWINC"},
    }

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = 30.0,
        public_base_url: str = "https://api.company-information.service.gov.uk",
        document_base_url: str = "https://document-api.company-information.service.gov.uk",
        max_retries_on_429: int = 3,
        retry_backoff_seconds: float = 10.0,
        respect_retry_after: bool = True,
        cache_enabled: bool = True,
        cache_dir: str = "output/ch_document_cache",
        cache_lock_timeout_seconds: float = 30.0,
    ) -> None:
        """Configure API auth/session and validate required runtime settings."""
        resolved_key = api_key or os.getenv("CH_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Missing Companies House API key. Provide api_key or set CH_API_KEY."
            )
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        if max_retries_on_429 < 0:
            raise ValueError("max_retries_on_429 must be >= 0")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be >= 0")
        if cache_lock_timeout_seconds <= 0:
            raise ValueError("cache_lock_timeout_seconds must be > 0")

        token = base64.b64encode(f"{resolved_key}:".encode("ascii")).decode("ascii")
        self._headers = {"Authorization": f"Basic {token}"}
        self._timeout = timeout
        self.public_base_url = public_base_url.rstrip("/")
        self.document_base_url = document_base_url.rstrip("/")
        self.max_retries_on_429 = max_retries_on_429
        self.retry_backoff_seconds = retry_backoff_seconds
        self.respect_retry_after = respect_retry_after
        self.cache_enabled = cache_enabled
        self.cache_dir = Path(cache_dir)
        self.cache_lock_timeout_seconds = cache_lock_timeout_seconds
        self.last_download_cache_hit: bool | None = None
        self.session = requests.Session()

    def search_companies(
        self,
        query: str,
        items_per_page: int = 20,
        start_index: int = 0,
        restrictions: str | None = None,
    ) -> dict[str, Any]:
        """Search companies by free-text query using `/search/companies`."""
        self._require_non_empty(query, "query")
        self._require_non_negative(items_per_page, "items_per_page")
        self._require_non_negative(start_index, "start_index")

        params: dict[str, Any] = {
            "q": query,
            "items_per_page": items_per_page,
            "start_index": start_index,
        }
        if restrictions:
            params["restrictions"] = restrictions

        url = f"{self.public_base_url}/search/companies"
        return self._request_json("GET", url, params=params)

    def get_company_profile(self, company_number: str) -> dict[str, Any]:
        """Fetch a company profile by registration number."""
        self._require_non_empty(company_number, "company_number")
        url = f"{self.public_base_url}/company/{company_number}"
        return self._request_json("GET", url)

    def get_filing_history(
        self,
        company_number: str,
        items_per_page: int = 100,
        start_index: int = 0,
        category: str | None = None,
    ) -> dict[str, Any]:
        """Fetch one page of filing history for a company."""
        self._require_non_empty(company_number, "company_number")
        self._require_non_negative(items_per_page, "items_per_page")
        self._require_non_negative(start_index, "start_index")

        params: dict[str, Any] = {
            "items_per_page": items_per_page,
            "start_index": start_index,
        }
        if category:
            params["category"] = category

        url = f"{self.public_base_url}/company/{company_number}/filing-history"
        return self._request_json("GET", url, params=params)

    def get_all_filing_history(
        self, company_number: str, page_size: int = 100, category: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all filing history pages and return a flattened filing list."""
        self._require_non_empty(company_number, "company_number")
        self._require_non_negative(page_size, "page_size")
        if page_size == 0:
            raise ValueError("page_size must be > 0")

        all_items: list[dict[str, Any]] = []
        start_index = 0
        total_count = None

        while total_count is None or start_index < total_count:
            page = self.get_filing_history(
                company_number=company_number,
                items_per_page=page_size,
                start_index=start_index,
                category=category,
            )
            items = page.get("items", []) or []
            total_count = int(page.get("total_count", len(items)))
            all_items.extend(items)
            start_index += page_size
            if not items:
                break

        if total_count is not None:
            return all_items[:total_count]
        return all_items

    def get_document_metadata(self, document_id: str) -> dict[str, Any]:
        """Fetch metadata for a document id from the Document API."""
        self._require_non_empty(document_id, "document_id")
        url = f"{self.document_base_url}/document/{document_id}"
        return self._request_json("GET", url)

    def list_filing_documents(self, company_number: str) -> list[dict[str, Any]]:
        """
        Build a normalized list of filing documents and their available content types.

        Each item includes:
        - document_id
        - date
        - filing_type
        - description
        - metadata_url
        - content_types
        """
        self._require_non_empty(company_number, "company_number")
        filings = self.get_all_filing_history(company_number=company_number)
        docs: list[dict[str, Any]] = []

        for item in filings:
            metadata_url = self._metadata_url_from_filing_item(item)
            if not metadata_url:
                continue

            document_id = self._extract_document_id(metadata_url)
            metadata = self._request_json("GET", metadata_url)
            content_types = self._content_types_from_metadata(metadata)
            docs.append(
                self._build_filing_document_record(
                    item=item,
                    document_id=document_id,
                    metadata_url=metadata_url,
                    content_types=content_types,
                )
            )

        return docs

    def get_latest_document(
        self,
        company_number: str,
        document_type: FilingDocumentType | None = None,
    ) -> dict[str, Any] | None:
        """
        Return the newest filing document for a company.

        If `document_type` is supplied, only documents matching that friendly type
        are considered.
        """
        docs = self.list_filing_documents(company_number=company_number)
        if not docs:
            return None

        if document_type is not None:
            docs = [
                d
                for d in docs
                if document_type.value in (d.get("friendly_types") or [])
            ]
            if not docs:
                return None

        return sorted(docs, key=lambda d: (d.get("date") or ""), reverse=True)[0]

    def download_document(
        self,
        document_id: str,
        output_path: str,
        accept: str = "application/pdf",
        company_number: str | None = None,
    ) -> str:
        """
        Download a filing document to disk using the requested `Accept` type.

        Applies rate-limit retry behavior for HTTP 429 responses.
        """
        self._require_non_empty(document_id, "document_id")
        self._require_non_empty(output_path, "output_path")
        self._require_non_empty(accept, "accept")
        if company_number is not None:
            company_number = company_number.strip()
            if not company_number:
                company_number = None

        target = Path(output_path)
        if self.cache_enabled:
            cache_path = self._cache_file_path(document_id=document_id, accept=accept)
            if self._is_usable_file(cache_path):
                self.last_download_cache_hit = True
                self._copy_if_needed(cache_path=cache_path, target=target)
                self._record_cache_index(
                    document_id=document_id,
                    accept=accept,
                    cache_path=cache_path,
                    cache_hit=True,
                    company_number=company_number,
                )
                return str(target)

            lock_path = cache_path.with_suffix(cache_path.suffix + ".lock")
            with self._cache_lock(lock_path):
                if self._is_usable_file(cache_path):
                    self.last_download_cache_hit = True
                    self._copy_if_needed(cache_path=cache_path, target=target)
                    self._record_cache_index(
                        document_id=document_id,
                        accept=accept,
                        cache_path=cache_path,
                        cache_hit=True,
                        company_number=company_number,
                    )
                    return str(target)

                self._download_document_to_path(
                    document_id=document_id,
                    destination=cache_path,
                    accept=accept,
                )
                self.last_download_cache_hit = False

            self._copy_if_needed(cache_path=cache_path, target=target)
            self._record_cache_index(
                document_id=document_id,
                accept=accept,
                cache_path=cache_path,
                cache_hit=False,
                company_number=company_number,
            )
            return str(target)

        self._download_document_to_path(
            document_id=document_id,
            destination=target,
            accept=accept,
        )
        self.last_download_cache_hit = False
        return str(target)

    def _record_cache_index(
        self,
        document_id: str,
        accept: str,
        cache_path: Path,
        cache_hit: bool,
        company_number: str | None,
    ) -> None:
        """Append cache lookup metadata for reverse-mapping document ids to companies."""
        if not company_number:
            return

        index_path = self.cache_dir / "cache_index.jsonl"
        lock_path = self.cache_dir / "cache_index.lock"
        payload = {
            "ts_utc": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "document_id": document_id,
            "company_number": company_number,
            "accept": accept,
            "cache_path": str(cache_path),
            "cache_hit": cache_hit,
        }
        with self._cache_lock(lock_path):
            index_path.parent.mkdir(parents=True, exist_ok=True)
            with index_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def _download_document_to_path(
        self,
        document_id: str,
        destination: Path,
        accept: str,
    ) -> None:
        """Download a document from Companies House and atomically write to `destination`."""
        url = f"{self.document_base_url}/document/{document_id}/content"
        combined_headers = {**self._headers, "Accept": accept}

        response = self._request_with_rate_limit(
            method="GET",
            url=url,
            headers=combined_headers,
            allow_redirects=True,
            stream=True,
        )

        if not response.ok:
            body = self._safe_response_body(response)
            raise CompaniesHouseApiError(
                status_code=response.status_code,
                url=url,
                message="Document download request failed",
                response_body=body,
            )

        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_path = destination.with_suffix(destination.suffix + ".part")
        with temp_path.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
        os.replace(temp_path, destination)

    def _cache_file_path(self, document_id: str, accept: str) -> Path:
        """Resolve deterministic cache path for a CH document and requested media type."""
        safe_accept = re.sub(r"[^A-Za-z0-9._-]+", "_", accept.strip().lower())
        return self.cache_dir / safe_accept / f"{document_id}.bin"

    @staticmethod
    def _is_usable_file(path: Path) -> bool:
        return path.exists() and path.is_file() and path.stat().st_size > 0

    @staticmethod
    def _copy_if_needed(cache_path: Path, target: Path) -> None:
        """Materialize cached content to caller's target path unless already the same file."""
        target.parent.mkdir(parents=True, exist_ok=True)
        if cache_path.resolve() == target.resolve():
            return
        shutil.copy2(cache_path, target)

    @contextlib.contextmanager
    def _cache_lock(self, lock_path: Path):
        """Cross-process lock via lock file; avoids duplicate downloads per document id."""
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd: int | None = None
        started = time.monotonic()
        while True:
            try:
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
                break
            except FileExistsError:
                if (time.monotonic() - started) >= self.cache_lock_timeout_seconds:
                    raise TimeoutError(f"Timed out waiting for cache lock: {lock_path}")
                time.sleep(0.1)

        try:
            yield
        finally:
            if fd is not None:
                os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                lock_path.unlink()

    def extract_latest_full_accounts(
        self,
        company_number: str,
        output_path: str,
        extractor: "OpenRouterDocumentExtractor",
        extraction_types: list["ExtractionType"],
        accept: str = "application/pdf",
    ) -> "ExtractionResult":
        """
        Download the latest full-accounts filing and run requested extraction types.
        """
        self._require_non_empty(company_number, "company_number")
        self._require_non_empty(output_path, "output_path")
        if extractor is None:
            raise ValueError("extractor must not be None")
        if not extraction_types:
            raise ValueError("extraction_types must not be empty")

        latest = self.get_latest_document(
            company_number=company_number,
            document_type=FilingDocumentType.FULL_ACCOUNTS,
        )
        if latest is None:
            raise ValueError("No full accounts document found for company")

        document_id = str(latest.get("document_id") or "").strip()
        if not document_id:
            raise ValueError("Latest full accounts document did not include a document_id")

        downloaded_path = self.download_document(
            document_id=document_id, output_path=output_path, accept=accept
        )
        return extractor.extract(
            document_path=downloaded_path, extraction_types=extraction_types
        )

    def extract_latest_mat_annual_report(
        self,
        company_number: str,
        output_path: str,
        extractor: "OpenRouterDocumentExtractor",
        accept: str = "application/pdf",
    ) -> "ExtractionResult":
        """
        Convenience flow for extracting a full MAT annual report payload.
        """
        return self.extract_latest_full_accounts(
            company_number=company_number,
            output_path=output_path,
            extractor=extractor,
            extraction_types=[ExtractionType.AcademyTrustAnnualReport],
            accept=accept,
        )

    def _request_json(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute an HTTP request and return parsed JSON or raise typed API errors."""
        combined_headers = dict(self._headers)
        if headers:
            combined_headers.update(headers)

        response = self._request_with_rate_limit(
            method=method, url=url, params=params, headers=combined_headers
        )

        if not response.ok:
            body = self._safe_response_body(response)
            raise CompaniesHouseApiError(
                status_code=response.status_code,
                url=url,
                message="API request failed",
                response_body=body,
            )

        try:
            return response.json()
        except ValueError as exc:
            raise CompaniesHouseApiError(
                status_code=response.status_code,
                url=url,
                message="Response was not valid JSON",
                response_body=self._safe_response_body(response),
            ) from exc

    def _request_with_rate_limit(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        **request_kwargs: Any,
    ) -> requests.Response:
        """Send an HTTP request and optionally retry when API rate limits are hit."""
        retries_used = 0
        while True:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=self._timeout,
                    **request_kwargs,
                )
            except requests.RequestException as exc:
                raise CompaniesHouseApiError(
                    status_code=None,
                    url=url,
                    message=f"Request failed: {exc}",
                ) from exc

            if response.status_code != 429 or retries_used >= self.max_retries_on_429:
                return response

            delay = self._get_retry_delay_seconds(response)
            time.sleep(delay)
            retries_used += 1

    def _get_retry_delay_seconds(self, response: requests.Response) -> float:
        """Resolve wait duration for a 429 response."""
        if self.respect_retry_after:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    value = float(retry_after.strip())
                    if value >= 0:
                        return value
                except ValueError:
                    pass

        return self.retry_backoff_seconds

    @staticmethod
    def _extract_document_id(metadata_url: str) -> str:
        """Extract `{document_id}` from a document metadata URL/path."""
        if not metadata_url or not metadata_url.strip():
            raise ValueError("metadata_url must not be empty")

        parsed = urlparse(metadata_url)
        path = parsed.path if parsed.scheme else metadata_url
        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) < 2 or parts[-2] != "document":
            raise ValueError(f"Unsupported document metadata URL: {metadata_url}")
        return parts[-1]

    @staticmethod
    def _friendly_document_types_for_item(item: dict[str, Any]) -> list[FilingDocumentType]:
        """Map raw filing fields to friendly document categories."""
        filing_type = str(item.get("type") or "").upper()
        description = str(item.get("description") or "").lower()
        friendly: list[FilingDocumentType] = []

        if filing_type == "CS01" or "confirmation-statement" in description:
            friendly.append(FilingDocumentType.CONFIRMATION_STATEMENT)

        if filing_type == "AA" and (
            "accounts-type-full" in description or "full" in description
        ):
            friendly.append(FilingDocumentType.FULL_ACCOUNTS)

        for friendly_type, type_codes in CompaniesHouseClient._FRIENDLY_TYPE_CODE_SETS.items():
            if filing_type in type_codes and friendly_type not in friendly:
                friendly.append(friendly_type)

        return friendly

    @staticmethod
    def _metadata_url_from_filing_item(item: dict[str, Any]) -> str | None:
        links = item.get("links", {})
        if not isinstance(links, dict):
            return None
        metadata_url = links.get("document_metadata")
        if not isinstance(metadata_url, str):
            return None
        return metadata_url

    @staticmethod
    def _content_types_from_metadata(metadata: dict[str, Any]) -> list[str]:
        resources = metadata.get("resources", {})
        if isinstance(resources, dict):
            return sorted(resources.keys())
        return []

    @staticmethod
    def _build_filing_document_record(
        item: dict[str, Any],
        document_id: str,
        metadata_url: str,
        content_types: list[str],
    ) -> dict[str, Any]:
        friendly_types = CompaniesHouseClient._friendly_document_types_for_item(item)
        return {
            "document_id": document_id,
            "date": item.get("date"),
            "filing_type": item.get("type"),
            "description": item.get("description"),
            "metadata_url": metadata_url,
            "content_types": content_types,
            "friendly_types": [t.value for t in friendly_types],
        }

    @staticmethod
    def _require_non_empty(value: str, name: str) -> None:
        """Guard helper for required string fields."""
        if not value or not str(value).strip():
            raise ValueError(f"{name} must not be empty")

    @staticmethod
    def _require_non_negative(value: int, name: str) -> None:
        """Guard helper for numeric fields that must be >= 0."""
        if value < 0:
            raise ValueError(f"{name} must be >= 0")

    @staticmethod
    def _safe_response_body(response: requests.Response) -> str:
        """Best-effort response body extraction for richer error messages."""
        try:
            return response.text
        except Exception:
            return "<unavailable>"



__all__ = [
    "AcademyTrustAnnualReport",
    "BalanceSheetEntry",
    "CompaniesHouseApiError",
    "CompaniesHouseClient",
    "DetailedBalanceSheet",
    "DocumentExtractionError",
    "ExtractionResult",
    "ExtractionType",
    "FilingDocumentType",
    "Governance",
    "Metadata",
    "OpenRouterDocumentExtractor",
    "PersonnelDetail",
    "StaffingData",
    "StatementOfFinancialActivities",
]
