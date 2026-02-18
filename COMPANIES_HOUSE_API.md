# Companies House API Reference (Local Consolidation)

This file consolidates the information from the saved local specs so you can remove the downloaded HTML files.

Source files used:
- `swagger.json`
- `swagger (1).json`
- `Authentication.html`
- `Companies House Public Data API_ Search companies.html`
- `Companies House Public Data API_ Company profile.html`
- `Companies House Public Data API_ filingHistoryList resource.html`
- `Document API_ Fetch a document's metadata.html`
- `Document API_ Fetch a document.html`

## 0) Live Reference Notes (from developer-specs site)

Visited links:
- `https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference`
- `https://developer-specs.company-information.service.gov.uk/document-api/reference`

### Introductory info

On the API products page:
- **Companies House Public Data API** is described as read-only access to search and retrieve public company data.
- **Document API** is described as the API for filing-history document metadata and downloads.

### Detailed info from the two reference pages

#### Companies House Public Data API

Reference page shows grouped operations across these areas:
- Registered office address
- Company profile
- Search (advanced, all, companies, officers, disqualified officers, alphabetical, dissolved)
- Officers
- Registers
- Charges
- Filing history
- Insolvency
- Exemptions
- Officer disqualifications
- Officer appointments
- UK establishments
- Persons with significant control (PSC)

This matches the endpoint catalog in `swagger.json` and confirms the API is broad but read-only.

#### Document API

Reference page exposes two operations:
- `GET /document/{document_id}` for metadata
- `GET /document/{document_id}/content` for file retrieval

Operation details confirmed:
- `document_id` is required for both endpoints.
- `Accept` header is required for `/content`.
- Unsupported `Accept` can return `406`.
- `/content` returns `302 Found` with a `Location` header for the actual file URL.
- Unauthorized access returns `401`.

## 1) Base URLs

Public Data API:
- `https://api.company-information.service.gov.uk`

Document API:
- `https://document-api.company-information.service.gov.uk`

## 2) Authentication

Primary auth for these endpoints is HTTP Basic using your API key as username and an empty password.

Header format:
- `Authorization: Basic <base64("API_KEY:")>`

Example:
- If API key is `my_api_key`, encode `my_api_key:` and send as Basic auth header.

Bearer auth is shown in the auth docs but for this project flow (public data + document endpoints), Basic auth with API key is what you use.

## 3) Rate Limiting (Live Guide)

Source:
- `https://developer-specs.company-information.service.gov.uk/guides/rateLimiting`

Current guide states:
- Default application limit: **600 requests per 5-minute window**
- Exceeding the limit returns: **`429 Too Many Requests`**
- Requests continue to get `429` for the remainder of that 5-minute window.
- The quota resets to the full 600 at the end of the window.
- Companies House may ban applications that regularly exceed or attempt to bypass limits.
- Higher limits can be requested via Companies House support/contact.

Implementation implications:
- Use retry/backoff only for transient failures; do not aggressively retry `429`.
- Respect `429` as a quota signal and delay until the next window.
- Keep concurrency bounded in batch jobs.
- Current client defaults: `max_retries_on_429=3`, `retry_backoff_seconds=10`, `respect_retry_after=True`.

## 4) Quick Workflow (Most Common)

1. Search for company
- `GET /search/companies?q=<query>&items_per_page=<n>&start_index=<n>`

2. Get company profile
- `GET /company/{companyNumber}`

3. Get filing history
- `GET /company/{company_number}/filing-history?items_per_page=<n>&start_index=<n>`

4. From filing items, read `links.document_metadata` (when present)

5. Get document metadata
- `GET /document/{document_id}`

6. Download document content
- `GET /document/{document_id}/content`
- Required header: `Accept: <supported content type>` (commonly `application/pdf`)
- The endpoint responds with a redirect to a `Location` URL; follow redirects.

## 5) Endpoint Catalog

### 5.1 Public Data API (`swagger.json`)

- `GET /advanced-search/companies`
- `GET /alphabetical-search/companies`
- `GET /company/{company_number}/appointments/{appointment_id}`
- `GET /company/{company_number}/charges`
- `GET /company/{company_number}/charges/{charge_id}`
- `GET /company/{company_number}/exemptions`
- `GET /company/{company_number}/filing-history`
- `GET /company/{company_number}/filing-history/{transaction_id}`
- `GET /company/{company_number}/insolvency`
- `GET /company/{company_number}/officers`
- `GET /company/{company_number}/persons-with-significant-control`
- `GET /company/{company_number}/persons-with-significant-control/corporate-entity/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/corporate-entity-beneficial-owner/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/individual/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/individual-beneficial-owner/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/legal-person/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/legal-person-beneficial-owner/{psc_id}`
- `GET /company/{company_number}/persons-with-significant-control/super-secure/{super_secure_id}`
- `GET /company/{company_number}/persons-with-significant-control/super-secure-beneficial-owner/{super_secure_id}`
- `GET /company/{company_number}/persons-with-significant-control-statements`
- `GET /company/{company_number}/persons-with-significant-control-statements/{statement_id}`
- `GET /company/{company_number}/registers`
- `GET /company/{company_number}/uk-establishments`
- `GET /company/{companyNumber}`
- `GET /company/{companyNumber}/registered-office-address`
- `GET /disqualified-officers/corporate/{officer_id}`
- `GET /disqualified-officers/natural/{officer_id}`
- `GET /dissolved-search/companies`
- `GET /officers/{officer_id}/appointments`
- `GET /search`
- `GET /search/companies`
- `GET /search/disqualified-officers`
- `GET /search/officers`

### 5.2 Document API (`swagger (1).json`)

- `GET /document/{document_id}`
- `GET /document/{document_id}/content`

## 6) Important Request Parameters (from local HTML pages)

### `GET /search/companies`
- `q` (required): search term
- `items_per_page` (optional)
- `start_index` (optional)
- `restrictions` (optional)

### `GET /company/{companyNumber}`
- path parameter: `company_number`

### `GET /company/{company_number}/filing-history`
- path parameter: `company_number`
- query parameters include:
- `category` (optional)
- `items_per_page` (optional)
- `start_index` (optional)

### `GET /document/{document_id}/content`
- path parameter: `document_id`
- header parameter: `Accept` (required)

## 7) Common Response Statuses (seen in local docs)

- `200` OK
- `401` Unauthorized
- `404` Not found
- `302` Found/Redirect (Document content endpoint with `Location`)
- `429` Too Many Requests (rate limit exceeded)

## 8) Example Calls

## 8.1 cURL

```bash
# Set your key once in shell
export CH_API_KEY="your_api_key"

# Search companies
curl -s -u "$CH_API_KEY:" \
  "https://api.company-information.service.gov.uk/search/companies?q=tesco&items_per_page=5"

# Company profile
curl -s -u "$CH_API_KEY:" \
  "https://api.company-information.service.gov.uk/company/00445790"

# Filing history
curl -s -u "$CH_API_KEY:" \
  "https://api.company-information.service.gov.uk/company/00445790/filing-history?items_per_page=25"

# Document metadata
curl -s -u "$CH_API_KEY:" \
  "https://document-api.company-information.service.gov.uk/document/<document_id>"

# Document content (follow redirect and save)
curl -L -u "$CH_API_KEY:" \
  -H "Accept: application/pdf" \
  "https://document-api.company-information.service.gov.uk/document/<document_id>/content" \
  -o document.pdf
```

## 8.2 PowerShell

```powershell
$pair = "{0}:" -f $env:CH_API_KEY
$auth = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
$headers = @{ Authorization = "Basic $auth" }

# Company profile by registration number
Invoke-RestMethod -Method GET `
  -Uri "https://api.company-information.service.gov.uk/company/09618502" `
  -Headers $headers
```

## 9) Local Project Usage

Scripts already created in this repo:
- `companieshouse_fetch.ps1` (end-to-end workflow)
- `run_companieshouse.bat` (wrapper)
- `batch_extract_trusts.py` (batch XLSX -> PDF + JSON + SQLite extraction pipeline)

Core Python modules:
- `companies_house_client.py` (Companies House API client + compatibility exports)
- `document_extraction_models.py` (extraction enums and Pydantic models)
- `openrouter_document_extractor.py` (OpenRouter extraction implementation)

Expected env variable:
- `CH_API_KEY` (stored in `.env`)

## 10) Cleanup Guidance

After confirming this file and your scripts are enough, you can remove the saved HTML folders/files:
- `Authentication.html` and `Authentication_files/`
- `Companies House Public Data API_*.html` and related `_files/` folders
- `Document API_*.html` and related `_files/` folders
- `_ Specifications list.html` and `_ Specifications list_files/`

Keep:
- `.env`
- `companieshouse_fetch.ps1`
- `run_companieshouse.bat`
- `COMPANIES_HOUSE_API.md`
- Optional: `swagger.json` and `swagger (1).json` as machine-readable backups

## 11) LLM Document Extraction (OpenRouter SDK)

The client now includes an OpenRouter-backed extractor for downloaded filings.

Required env variables:
- `CH_API_KEY`
- `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL` (or pass model in code)
- Cost warning: avoid `openrouter/auto` for production/cost control; set an explicit model in `OPENROUTER_MODEL`.

Current extraction behavior:
- `extract_latest_full_accounts(...)` selects the latest `FilingDocumentType.FULL_ACCOUNTS` document only for retrieval.
- `extract_latest_mat_annual_report(...)` is a convenience wrapper that calls `extract_latest_full_accounts(...)` with `ExtractionType.AcademyTrustAnnualReport`.
- Extraction itself is requested independently via `ExtractionType` (you can pass one or many):
  - `ExtractionType.PersonnelDetails` -> `personnel_details[]`
  - `ExtractionType.BalanceSheet` -> legacy `balance_sheet[]` line-item output
  - `ExtractionType.Metadata` -> `metadata`
  - `ExtractionType.Governance` -> `governance`
  - `ExtractionType.StatementOfFinancialActivities` -> `statement_of_financial_activities`
  - `ExtractionType.DetailedBalanceSheet` -> `detailed_balance_sheet`
  - `ExtractionType.StaffingData` -> `staffing_data`
  - `ExtractionType.AcademyTrustAnnualReport` -> `academy_trust_annual_report` (full MAT object)
- MAT numeric fields are normalized during model parsing:
  - Supports commas, `£/$/€`, parenthesized negatives, and null-like placeholders (`-`, `—`, `N/A`, `null`).
- The extractor sends the downloaded document file directly to OpenRouter chat completions as message content `type: "file"` with `file.file_data` data URI.
- No local PDF parsing/text extraction is performed in this project.
- OpenRouter structured output is enforced with `response_format.type = "json_schema"` and strict schema mode.
- OpenRouter provider preferences set `require_parameters = false` for model-specific compatibility and lower-friction routing.
- JSON parsing strategy is strict-first, then repair fallback:
  - First attempt: `json.loads(...)`
  - Fallback: `json-repair` (`json_repair.loads(..., skip_json_loads=True)`) for malformed but recoverable JSON emitted by models
- `ExtractionResult.validation_warnings` contains non-fatal reconciliation warnings. Extraction continues even when warnings are present.
- Reconciliation warning checks currently include:
  - SOFA fund component sums vs reported `total`.
  - Detailed balance sheet computed net assets vs reported `net_assets`.
  - Top-level `detailed_balance_sheet` vs nested `academy_trust_annual_report.balance_sheet` mismatch.
- Reconciliation tolerance is absolute difference `> 1` (to reduce false positives from rounding).

PowerShell example:

```powershell
@'
import os
from companies_house_client import (
    CompaniesHouseClient,
    ExtractionType,
    OpenRouterDocumentExtractor,
)

client = CompaniesHouseClient(api_key=os.getenv("CH_API_KEY"))
extractor = OpenRouterDocumentExtractor(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    model=os.getenv("OPENROUTER_MODEL"),
)

result = client.extract_latest_full_accounts(
    company_number="09618502",
    output_path="output\\09618502\\latest_full_accounts.pdf",
    extractor=extractor,
    extraction_types=[
        ExtractionType.PersonnelDetails,
        ExtractionType.BalanceSheet,
    ],
)

print("Personnel:", [p.model_dump() for p in (result.personnel_details or [])])
print("Balance sheet:", [b.model_dump() for b in (result.balance_sheet or [])])
'@ | .\.venv\Scripts\python -
```

MAT full report example:

```powershell
@'
import os
from companies_house_client import CompaniesHouseClient, OpenRouterDocumentExtractor

client = CompaniesHouseClient(api_key=os.getenv("CH_API_KEY"))
extractor = OpenRouterDocumentExtractor(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    model=os.getenv("OPENROUTER_MODEL"),
)

result = client.extract_latest_mat_annual_report(
    company_number="09618502",
    output_path="output\\09618502\\latest_full_accounts.pdf",
    extractor=extractor,
)

print("Warnings:", result.validation_warnings)
report = result.academy_trust_annual_report
print("MAT report keys:", list(report.model_dump().keys()) if report else [])
'@ | .\.venv\Scripts\python -
```

## 12) Batch Trust Extraction Pipeline

Purpose:
- Read `SourceData\allgroupslinksdata20260217\Trusts.xlsx`
- Use `Companies House Number` as the canonical identifier
- Download latest full-accounts PDF per trust
- Extract all available structured fields via OpenRouter
- Save outputs as files and in SQLite

PowerShell run example:

```powershell
.\.venv\Scripts\python .\batch_extract_trusts.py `
  --input-xlsx "SourceData\allgroupslinksdata20260217\Trusts.xlsx" `
  --output-root "output\trusts_extraction" `
  --model "google/gemini-2.5-flash-lite"
```

Useful controls:
- `--max-companies 10` to run a small sample first
- `--start-index 100` to resume from an offset in the deduplicated list
- `--db-path "output\trusts_extraction\companies_house_extractions.db"` to override DB location
- `--random-sample-size 10` to process a random sample from the selected batch
- `--random-seed 42` to make random sampling reproducible
- `--fallback-models "<modelA>,<modelB>"` optional fallback list when a provider rejects file message content for your primary model
- `--ch-min-request-interval-seconds 2.0` minimum delay between Companies House HTTP requests (default is `2.0`, which is ~25% of the documented `600/5min` limit). Set `0` to disable pacing.
- `--write-summary-json` to emit run-level `summary.json` in the run folder
- `--summary-json-path "<path>"` optional explicit location for summary JSON
- `--filing-history-items-per-page 100` fetches only the first filing-history page for latest full-accounts selection (faster and lower request volume)

Per-run output layout:
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\documents\<company_number>_latest_full_accounts_<document_id>.pdf`
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\api\profile.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\api\filing_history.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\extraction\extraction_result.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\extraction\validation_warnings.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company_number>\extraction\run_report.json`
- Optional run summary: `output\trusts_extraction\run_<UTCSTAMP>\summary.json` (when `--write-summary-json` is set)

SQLite persistence:
- Default DB path: `output\trusts_extraction\companies_house_extractions.db`
- Table `runs`: one row per batch run with counters and model metadata
- Table `company_reports`: one row per company with status, document id, file paths, `model_used`, `pdf_size_bytes`, `approx_llm_tokens`, error text, and JSON payloads (`profile_json`, `filing_history_json`, `extraction_json`, `warnings_json`)
- Batch extractor behavior: retries the same model up to 3 attempts total when OpenRouter returns non-JSON content, to reduce transient parse failures.

## 13) Smoke Test And Final Validation

Unit test command:

```powershell
.\.venv\Scripts\python -m unittest -v test_companies_house_client.py
```

Expected baseline:
- Unit tests pass without network access.
- Live smoke tests are skipped when `CH_API_KEY` is not set.
- Live extraction smoke test is skipped when `OPENROUTER_API_KEY` is not set.

Live smoke guidance:
1. Set `CH_API_KEY` in `.env`.
2. Set `OPENROUTER_API_KEY` in `.env` for extraction smoke.
3. Set an explicit model in `.env` (example: `OPENROUTER_MODEL=google/gemini-2.5-flash-lite`).
4. Re-run `.\.venv\Scripts\python -m unittest -v test_companies_house_client.py`.

Known limitations:
- `validation_warnings` are advisory only; they do not fail extraction.
- Reconciliation uses tolerance `abs(diff) > 1`; small rounding differences are ignored.
- Warnings only run when all required numeric inputs for the specific check are present.
- LLM extraction quality depends on model output and document quality/layout.
