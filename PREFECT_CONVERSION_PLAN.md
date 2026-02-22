# Prefect Workflow Conversion Plan

## 1. Executive Summary

This document examines how to convert the CompaniesHouse project into Prefect 3.x workflows. The project currently runs as a monolithic batch script (`batch_extract_companies.py`) that sequentially processes companies from an XLSX input, calling the Companies House API and OpenRouter LLM extraction for each. Converting to Prefect would add observability, retry management, concurrency control, scheduling, and per-company visibility into the pipeline.

---

## 2. Current Architecture

### 2.1 Module Overview

| File | Role |
|---|---|
| `batch_extract_companies.py` | Main batch orchestrator: reads XLSX, iterates companies, coordinates API calls + extraction, writes results to SQLite + JSON. Accepts `--company-type` flag (generic or academy_trust) |
| `company_type.py` | `CompanyType` enum (GENERIC, ACADEMY_TRUST) and prompt profiles controlling LLM text, field names, and terminology per company type |
| `companies_house_client.py` | HTTP client for Companies House Public Data & Document APIs (search, profile, filing history, document download) |
| `openrouter_document_extractor.py` | Sends downloaded PDFs to OpenRouter LLM API for structured data extraction. Accepts `company_type` to select prompts and schemas |
| `document_extraction_models.py` | Pydantic models for all extraction schemas. Includes both trust-specific (`AcademyTrustAnnualReport`, `Metadata`, `TrusteeAttendance`) and generic (`AnnualReport`, `CompanyMetadata`, `DirectorAttendance`) models |
| `test_companies_house_client.py` | Unit tests (mocked HTTP) |

### 2.2 Current Data Flow

```
XLSX Input
    |
    v
[Read & deduplicate company numbers]
    |
    v
FOR EACH company (sequential):
    |-- 1. GET /company/{number}                  (Companies House API)
    |-- 2. GET /company/{number}/filing-history    (Companies House API)
    |-- 3. Find latest full-accounts document_id
    |-- 4. GET /document/{id}/content              (Companies House Document API)
    |-- 5. POST to OpenRouter chat/completions     (LLM extraction)
    |-- 6. Parse & validate extraction result
    |-- 7. Write JSON files + SQLite row
    v
[Finalize run in SQLite, optional summary JSON]
```

### 2.3 Key Characteristics

- **Sequential processing**: Companies are processed one at a time in a loop
- **Manual rate limiting**: A monkey-patched `session.request` throttle enforces 2s minimum between Companies House requests
- **Model fallback**: If the primary LLM model rejects file input, fallback models are tried
- **Schema fallback**: If a "schema depth" error occurs, the extraction schema downgrades from `compact_single_call` to `light_core`
- **SQLite tracking**: Run metadata and per-company results stored in a local SQLite database
- **Error tolerance**: Individual company failures are caught and recorded; the batch continues

---

## 3. Proposed Prefect Architecture

### 3.1 Flow Hierarchy

```
batch_extract_companies_flow (top-level flow)
    |
    |-- Parameters include company_type: CompanyType (GENERIC or ACADEMY_TRUST)
    |
    |-- load_and_prepare_batch (task)
    |       Read XLSX, deduplicate, apply slicing/sampling
    |
    |-- initialize_run (task)
    |       Create output dirs, SQLite tables, insert run row
    |
    |-- FOR EACH company:
    |       process_company_flow (subflow, one per company)
    |           |-- fetch_company_profile (task)
    |           |-- fetch_filing_history (task)
    |           |-- find_latest_full_accounts (task)
    |           |-- download_document (task)
    |           |-- extract_document (task)  ← receives company_type
    |           |-- save_results (task)
    |
    |-- finalize_run (task)
            Update run totals, write summary JSON
```

### 3.2 Why This Structure

- **Per-company subflows** give first-class observability in the Prefect UI: each company appears as its own flow run with individual status, duration, and logs.
- **Granular tasks** within each subflow allow Prefect to track exactly which stage failed (API call vs download vs extraction) and enable targeted retries.
- **The top-level flow** manages batch-level concerns: input preparation, run initialization, result aggregation.

---

## 4. Detailed Conversion Mapping

### 4.1 Task Definitions

Below is how each major piece of work maps to a Prefect task:

#### `load_and_prepare_batch`
```python
@task(name="load-and-prepare-batch", retries=0)
def load_and_prepare_batch(
    input_xlsx: str,
    start_index: int,
    max_companies: int,
    random_sample_size: int,
    random_seed: int,
) -> list[dict]:
    """Read XLSX, deduplicate company numbers, apply slicing/sampling."""
    # Wraps: read_xlsx_rows() + normalize_company_number() + slicing logic
    # from batch_extract_companies.py
```

#### `initialize_run`
```python
@task(name="initialize-run", retries=0)
def initialize_run(
    input_xlsx_path: str,
    output_root: str,
    model: str,
    extraction_types: list[ExtractionType],
) -> dict:
    """Create directories, SQLite tables, and insert run row.
    Returns dict with run_id, output_run_dir, db_path, conn info."""
    # Wraps: lines 673-693
```

#### `fetch_company_profile`
```python
@task(
    name="fetch-company-profile",
    retries=2,
    retry_delay_seconds=15,
    tags=["companies-house-api"],
)
def fetch_company_profile(client: CompaniesHouseClient, company_number: str) -> dict:
    """GET /company/{number} -- Companies House profile."""
    return client.get_company_profile(company_number)
```

#### `fetch_filing_history`
```python
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
```

#### `find_latest_full_accounts`
```python
@task(name="find-latest-full-accounts", retries=0)
def find_latest_full_accounts(filings: list[dict]) -> str:
    """Identify the document_id for the most recent full accounts filing.
    Raises ValueError if none found."""
    # Wraps: _latest_full_accounts_document_id_from_filing_history()
```

#### `download_document`
```python
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
```

#### `extract_document`
```python
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
    company_type controls which prompt text and schema field names are used."""
    # Wraps: _extract_with_model_fallback() + schema depth fallback logic
    # CompanyType is a str enum -- Prefect serializes it natively as a parameter
```

#### `save_results`
```python
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
    pdf_path: str,
    pdf_size_bytes: int,
    output_dirs: dict,
    db_path: str,
) -> dict:
    """Write JSON files and insert SQLite row for a successful company."""
    # Wraps: write_json() calls + _insert_company_row()
```

### 4.2 Subflow: Per-Company Processing

```python
@flow(
    name="process-company",
    retries=0,  # Failures are recorded, not retried at flow level
    log_prints=True,
)
def process_company_flow(
    client: CompaniesHouseClient,
    openrouter_api_key: str,
    model_candidates: list[str],
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

    pdf_path = f"{output_run_dir}/{company_number}/documents/{company_number}_latest_full_accounts_{document_id}.pdf"
    downloaded_path = download_document(client, document_id, pdf_path)

    extraction_payload, warnings, model_used = extract_document(
        openrouter_api_key=openrouter_api_key,
        model_candidates=model_candidates,
        document_path=downloaded_path,
        extraction_types=extraction_types,
        retries_on_invalid_json=retries_on_invalid_json,
        schema_profile=schema_profile,
        company_type=company_type,
    )

    result = save_results(
        run_id=run_id,
        item=item,
        company_number=company_number,
        profile=profile,
        filing_history=filings,
        extraction_payload=extraction_payload,
        warnings_payload=warnings,
        model_used=model_used,
        pdf_path=str(pdf_path),
        pdf_size_bytes=Path(downloaded_path).stat().st_size,
        output_dirs={...},
        db_path=db_path,
    )
    return result
```

### 4.3 Top-Level Flow

```python
from company_type import CompanyType

@flow(
    name="batch-extract-companies",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(max_workers=1),  # See concurrency section
)
def batch_extract_companies_flow(
    input_xlsx: str = DEFAULT_INPUT_XLSX,
    output_root: str = DEFAULT_OUTPUT_ROOT,
    model: str = "",
    max_companies: int = 0,
    start_index: int = 0,
    schema_profile: str = "compact_single_call",
    company_type: str = "generic",  # Prefect UI passes strings; convert below
    # ... other params matching current CLI args
):
    ct = CompanyType(company_type)
    extraction_types = _extraction_types_for_schema_profile(schema_profile, ct)
    batch = load_and_prepare_batch(input_xlsx, start_index, max_companies, ...)
    run_context = initialize_run(input_xlsx, output_root, model, extraction_types)

    results = []
    for item in batch:
        result = process_company_flow(
            client=run_context["client"],
            company_type=ct,
            item=item,
            run_id=run_context["run_id"],
            ...
        )
        results.append(result)

    finalize_run(run_context, results)
```

> **Note on CompanyType with Prefect**: `CompanyType` is a `str` enum (`class CompanyType(str, Enum)`), so Prefect 3.x serializes it natively as a string parameter. When deploying via the Prefect UI or API, pass `"generic"` or `"academy_trust"` as the `company_type` parameter value. The flow converts it to the enum internally.

---

## 5. Key Conversion Considerations

### 5.1 Rate Limiting the Companies House API

**Current approach**: A monkey-patched `session.request` enforces a 2-second minimum between all CH API requests.

**Prefect approach**: Use Prefect's **global concurrency limits with rate limiting** (`slot_decay_per_second`):

```bash
# Create a rate limit: 1 slot, decays every 2 seconds = max 0.5 req/sec
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5
```

Then in tasks:

```python
from prefect.concurrency.sync import rate_limit

@task(name="fetch-company-profile", tags=["companies-house-api"])
def fetch_company_profile(client, company_number):
    rate_limit("companies-house-api")  # Block until a slot is available
    return client.get_company_profile(company_number)
```

This replaces the monkey-patched throttle with a first-class, observable rate limiter that works correctly even with concurrent workers.

**Alternative (simpler)**: Keep the existing throttle mechanism inside `CompaniesHouseClient` and use Prefect's tag-based concurrency limit (`--limit 1`) on the `companies-house-api` tag to ensure only one CH API task runs at a time.

### 5.2 Concurrency Strategy

The pipeline has two external bottlenecks with different characteristics:

| Bottleneck | Current Behavior | Prefect Strategy |
|---|---|---|
| Companies House API | ~0.5 req/sec rate limit | Tag concurrency limit (`companies-house-api`, limit=1) + rate_limit() |
| OpenRouter LLM API | No explicit rate limit, but expensive | Tag concurrency limit (`openrouter-llm`, limit=1-3) based on budget |

**Phase 1 (safe)**: Run sequentially as today -- `max_workers=1`. Get observability benefits with zero risk.

**Phase 2 (parallel extraction)**: Since each company's LLM extraction is independent and doesn't hit the CH API, you can parallelize extraction while keeping CH API calls serialized:
- CH API tasks: tag concurrency limit of 1 + rate_limit
- LLM tasks: tag concurrency limit of 2-3

**Phase 3 (full concurrency)**: Process multiple companies in parallel using `process_company_flow.submit()` with appropriate tag limits.

### 5.3 Retry Strategy

| Task | Retries | Delay | Rationale |
|---|---|---|---|
| `fetch_company_profile` | 2 | 15s | Transient API errors, 429 handled by client |
| `fetch_filing_history` | 2 | 15s | Same as above |
| `download_document` | 2 | 15s | Same as above, plus network issues |
| `extract_document` | 1 | 30s | LLM calls are expensive; existing model fallback handles most errors internally |
| `find_latest_full_accounts` | 0 | - | Pure logic, no external calls |
| `save_results` | 0 | - | Local I/O, should not fail |
| `load_and_prepare_batch` | 0 | - | Local file read |

Note: The existing `CompaniesHouseClient` already handles HTTP 429 retries with backoff internally. Prefect-level retries catch errors that the client doesn't handle (network timeouts, 5xx errors, etc.).

### 5.4 SQLite Considerations

SQLite does not support concurrent writes well. Two options:

1. **Keep SQLite, serialize writes**: Use a Prefect concurrency limit on `save_results` tasks, or use a threading lock. This is simplest for Phase 1.
2. **Switch to a more concurrent backend**: For Phase 3 (full concurrency), consider PostgreSQL or use `aiosqlite` with WAL mode.

For Phase 1 (sequential processing), SQLite works without changes.

### 5.5 CompaniesHouseClient Serialization

The `CompaniesHouseClient` holds a `requests.Session` object which is not serializable and should not be passed between processes. In Prefect 3:

- **ThreadPoolTaskRunner** (default): Client can be created once in the flow and passed to tasks -- works because tasks share the same process.
- **DaskTaskRunner / ProcessPoolTaskRunner**: Client must be created inside each task or subflow, not passed as a parameter.

Recommendation: Create the client inside `process_company_flow` using the API key (a string), rather than passing the client object.

### 5.6 Secrets Management

**Current**: `.env` file loaded manually with `_load_dotenv_file()`.

**Prefect approach**: Use Prefect Secret blocks or environment variables:

```python
from prefect.blocks.system import Secret

ch_api_key = Secret.load("companies-house-api-key").get()
```

This integrates with Prefect Cloud's secrets management. For local development, environment variables continue to work.

### 5.7 Logging

**Current**: `print()` statements throughout.

**Prefect approach**: Use `log_prints=True` on flows (already shown above) which captures all `print()` output as Prefect logs. Alternatively, use Prefect's logger:

```python
from prefect import get_run_logger

@task
def fetch_company_profile(client, company_number):
    logger = get_run_logger()
    logger.info(f"Fetching profile for {company_number}")
    ...
```

### 5.8 CLI Arguments to Flow Parameters

**Current**: `argparse` in `main()`.

**Prefect approach**: Flow parameters replace CLI args. For CLI invocation, keep a thin `if __name__ == "__main__"` wrapper:

```python
if __name__ == "__main__":
    args = build_parser().parse_args()
    batch_extract_companies_flow(
        input_xlsx=args.input_xlsx,
        output_root=args.output_root,
        model=args.model,
        company_type=args.company_type,
        ...
    )
```

For deployed flows, parameters are set through the Prefect UI or API.

---

## 6. File Structure After Conversion

```
CompaniesHouse/
    company_type.py                    # CompanyType enum + prompt profiles
    companies_house_client.py          # Unchanged (API client)
    document_extraction_models.py      # Pydantic models (trust-specific + generic)
    openrouter_document_extractor.py   # LLM extractor (company-type-aware)
    flows/
        __init__.py
        batch_extract.py               # Top-level flow + subflows
        tasks/
            __init__.py
            data_loading.py            # load_and_prepare_batch, initialize_run, finalize_run
            companies_house.py         # fetch_company_profile, fetch_filing_history, download_document
            extraction.py              # find_latest_full_accounts, extract_document
            persistence.py             # save_results
    prefect.yaml                       # Deployment configuration (multiple deployments per company_type)
    requirements.txt                   # Add: prefect>=3.0
    batch_extract_companies.py         # CLI entry point, calls flow
    test_companies_house_client.py     # Unit tests
```

---

## 7. Deployment Options

### 7.1 Local Development (`.serve()`)

Simplest option. The flow process runs continuously and picks up scheduled or manual runs:

```python
if __name__ == "__main__":
    batch_extract_companies_flow.serve(
        name="batch-extract-companies-local",
        cron="0 6 * * 1",  # Weekly Monday 6am
        parameters={"company_type": "generic"},  # or "academy_trust"
    )
```

### 7.2 Work Pool Deployment (`.deploy()`)

For production with Prefect Cloud or self-hosted server:

```python
# Deploy for academy trusts
batch_extract_companies_flow.deploy(
    name="batch-extract-trusts-prod",
    work_pool_name="default-agent-pool",
    cron="0 6 * * 1",
    parameters={
        "input_xlsx": "SourceData/allgroupslinksdata20260217/Trusts.xlsx",
        "schema_profile": "compact_single_call",
        "company_type": "academy_trust",
    },
)

# Deploy for generic companies (different input, schedule, etc.)
batch_extract_companies_flow.deploy(
    name="batch-extract-companies-prod",
    work_pool_name="default-agent-pool",
    cron="0 8 * * 1",
    parameters={
        "input_xlsx": "SourceData/companies.xlsx",
        "schema_profile": "compact_single_call",
        "company_type": "generic",
    },
)
```

### 7.3 `prefect.yaml` Configuration

```yaml
deployments:
  - name: batch-extract-trusts
    entrypoint: flows/batch_extract.py:batch_extract_companies_flow
    work_pool:
      name: default
    parameters:
      input_xlsx: "SourceData/allgroupslinksdata20260217/Trusts.xlsx"
      output_root: "output/companies_extraction"
      schema_profile: "compact_single_call"
      company_type: "academy_trust"
    schedules:
      - cron: "0 6 * * 1"

  - name: batch-extract-companies
    entrypoint: flows/batch_extract.py:batch_extract_companies_flow
    work_pool:
      name: default
    parameters:
      input_xlsx: "SourceData/companies.xlsx"
      output_root: "output/companies_extraction"
      schema_profile: "compact_single_call"
      company_type: "generic"
    schedules:
      - cron: "0 8 * * 1"
```

---

## 8. Migration Steps

### Phase 1: Add Prefect Decorators (Minimal Changes)

1. Add `prefect>=3.0` to `requirements.txt`
2. Create `flows/` directory structure
3. Extract functions from `batch_extract_companies.py` into task/flow files with `@task` and `@flow` decorators
4. Thread `company_type` parameter through the flow hierarchy (top-level flow -> subflow -> extract_document task)
5. Keep processing sequential (`max_workers=1`)
6. Keep existing SQLite persistence
7. Keep existing rate limiting mechanism
8. Test: run the flow locally with both `--company-type generic` and `--company-type academy_trust`, verify identical output to current script

**Benefit**: Full Prefect observability (UI dashboard, logs, run history) with minimal risk.

### Phase 2: Add Prefect-Native Features

1. Replace monkey-patched throttle with Prefect `rate_limit()` on CH API tasks
2. Add Prefect Secret blocks for API keys
3. Enable task-level retries (replace some internal retry logic)
4. Add Prefect notifications on flow failure
5. Create deployment with schedule

**Benefit**: Better error handling, secrets management, scheduling.

### Phase 3: Enable Concurrency

1. Use `.submit()` to process multiple companies in parallel
2. Configure tag-based concurrency limits for CH API and OpenRouter
3. Consider switching SQLite to PostgreSQL for concurrent writes
4. Tune `ThreadPoolTaskRunner(max_workers=N)` based on rate limits

**Benefit**: Faster batch processing while respecting external rate limits.

---

## 9. What NOT to Convert

Some components should remain as-is:

- **`CompaniesHouseClient`**: This is a clean, stateless-ish HTTP client. It should not have Prefect decorators. Tasks *call* it; it doesn't become a task itself.
- **`OpenRouterDocumentExtractor`**: Same reasoning. Keep it as a plain class. It already accepts `company_type` via its constructor.
- **`company_type.py`**: Pure enum and config dicts. No orchestration concern.
- **`document_extraction_models.py`**: Pure Pydantic models. No orchestration concern.
- **`test_companies_house_client.py`**: Unit tests should test the client in isolation, not through Prefect.

---

## 10. Dependency Changes

```
# requirements.txt additions
prefect>=3.0
```

Prefect 3.x brings its own dependencies (httpx, pydantic, etc.). Since the project already uses `pydantic>=2.7.0` and `requests>=2.31.0`, there should be no conflicts.

---

## 11. Observability Gains

| Metric | Current | With Prefect |
|---|---|---|
| Run history | SQLite only | Prefect UI + SQLite |
| Per-company status | Print logs | Individual subflow runs in UI |
| Failure diagnosis | Parse error logs | Click-through to failed task with traceback |
| Retry visibility | None | Retry attempts visible in task run timeline |
| Duration tracking | None per-stage | Per-task duration in UI |
| Alerting | None | Prefect notifications (email, Slack, webhook) |
| Scheduling | Manual/cron | Prefect schedules with UI management |

---

## 12. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Prefect server dependency | Use `.serve()` for local mode (no server required) |
| SQLite concurrent writes | Phase 1 stays sequential; Phase 3 considers PostgreSQL |
| Client object serialization | Create clients inside subflows, pass only config strings |
| Increased complexity | Phase 1 is a thin wrapper; complexity grows only as needed |
| Prefect version churn | Pin `prefect>=3.0,<4.0` in requirements |
| Cost of Prefect Cloud | Self-hosted Prefect server is free; Cloud is optional |
