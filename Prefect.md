# Prefect Workflow Guide

This guide covers how to run, deploy, and monitor the Companies House batch extraction pipeline using Prefect 3.x.

---

## Prerequisites

- Python 3.10+
- Virtual environment: `.venv`
- Install dependencies:
  ```
  pip install -r requirements.txt
  ```
- Environment variables in `.env` (see `.env.example`):
  - `CH_API_KEY` -- Companies House API key (required)
  - `OPENROUTER_API_KEY` -- OpenRouter LLM API key (required)
  - `OPENROUTER_MODEL` -- LLM model name (required, e.g. `google/gemini-2.0-flash-001`)

Alternatively, store secrets as Prefect Secret blocks (see [Secrets Management](#secrets-management)).

---

## Quick Start

### 1. Set up concurrency limits

Create the Prefect global concurrency limits for API rate limiting:

```bash
# Companies House API: max 0.5 req/sec (one request every 2 seconds)
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5

# OpenRouter LLM API: max 3 concurrent extraction calls
prefect gcl create openrouter-llm --limit 3
```

### 2. Run the flow directly

```bash
python -c "
from flows.batch_extract import batch_extract_companies_flow
batch_extract_companies_flow(
    input_xlsx='SourceData/allgroupslinksdata20260217/Trusts.xlsx',
    company_type='academy_trust',
    max_companies=5,
)
"
```

### 3. Or deploy and run via Prefect

```bash
# Start the Prefect server (for local development)
prefect server start

# Deploy all configurations from prefect.yaml
prefect deploy --all

# Start a worker to pick up runs
prefect worker start --pool default

# Trigger a run
prefect deployment run 'batch-extract-companies/batch-extract-trusts'
```

---

## Flow Architecture

```
batch_extract_companies_flow (top-level flow)
    |
    |-- load_and_prepare_batch (task)
    |       Read XLSX, deduplicate company numbers, apply slicing/sampling
    |
    |-- initialize_run (task)
    |       Create output dirs, SQLite tables (WAL mode), insert run row
    |
    |-- CONCURRENT (ThreadPoolExecutor, max_concurrent_companies workers):
    |       process_company_flow (subflow, one per company)
    |           |-- fetch_company_profile (task)    [rate_limit: companies-house-api]
    |           |-- fetch_filing_history (task)     [rate_limit: companies-house-api]
    |           |-- find_latest_full_accounts (task)
    |           |-- download_document (task)        [rate_limit: companies-house-api]
    |           |-- extract_document (task)         [rate_limit: openrouter-llm]
    |           |-- save_results (task)
    |
    |-- finalize_run (task)
            Update run totals, mark finished
```

Companies are processed **concurrently** via a `ThreadPoolExecutor` controlled by the `max_concurrent_companies` parameter (default 4). Each company subflow gets its own `CompaniesHouseClient` instance for thread safety. Rate limiting is enforced at the task level by Prefect global concurrency limits, which properly serialize API access across concurrent subflows.

Each company appears as its own **subflow** in the Prefect UI with individual status, duration, and logs.

---

## Flow Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_xlsx` | str | `SourceData/.../Trusts.xlsx` | Path to input XLSX file with company data |
| `output_root` | str | `output/companies_extraction` | Root directory for output files |
| `model` | str | `""` | OpenRouter model name (overrides `OPENROUTER_MODEL` env var) |
| `max_companies` | int | `0` | Limit number of companies to process (0 = all) |
| `start_index` | int | `0` | Skip this many companies from the start |
| `schema_profile` | str | `compact_single_call` | Extraction schema: `compact_single_call` or `light_core` |
| `company_type` | str | `generic` | Company type: `generic` or `academy_trust` |
| `db_path` | str | `""` | Custom SQLite database path (default: `output_root/companies_house_extractions.db`) |
| `fallback_models` | str | `""` | Comma-separated fallback LLM models |
| `write_summary_json` | bool | `False` | Write a summary JSON file after the run |
| `summary_json_path` | str | `""` | Custom path for summary JSON |
| `filing_history_items_per_page` | int | `100` | Items per page for filing history API calls |
| `retries_on_invalid_json` | int | `2` | Number of retries when LLM returns invalid JSON |
| `random_sample_size` | int | `0` | Randomly sample this many companies (0 = disabled) |
| `random_seed` | int | `0` | Seed for random sampling (0 = random) |
| `max_concurrent_companies` | int | `4` | Max companies processed in parallel (1 = sequential) |

---

## Deployments

Two preconfigured deployments in `prefect.yaml`:

| Deployment | Company Type | Schedule | Input |
|---|---|---|---|
| `batch-extract-trusts` | `academy_trust` | Monday 6:00 AM | `SourceData/.../Trusts.xlsx` |
| `batch-extract-companies` | `generic` | Monday 8:00 AM | `SourceData/companies.xlsx` |

### Deploy

```bash
prefect deploy --all
```

### Run a deployment manually

```bash
prefect deployment run 'batch-extract-companies/batch-extract-trusts'
```

### Start a worker

```bash
prefect worker start --pool default
```

---

## Running Locally with .serve()

For development without deploying to a work pool:

```python
from flows.batch_extract import batch_extract_companies_flow

if __name__ == "__main__":
    batch_extract_companies_flow.serve(
        name="batch-extract-local",
        cron="0 6 * * 1",
        parameters={"company_type": "academy_trust"},
    )
```

---

## Monitoring

### Prefect UI

Start the local Prefect server:

```bash
prefect server start
```

Open http://localhost:4200 to view:

- **Flow runs** -- each batch run with status, duration, and logs
- **Subflow runs** -- each company appears as its own flow run
- **Task runs** -- individual task status within each company (API calls, extraction, persistence)
- **Retry attempts** -- visible in the task run timeline
- **Failure details** -- click through to failed tasks with full tracebacks

### Failure notifications

The top-level flow has an `on_failure` hook that logs a prominent error message. For external notifications (email, Slack, webhook), configure [Prefect Automations](https://docs.prefect.io/latest/automate/) in the UI.

---

## Task Configuration

| Task | File | Retries | Retry Delay | Tags | Timeout |
|---|---|---|---|---|---|
| `fetch-company-profile` | `flows/tasks/companies_house.py` | 2 | 15s | `companies-house-api` | -- |
| `fetch-filing-history` | `flows/tasks/companies_house.py` | 2 | 15s | `companies-house-api` | -- |
| `download-document` | `flows/tasks/companies_house.py` | 2 | 15s | `companies-house-api` | -- |
| `extract-document` | `flows/tasks/extraction.py` | 1 | 30s | `openrouter-llm` | 300s |
| `find-latest-full-accounts` | `flows/tasks/extraction.py` | 0 | -- | -- | -- |
| `load-and-prepare-batch` | `flows/tasks/data_loading.py` | 0 | -- | -- | -- |
| `initialize-run` | `flows/tasks/data_loading.py` | 0 | -- | -- | -- |
| `finalize-run` | `flows/tasks/data_loading.py` | 0 | -- | -- | -- |
| `save-results` | `flows/tasks/persistence.py` | 0 | -- | -- | -- |

---

## Rate Limiting & Concurrency

Two Prefect global concurrency limits control external API access across all concurrent subflows:

| Limit | Purpose | Tasks |
|---|---|---|
| `companies-house-api` | Rate-limit CH API to 0.5 req/sec | `fetch_company_profile`, `fetch_filing_history`, `download_document` |
| `openrouter-llm` | Cap concurrent LLM extractions at 3 | `extract_document` |

### Setup (required before first run)

```bash
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5
prefect gcl create openrouter-llm --limit 3
```

The `companies-house-api` limit enforces one request every 2 seconds (0.5 req/sec), matching the Companies House API rate limit. The `openrouter-llm` limit caps concurrent LLM calls at 3 to control costs. Adjust the OpenRouter limit based on your budget and provider rate limits.

### Verify

```bash
prefect gcl ls
```

---

## Secrets Management

API keys are loaded in this order:

1. **Prefect Secret blocks** (recommended for production):
   ```bash
   prefect block register -m prefect.blocks.system
   # Then create secrets via the Prefect UI or:
   python -c "
   from prefect.blocks.system import Secret
   Secret(value='your-api-key').save('companies-house-api-key')
   Secret(value='your-api-key').save('openrouter-api-key')
   Secret(value='your-model-name').save('openrouter-model')
   "
   ```

2. **Environment variables / `.env` file** (fallback):
   ```
   CH_API_KEY=your-api-key
   OPENROUTER_API_KEY=your-api-key
   OPENROUTER_MODEL=google/gemini-2.0-flash-001
   ```

---

## Error Handling

- **Company-level failures** are caught and recorded in SQLite; the batch continues processing remaining companies
- **Schema fallback**: If the LLM returns a "schema depth" error with `compact_single_call`, the extraction automatically retries with the simpler `light_core` schema
- **Model fallback**: If the primary LLM model rejects file input, fallback models are tried in sequence
- **Prefect retries**: API tasks retry 2 times (15s delay) on transient errors; extraction retries once (30s delay)
- **Flow failure hook**: The `on_failure` hook logs errors prominently; configure Prefect Automations for external alerts

---

## Output Structure

Each run creates:

```
output/companies_extraction/
    companies_house_extractions.db      # SQLite database (shared across runs)
    run_YYYYMMDDTHHMMSSZ/               # Per-run directory
        summary.json                    # (optional) Run summary
        {company_number}/
            api/
                profile.json            # Companies House profile
                filing_history.json     # Filing history
            documents/
                {company_number}_latest_full_accounts_{doc_id}.pdf
            extraction/
                extraction_result.json  # LLM extraction output
                validation_warnings.json
                run_report.json         # Per-company run metadata
```

---

## Troubleshooting

### "No global concurrency limit found"

Create the concurrency limits before running:
```bash
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5
prefect gcl create openrouter-llm --limit 3
```

### "Missing CH_API_KEY" or "Missing OPENROUTER_API_KEY"

Set API keys either as Prefect Secret blocks or in a `.env` file. See [Secrets Management](#secrets-management).

### "No full accounts document found"

The company has no filing with type `AA` (annual accounts) and description containing "full". This is expected for some companies.

### Flow stuck or slow

Check the Prefect UI for task-level status. Common causes:
- Rate limiting: API tasks wait for concurrency slots (expected behavior)
- LLM extraction: large PDFs can take up to 300s (the task timeout)
- Network issues: API tasks retry with 15s delay

### Tests failing after changes

Run the test suite:
```bash
python -m unittest -v test_companies_house_client.py
```

Tests do not import from `flows/` and should not be affected by Prefect flow changes.
