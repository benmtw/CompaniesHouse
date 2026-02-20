# CLAUDE.md

## Project Overview

Companies House data retrieval, document download, and LLM-based extraction tooling. Downloads UK company filings (PDFs) via the Companies House API and extracts structured data using OpenRouter-hosted LLMs.

## Environment

- Python virtual environment: `.venv` (never use system Python)
- Run Python: `.\.venv\Scripts\python`
- Install dependencies: `.\.venv\Scripts\python -m pip install -r requirements.txt`
- Platform: Windows (use PowerShell-compatible commands in docs/examples)

## Key Files

| File | Purpose |
|------|---------|
| `companies_house_client.py` | Core API client + compatibility exports |
| `document_extraction_models.py` | Extraction enums and Pydantic models |
| `openrouter_document_extractor.py` | OpenRouter extraction implementation |
| `pipeline_shared.py` | Shared pipeline utilities and throttling |
| `batch_extract_trusts.py` | Batch XLSX → PDF + JSON + SQLite extraction |
| `download_trusts_full_reports.py` | Batch XLSX → latest full-accounts PDF downloads |
| `companies_house_parsl_pipeline.py` | **Primary** Parsl-based CH download + OpenRouter extraction pipeline |
| `parsl_pipeline_apps.py` | Parsl @python_app definitions for download/extract stages |
| `parsl_pipeline_config.py` | Parsl Config factory (ThreadPoolExecutor/HighThroughputExecutor) |
| `companies_house_full_reports_extraction_pipeline_legacy.py` | Legacy ThreadPoolExecutor pipeline (deprecated) |
| `test_companies_house_client.py` | Unit tests (mocked HTTP) + optional live smoke tests |
| `test_parsl_pipeline.py` | Unit tests for Parsl pipeline |
| `COMPANIES_HOUSE_API.md` | API contract reference — keep in sync with code |
| `AGENTS.md` | Extended project documentation and operational notes |

## Secrets & Environment Variables

- Store secrets in `.env` (never hardcode)
- `CH_API_KEY` — Companies House API key (required for download)
- `OPENROUTER_API_KEY` — OpenRouter API key (required for extraction)
- `OPENROUTER_MODEL` — explicit model name (e.g. `google/gemini-2.5-flash-lite`); avoid `openrouter/auto` for cost control

## Testing

```powershell
# Unit tests (mocked HTTP, no network required)
.\.venv\Scripts\python -m unittest -v test_companies_house_client.py

# Parsl tests (Unix/WSL2/macOS only - skipped on Windows)
# On Windows use WSL2 or run: wsl python -m unittest -v test_parsl_pipeline.py
.\.venv\Scripts\python -m unittest -v test_parsl_pipeline.py

# Run all tests
.\.venv\Scripts\python -m unittest discover -v
```

- Unit tests pass without network access (mocked HTTP).
- Parsl tests are **skipped on Windows** (requires fork-based multiprocessing).
- Live smoke tests run only when `CH_API_KEY` is set.
- Live extraction smoke tests run only when `OPENROUTER_API_KEY` is also set.

## API Essentials

- Companies House rate limit: **600 requests per 5-minute window**; `429` means wait for window reset.
- Auth: HTTP Basic with API key as username, empty password.
- Document downloads are cached locally under `output/ch_document_cache/` (keyed by `document_id` + media type).

## Parsl Pipeline Usage

**IMPORTANT**: Parsl requires fork-based multiprocessing and is **NOT compatible with Windows**. Use WSL2, Linux, or macOS to run the Parsl pipeline. On Windows, use the legacy pipeline: `companies_house_full_reports_extraction_pipeline_legacy.py`

The primary pipeline is `companies_house_parsl_pipeline.py` which uses Parsl for parallel execution:

```powershell
# Basic usage with company numbers
.\.venv\Scripts\python companies_house_parsl_pipeline.py --company-numbers 00000006 --mode all

# Download-only with more workers
.\.venv\Scripts\python companies_house_parsl_pipeline.py --input-xlsx Trusts.xlsx --mode download --ch-workers 4

# Extract-only from cached PDFs
.\.venv\Scripts\python companies_house_parsl_pipeline.py --input-xlsx Trusts.xlsx --mode extract --or-workers 8

# Use HighThroughputExecutor (for distributed/cluster)
.\.venv\Scripts\python companies_house_parsl_pipeline.py --company-numbers 00000006 --executor-type htex
```

### Key CLI Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--mode` | `all` | `all`, `download`, or `extract` |
| `--ch-workers` | `2` | Concurrent Companies House API workers |
| `--or-workers` | `4` | Concurrent OpenRouter extraction workers |
| `--executor-type` | `thread` | `thread` (local) or `htex` (distributed) |
| `--parsl-monitoring` | off | Enable Parsl monitoring hub |
| `--schema-profile` | `compact_single_call` | Extraction schema profile |

### Global Throttling

Parsl apps use a run-scoped file-backed throttle state (`ch_throttle_state.json` + file lock) shared across all worker processes/threads. This keeps Companies House pacing global even when running with `--executor-type htex`.

## Conventions

- Raise explicit API errors with status code and URL context.
- Validate required inputs (`query`, `company_number`, `document_id`).
- Keep documentation (`COMPANIES_HOUSE_API.md`, `AGENTS.md`) in sync when behavior, APIs, models, env vars, or examples change.
- When looking up third-party library docs, query the `context7` MCP server before web search.
- Process monitoring: capture PID at start, monitor with `Get-Process -Id <PID>` or `ps -p <PID>` — avoid self-matching `pgrep -f` loops.
