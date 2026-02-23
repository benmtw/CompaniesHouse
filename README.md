# Companies House Extraction Pipeline

Batch processes UK company filings from XLSX input, downloads PDFs via the [Companies House API](https://developer.company-information.service.gov.uk/), and extracts structured data using LLMs via [OpenRouter](https://openrouter.ai/). Orchestrated with [Prefect 3.x](https://docs.prefect.io/) for observability, retry management, and scheduling.

## Features

- **Batch processing** -- reads company numbers from XLSX spreadsheets and processes them in parallel
- **Companies House API client** -- fetches company profiles, filing histories, officers, and downloads filing documents (PDFs)
- **Two modes**: `extract` (LLM-powered document extraction) and `personnel` (current officers lookup, no LLM needed)
- **Personnel caching** -- officer lookups are cached with configurable TTL (default 7 days) to minimise API calls
- **LLM-powered extraction** -- sends downloaded PDFs to OpenRouter for structured data extraction (personnel, financials, governance, staffing)
- **Academy Trust (MAT) support** -- specialised extraction for Multi-Academy Trust annual reports (SOFA, balance sheets, fund breakdowns)
- **Prefect orchestration** -- concurrent company processing, rate limiting, secret management, and failure hooks
- **SQLite persistence** -- per-run tracking with full extraction results, validation warnings, and run metadata

## Installing as a Dependency

To use this package in another project, install it directly from GitHub with pip:

```bash
# Install from the default branch
pip install git+https://github.com/benmtw/CompaniesHouse.git

# Install a specific branch
pip install git+https://github.com/benmtw/CompaniesHouse.git@branch-name

# Install a specific tag or commit
pip install git+https://github.com/benmtw/CompaniesHouse.git@v0.1.0
pip install git+https://github.com/benmtw/CompaniesHouse.git@abc1234
```

### Private repo authentication

This is a private repository. You need to authenticate with GitHub to install it.

**Option A -- Personal Access Token (PAT) via HTTPS (recommended)**

1. Create a GitHub Personal Access Token at https://github.com/settings/tokens with `repo` scope.
2. Configure git to use the token so pip can clone the repo:

```bash
git config --global url."https://<YOUR_GITHUB_TOKEN>@github.com/".insteadOf "https://github.com/"
```

Then install normally:

```bash
pip install git+https://github.com/benmtw/CompaniesHouse.git
```

Alternatively, embed the token directly in the URL (useful for CI/Docker):

```bash
pip install git+https://<YOUR_GITHUB_TOKEN>@github.com/benmtw/CompaniesHouse.git
```

**Option B -- SSH**

If you have an SSH key registered with GitHub:

```bash
pip install git+ssh://git@github.com/benmtw/CompaniesHouse.git
```

### Adding to your project's dependencies

In `requirements.txt`:

```
# HTTPS (token must be configured via git config or environment)
companies-house-extraction @ git+https://github.com/benmtw/CompaniesHouse.git

# SSH
companies-house-extraction @ git+ssh://git@github.com/benmtw/CompaniesHouse.git
```

Or in `pyproject.toml`:

```toml
dependencies = [
    "companies-house-extraction @ git+https://github.com/benmtw/CompaniesHouse.git",
]
```

> **Note:** Never commit a token directly in `requirements.txt` or `pyproject.toml`. Use `git config --global url...insteadOf` or set the `GIT_AUTH_TOKEN` environment variable in your CI pipeline instead.

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/benmtw/CompaniesHouse.git
cd CompaniesHouse
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .\.venv\Scripts\activate  # Windows PowerShell
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your API keys:

```
CH_API_KEY=your_companies_house_api_key
OPENROUTER_API_KEY=your_openrouter_api_key      # Only needed for extract mode
OPENROUTER_MODEL=google/gemini-2.5-flash-lite   # Only needed for extract mode
```

Get a Companies House API key at https://developer.company-information.service.gov.uk/.

> **Cost warning:** avoid setting `OPENROUTER_MODEL=openrouter/auto` as it can route to expensive models. Set an explicit model.

### 3. Run the CLI

**Extract mode** (document extraction with LLM):

```bash
python batch_extract_companies.py \
  --mode extract \
  --input-xlsx "SourceData/Trusts.xlsx" \
  --output-root "output/trusts_extraction" \
  --model "google/gemini-2.5-flash-lite" \
  --max-companies 5
```

**Personnel mode** (officers only, no LLM needed):

```bash
python batch_extract_companies.py \
  --mode personnel \
  --input-xlsx "SourceData/Trusts.xlsx" \
  --output-root "output/personnel" \
  --max-companies 5
```

### 4. Run with Prefect (optional)

```bash
# Create rate limiting concurrency limit
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5

# Run the flow
python -m flows.batch_extract
```

See [docs/Prefect.md](docs/Prefect.md) for full setup, deployment, and monitoring instructions.

## CLI Options

| Flag | Description |
|------|-------------|
| `--mode` | `extract` (default) or `personnel` -- see [Modes](#modes) below |
| `--input-xlsx` | Path to XLSX file with company numbers |
| `--output-root` | Root directory for output files |
| `--model` | OpenRouter model to use (extract mode only) |
| `--max-companies N` | Limit to first N companies |
| `--start-index N` | Resume from offset in the company list |
| `--random-sample-size N` | Process a random sample |
| `--random-seed N` | Reproducible random sampling |
| `--fallback-models "a,b"` | Comma-separated fallback model list |
| `--schema-profile` | `compact_single_call` (default), `full_legacy`, or `light_core` |
| `--write-summary-json` | Emit run-level summary.json |
| `--retries-on-invalid-json N` | Retries on malformed LLM JSON (default 3) |
| `--personnel-cache-dir` | Cache directory for personnel lookups (default: `output/personnel_cache`) |
| `--personnel-cache-ttl-days` | Days before cached personnel is stale (default: 7, set 0 to disable) |

## Modes

### Extract Mode (default)

Downloads filing documents (PDFs) from Companies House and runs LLM-powered extraction via OpenRouter. Requires `CH_API_KEY`, `OPENROUTER_API_KEY`, and `OPENROUTER_MODEL`.

```bash
python batch_extract_companies.py \
  --mode extract \
  --input-xlsx "SourceData/Trusts.xlsx" \
  --output-root "output/trusts_extraction" \
  --model "google/gemini-2.5-flash-lite" \
  --max-companies 5
```

### Personnel Mode

Fetches current company officers (directors, secretaries, etc.) directly from the Companies House API. **No OpenRouter credentials required** -- only `CH_API_KEY`.

Officers are cached locally to avoid repeated API calls. The cache stores each company's officers with a timestamp, and respects a configurable TTL (default 7 days). Since officer data can change over time (unlike historical documents), the cache expires automatically.

```bash
python batch_extract_companies.py \
  --mode personnel \
  --input-xlsx "SourceData/Trusts.xlsx" \
  --output-root "output/personnel" \
  --personnel-cache-ttl-days 7 \
  --max-companies 5
```

**Personnel mode options:**

| Flag | Description |
|------|-------------|
| `--personnel-cache-dir` | Directory for cached officer data (default: `output/personnel_cache`) |
| `--personnel-cache-ttl-days` | Cache expiry in days. Set to `0` to disable caching and always hit the API |

Each officer record contains: `first_name`, `middle_names`, `last_name`, `role`, `appointed_on`, `date_of_birth` (month/year only -- API restriction), and `correspondence_address`.

## Output

### Extract Mode

Each run creates a timestamped directory under `--output-root`:

```
output/trusts_extraction/run_<UTCSTAMP>/
  <company_number>/
    documents/    # Downloaded PDFs
    api/          # Profile and filing history JSON
    extraction/   # Extraction results, warnings, run report
  summary.json    # Optional run summary
```

Results are also persisted to SQLite (`companies_house_extractions.db`).

### Personnel Mode

```
output/personnel/run_<UTCSTAMP>/
  <company_number>/
    officers.json       # Current officers for this company
  personnel_summary.json  # Run summary with cache stats

output/personnel_cache/
  <company_number>.json  # Cached officer data with fetched_at timestamp
```

The `personnel_summary.json` includes `cache_hits` and `api_calls` counts so you can see how many companies were served from cache vs fresh API lookups.

## Testing

```bash
python -m unittest -v test_companies_house_client.py
```

Tests use mocked HTTP -- no API keys required. Live smoke tests run only when `CH_API_KEY` is set.

## Project Structure

```
batch_extract_companies.py          # CLI entry point
companies_house_client.py           # Companies House HTTP API client
openrouter_document_extractor.py    # LLM extraction via OpenRouter
document_extraction_models.py       # Pydantic models for extraction schemas
company_type.py                     # CompanyType enum (GENERIC, ACADEMY_TRUST)
shared.py                           # Shared utilities and DB operations
test_companies_house_client.py      # Unit tests

flows/                              # Prefect workflows
    batch_extract.py                # Top-level flow + per-company subflow
    tasks/                          # Prefect task modules

docs/                               # Documentation
    COMPANIES_HOUSE_API.md          # API contract reference
    Prefect.md                      # Prefect workflow guide
    MATFullTReportFormat.md         # MAT financial report schema
    archive/                        # Historical plans and reports
```

## Documentation

| Document | Description |
|----------|-------------|
| [Companies House API Reference](docs/COMPANIES_HOUSE_API.md) | API endpoints, authentication, rate limiting, extraction behaviour |
| [Prefect Workflow Guide](docs/Prefect.md) | Setup, deployments, parameters, monitoring |
| [MAT Report Format](docs/MATFullTReportFormat.md) | JSON schema for Academy Trust financial reports |

## Dependencies

- [requests](https://docs.python-requests.org/) -- HTTP client
- [pydantic](https://docs.pydantic.dev/) -- Data validation and extraction models
- [json-repair](https://github.com/mangiucugna/json_repair) -- Fallback JSON parsing for malformed LLM output
- [prefect](https://docs.prefect.io/) -- Workflow orchestration (3.x)

## License

This project is not currently licensed for redistribution.
