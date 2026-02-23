# Companies House Extraction Pipeline

Batch processes UK company filings from XLSX input, downloads PDFs via the [Companies House API](https://developer.company-information.service.gov.uk/), and extracts structured data using LLMs via [OpenRouter](https://openrouter.ai/). Orchestrated with [Prefect 3.x](https://docs.prefect.io/) for observability, retry management, and scheduling.

## Features

- **Batch processing** -- reads company numbers from XLSX spreadsheets and processes them in parallel
- **Companies House API client** -- fetches company profiles, filing histories, and downloads filing documents (PDFs)
- **LLM-powered extraction** -- sends downloaded PDFs to OpenRouter for structured data extraction (personnel, financials, governance, staffing)
- **Academy Trust (MAT) support** -- specialised extraction for Multi-Academy Trust annual reports (SOFA, balance sheets, fund breakdowns)
- **Prefect orchestration** -- concurrent company processing, rate limiting, secret management, and failure hooks
- **SQLite persistence** -- per-run tracking with full extraction results, validation warnings, and run metadata

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
OPENROUTER_API_KEY=your_openrouter_api_key
OPENROUTER_MODEL=google/gemini-2.5-flash-lite
```

Get a Companies House API key at https://developer.company-information.service.gov.uk/.

> **Cost warning:** avoid setting `OPENROUTER_MODEL=openrouter/auto` as it can route to expensive models. Set an explicit model.

### 3. Run the CLI

```bash
python batch_extract_companies.py \
  --input-xlsx "SourceData/Trusts.xlsx" \
  --output-root "output/trusts_extraction" \
  --model "google/gemini-2.5-flash-lite" \
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
| `--input-xlsx` | Path to XLSX file with company numbers |
| `--output-root` | Root directory for output files |
| `--model` | OpenRouter model to use |
| `--max-companies N` | Limit to first N companies |
| `--start-index N` | Resume from offset in the company list |
| `--random-sample-size N` | Process a random sample |
| `--random-seed N` | Reproducible random sampling |
| `--fallback-models "a,b"` | Comma-separated fallback model list |
| `--schema-profile` | `compact_single_call` (default), `full_legacy`, or `light_core` |
| `--write-summary-json` | Emit run-level summary.json |
| `--retries-on-invalid-json N` | Retries on malformed LLM JSON (default 3) |

## Output

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
