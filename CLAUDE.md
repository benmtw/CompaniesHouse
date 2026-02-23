# CLAUDE.md

## Project Overview

Companies House data extraction pipeline. Batch processes company filings from XLSX input, downloads PDFs via the Companies House API, and extracts structured data using LLMs via OpenRouter. Orchestrated with Prefect 3.x for observability, retry management, and scheduling.

## Environment

- Use the project virtual environment: `.venv`
- Install dependencies: `pip install -r requirements.txt`
- Run tests: `python -m unittest -v test_companies_house_client.py`

## Secrets

- Use `.env` for local secrets (see `.env.example`)
- Required: `CH_API_KEY`, `OPENROUTER_API_KEY`, `OPENROUTER_MODEL`
- Production: Use Prefect Secret blocks (`companies-house-api-key`, `openrouter-api-key`, `openrouter-model`)
- Never hardcode keys in code

## Key Documentation

| File | Purpose | Keep Updated |
|------|---------|:---:|
| `docs/Prefect.md` | How to run and deploy Prefect workflows | Yes |
| `docs/COMPANIES_HOUSE_API.md` | API contract reference | Yes |
| `docs/MATFullTReportFormat.md` | MAT financial report JSON schema reference | -- |
| `AGENTS.md` | Developer environment guidelines | Yes |

Archived documentation (completed plans, incident reports, legacy scripts) is in `docs/archive/`.

## Default Company Type

Always use `--company-type academy_trust` when running batch extractions. This is the default. The academy trust prompts extract `organisation_name` and `organisation_type` fields which name enrichment depends on. Using `generic` mode with enrichment enabled will produce a warning.

## Documentation Maintenance Rules

- When modifying Prefect flows, tasks, parameters, or deployments in `flows/` or `prefect.yaml`, **update `docs/Prefect.md`** in the same commit
- When modifying API client behavior in `companies_house_client.py`, **update `docs/COMPANIES_HOUSE_API.md`**
- When adding new project files or changing environment setup, **update `AGENTS.md`**
- Keep documentation in sync with code -- do not merge code changes without corresponding doc updates

## Code Structure

```
flows/                              # Prefect flows and tasks
    batch_extract.py                # Top-level flow + per-company subflow
    tasks/
        companies_house.py          # API tasks (profile, filing history, document download)
        data_loading.py             # Batch loading, run init/finalize
        extraction.py               # LLM extraction tasks
        persistence.py              # JSON + SQLite persistence
prefect.yaml                       # Deployment configuration
shared.py                          # Shared utilities, DB ops, extraction helpers (used by CLI + flows)
companies_house_client.py           # HTTP API client (not a Prefect task)
openrouter_document_extractor.py    # LLM extraction class (not a Prefect task)
document_extraction_models.py       # Pydantic models for extraction schemas
company_type.py                     # CompanyType enum (GENERIC, ACADEMY_TRUST)
batch_extract_companies.py          # CLI entry point + XLSX reader
test_companies_house_client.py      # Unit tests (mocked HTTP)
docs/                               # Active documentation
    COMPANIES_HOUSE_API.md          # API contract reference
    Prefect.md                      # Prefect workflow guide
    MATFullTReportFormat.md         # MAT financial report schema
    archive/                        # Completed plans, reports, legacy scripts
```

## Testing

- Run: `python -m unittest -v test_companies_house_client.py`
- Tests use mocked HTTP -- no API keys required
- Live API smoke tests are skipped unless `CH_API_KEY` is set
- Tests do not import from `flows/` -- Prefect flow changes should not break tests

## Prefect Setup

Before running flows, create the rate limiting concurrency limit:

```bash
prefect gcl create companies-house-api --limit 1 --slot-decay-per-second 0.5
```

See `docs/Prefect.md` for full setup and deployment instructions.
