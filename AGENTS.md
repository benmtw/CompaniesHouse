# AGENTS.md

## Scope
This repository manages Companies House data retrieval and document download tooling.

## Environment
- Always use the project virtual environment: `.venv`
- Do not use global/system Python for this repo.

## Python Commands
- Run Python with: `.\.venv\Scripts\python`
- Install deps with: `.\.venv\Scripts\python -m pip install -r requirements.txt`
- Run tests with: `.\.venv\Scripts\python -m unittest -v test_companies_house_client.py`

## Secrets
- Use `.env` for local secrets.
- Required key: `CH_API_KEY`
- Never hardcode keys in code.
- Do not store active keys in `comph.txt`; treat it as sensitive and legacy.

## API Client
Primary client file:
- `companies_house_client.py`

Expected usage:
1. Search companies
2. Fetch company profile
3. Fetch filing history
4. List filing document metadata
5. Download filing documents

## Error Handling Expectations
- Raise explicit API errors with status code and URL context.
- Validate required inputs (`query`, `company_number`, `document_id`).

## Testing Expectations
- Keep unit tests in `test_companies_house_client.py`
- Prefer mocked HTTP for unit tests.
- Live API smoke test is optional and should run only when `CH_API_KEY` is set.

## Project Files to Keep
- `.env`
- `requirements.txt`
- `companies_house_client.py`
- `test_companies_house_client.py`
- `COMPANIES_HOUSE_API.md`
- `AGENTS.md`

## Operational Notes
- Follow `COMPANIES_HOUSE_API.md` as the API contract reference.
- Keep documentation up to date with code changes. If behavior, APIs, models, env vars, or examples change, update `COMPANIES_HOUSE_API.md` and any related docs in the same change.
- Treat `OPENROUTER_MODEL=openrouter/auto` as high-cost mode. Prefer explicitly configured lower-cost models unless the user explicitly requests auto-routing.
- Use Windows PowerShell-compatible commands in docs/examples.
- When looking up information on third-party libraries, query the `context7` MCP server first before attempting web search.
- For live API/integration runs in Codex, request escalated execution (outside sandbox) and run commands directly with `.\.venv\Scripts\python`.
