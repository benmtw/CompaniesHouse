# JSON Output Issues

## Scope
This note documents OpenRouter extraction instability observed when requesting too many data sections in one structured-output call, with emphasis on schema depth/complexity behavior.

## Observed Behavior
- Model: `google/gemini-2.5-flash-lite`
- Primary symptom in batch logs: `OpenRouter response did not include text content`
- Actual provider payload for failing cases can be an error object (no `choices`) containing:
  - `A schema in GenerationConfig in the request exceeds the maximum allowed nesting depth.`

### Concrete Evidence
- Failure run (company `07846852`):
  - `output/trusts_extraction/run_20260219T131145Z/07846852/extraction/raw_openrouter_response.json`
  - Response contains provider error (`Google AI Studio`) with schema-depth rejection.
- Same company, reduced request sets succeed:
  - Personnel only: `output/trusts_extraction/probe_personnel_only_20260219T163806Z/parsed_result.json`
  - Detailed balance sheet only: `output/trusts_extraction/probe_detailed_balance_only_20260219T163939Z/parsed_result.json`
  - 4-type bundle (`personnel_details`, `metadata`, `balance_sheet`, `detailed_balance_sheet`):
    - `output/trusts_extraction/probe_4types_20260219T165031Z/parsed_result.json`
- Random sample with the same 4-type bundle:
  - `output/trusts_extraction/probe_random10_4types_20260219T165403Z/summary.json`
  - Outcome: 9 success, 1 failure (non-LLM reason: no full accounts filing found)

## Why This Happens
1. Structured output schema gets deep/large as sections are combined.
2. Provider validators can reject some requests with nesting-depth errors.
3. Error payloads may come back in non-completion shape (`error` object, no `choices`), which previously surfaced as generic "no text content".
4. Behavior is not perfectly deterministic across requests/providers, so the same profile can pass for one company and fail for another request.

## Current Schema Complexity Snapshot (Single Section Requests)
Depth here is measured on `response_format.json_schema.schema`.

1. `personnel_details`: depth 7
2. `metadata`: depth 7
3. `balance_sheet`: depth 8
4. `detailed_balance_sheet`: depth 9
5. `governance`: depth 10
6. `staffing_data`: depth 10
7. `statement_of_financial_activities`: depth 13
8. `academy_trust_annual_report`: depth 15

For context, `compact_single_call` (multi-section) schema body measured depth 13 in this codebase.

## Practical Guidance
- Prefer smaller section bundles for reliability.
- For high-throughput production runs, a stable 4-type bundle is currently:
  - `personnel_details`, `metadata`, `balance_sheet`, `detailed_balance_sheet`
- Treat `statement_of_financial_activities` and `academy_trust_annual_report` as high-complexity sections.
- Keep `--schema-profile compact_single_call` with adaptive fallback to `light_core` enabled (already in batch script) for safer operation.

## Debugging Guidance
Use batch flag to persist full OpenRouter artifacts per attempt:

```powershell
.\.venv\Scripts\python .\batch_extract_trusts.py `
  --model "google/gemini-2.5-flash-lite" `
  --write-summary-json `
  --write-openrouter-debug-artifacts
```

Artifacts are written under:
- `output\trusts_extraction\run_<UTCSTAMP>\<company>\extraction\openrouter_debug\<model>_attemptN\openrouter_request_payload.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company>\extraction\openrouter_debug\<model>_attemptN\response_format.json_schema.schema.json`
- `output\trusts_extraction\run_<UTCSTAMP>\<company>\extraction\openrouter_debug\<model>_attemptN\raw_openrouter_response.json`

## Recommendations
1. Continue using explicit model (`google/gemini-2.5-flash-lite`), not auto-routing.
2. Keep debug artifacts on for investigation runs.
3. Split high-complexity extraction into separate passes when provider depth errors recur.
4. Optionally add a dedicated CLI profile for the proven 4-type bundle to make this mode first-class.
