# JSON Structured Output Issues: Incident Report And Action Plan

## Scope

This report covers extraction failures observed while processing Companies House trust filings with:
- Companies House retrieval via `batch_extract_trusts.py`
- OpenRouter extraction using `OPENROUTER_MODEL=google/gemini-2.5-flash-lite`

It documents what failed, what was tested, what changed in code, and practical options going forward.

## Executive Summary

There are two separate reliability problems:

1. **Companies House rate limiting (`429`)**
- This was reproducible and mitigated by pacing requests.
- At 25% of documented max rate (1 request every 2 seconds), previously failing trusts succeeded through PDF download.

2. **OpenRouter structured output failures (primary blocker now)**
- Frequent provider-side schema rejection (`HTTP 400`):
  - `A schema in GenerationConfig in the request exceeds the maximum allowed nesting depth.`
- Additional model output issues:
  - `OpenRouter response did not include text content`
  - `OpenRouter response was not valid JSON` (often truncated/incomplete output)

Current status:
- CH pacing is in place and materially improved CH-side reliability.
- OpenRouter/Gemini structured-output reliability remains the main failure mode in high-volume runs.

## Evidence And Timeline

### Key runs

- **Run 5** (`output\\trusts_extraction\\run_20260218T163154Z`)
  - 10 random trusts, 0 success / 10 failed.
  - Failures included CH `429` and OpenRouter structured-output issues.

- **CH-only paced probe** (`output\\trusts_extraction\\ch_probe_25pct_20260218T165602Z\\summary.json`)
  - Re-tested the 4 prior CH-429 trusts at 2.0s min request interval.
  - 4/4 succeeded to `download_ok`.
  - Confirms CH failures were pacing/scheduling related.

- **Run 12** (`output\\trusts_extraction\\run_20260218T175018Z`)
  - 50 trusts, completed: 7 success / 43 failed.
  - Failure category counts (failed rows):
    - `schema_depth_400`: 16
    - `no_text_content`: 14
    - `invalid_json`: 12
    - `other` (no full accounts): 1

- **Run 13** (`run_20260218T191855Z`) single-company retry `07318714`
  - Failed with schema-depth `HTTP 400`.

- **Run 14** (`run_20260218T193434Z`) single-company retry `07318714` with compact profile
  - Still failed with schema-depth `HTTP 400`.

## Root-Cause Analysis

### A) Companies House `429`

Cause:
- Request volume/spikes from filing + metadata retrieval can exceed practical rate windows.

Evidence:
- Same trust set that failed with 429 later succeeded when paced at 2.0s/request.

Conclusion:
- CH side requires enforced global pacing in batch mode.

### B) OpenRouter/Gemini structured output failures

#### 1) Schema depth rejection (`HTTP 400`)

Error:
- `A schema in GenerationConfig in the request exceeds the maximum allowed nesting depth.`

What this means:
- Provider rejects schema **before generation**, so no model output is produced.
- This is not recoverable by JSON repair.

Measured schema complexity (local):
- `full_legacy`: chars `9848`, depth `14`
- `compact_single_call`: chars `5377`, depth `12`
- `light_core`: chars `1887`, depth `9`

Observation:
- Even reduced `compact_single_call` still hit schema-depth rejection for some documents/routes.

#### 2) `did not include text content`

Cause:
- OpenRouter completion payload may not return parseable `message.content` text in the shape current parser expects.

Impact:
- Extraction fails despite request returning.

#### 3) `not valid JSON`

Cause:
- Model returns malformed/truncated JSON payload.

Notes:
- `json-repair` support exists in code now, but only helps if provider returns fixable text payload.
- It does **not** help schema-depth 400 errors.

## What We Tried

1. Added CH throttling probe at 25% rate
- Result: CH 429 issue validated and mitigated.

2. Added default CH pacing into main batch
- Default `--ch-min-request-interval-seconds 2.0`.

3. Added retry/backoff defaults in client
- `max_retries_on_429=3`, `retry_backoff_seconds=10`, `respect_retry_after=True`.

4. Added JSON repair fallback in extractor
- Strict parse first, then `json_repair.loads(..., skip_json_loads=True)`.

5. Installed `json-repair` in `.venv`
- Confirmed importable and active for later runs.

6. Reduced CH request volume per company
- Switched to first-page filing-history strategy in batch path.

7. Added run summary telemetry
- `summary.json` with per-company status and metrics.
- Included `pdf_size_bytes` and `approx_llm_tokens`.

8. Fixed summary bug
- On failures after `download_ok`, summary now preserves `document_id`/`pdf_path`/size/token fields.

9. Added schema profile support
- `--schema-profile compact_single_call|full_legacy|light_core`
- Default: `compact_single_call`.

10. Added retry control for malformed JSON
- `--retries-on-invalid-json`.

## Current Code-State Changes (Implemented)

- `batch_extract_trusts.py`
  - CH request pacing and controls.
  - First-page filing-history retrieval mode.
  - Run summary JSON controls.
  - `pdf_size_bytes` and `approx_llm_tokens` metrics.
  - `schema_profile` support.
  - Failure summary carry-forward fix after `download_ok`.

- `openrouter_document_extractor.py`
  - `json-repair` fallback path for malformed JSON text.

- `companies_house_client.py`
  - More practical 429 retry defaults.

- `COMPANIES_HOUSE_API.md`
  - Updated for pacing, summary output, schema profile, and retry controls.

## Why This Is Still Failing Often

The dominant remaining issue is provider schema validation and response-shape variability on this model/provider route.

Even after reducing schema complexity:
- Some requests still fail at provider schema-depth checks.
- Some generated responses still omit text content or return malformed/truncated JSON.

This indicates the current single-call broad schema remains too ambitious for a significant fraction of calls on this route.

## Practical Options (Ranked)

### Option 1 (recommended): Keep single-call preference, add adaptive fallback by profile

Flow:
1. Try `compact_single_call`.
2. If schema-depth 400, retry once with `light_core` (still single call).
3. Optionally perform one secondary call only for missing financial sections when needed.

Pros:
- Keeps calls low by default.
- Handles schema-depth hard failures deterministically.

Cons:
- Sometimes needs a second call for full coverage.

### Option 2: Stay strict single-call, switch model/provider route

Pros:
- No multi-call complexity.

Cons:
- Model change may alter cost/quality/latency.
- Needs controlled A/B validation.

### Option 3: Force `light_core` only for bulk runs

Pros:
- Highest stability with one call.

Cons:
- Reduced output breadth unless post-processing or extra calls are added.

### Option 4: Full split extraction (many calls)

Pros:
- Best reliability per section.

Cons:
- High cost/latency; user preference is to avoid this unless necessary.

## Recommended Next Steps

1. [x] Implemented adaptive profile fallback in batch (minimal-call strategy):
- `compact_single_call` -> fallback to `light_core` on schema-depth error only.

2. Add explicit response-shape diagnostics in summary:
- Capture provider error category and parse stage for each failure.

3. Re-run 50 trusts with:
- CH pacing enabled (2.0s)
- adaptive profile fallback enabled
- compare success rate and calls/trust against run 12 baseline.

4. If schema-depth still dominates:
- test one alternative explicit model route for structured output robustness.

## Appendix: Known Baseline Artifacts

- CH probe success: `output\\trusts_extraction\\ch_probe_25pct_20260218T165602Z\\summary.json`
- Random run report: `LIVE_RANDOM_TEST_RUN_5.md`
- CH pacing report: `LIVE_CH_RATE_PROBE_25PCT.md`
- 50-run summary baseline: `output\\trusts_extraction\\run_20260218T175018Z\\summary.json`

