# Phase 1 Code Review: MAT Extraction Models & Pipeline

**Date:** 2026-02-22
**Tests:** 38 ran, 36 passed, 2 skipped (live API tests), 0 failed.

## Overall Assessment

The codebase is well-structured for an extraction pipeline. The phased delivery
approach was sound. The core abstractions — Pydantic models for validation,
enum-driven extraction types, JSON Schema for LLM response formatting — are
appropriate choices. There are, however, several issues ranging from bugs to
design concerns that should be addressed.

---

## Bugs & Correctness Issues

### 1. Metadata validator ordering allows empty required fields

**File:** `document_extraction_models.py:112-131`

The `Metadata` model has `_optional_trimmed` listed *before* `_required_non_empty`
for the same fields (`trust_name`, `company_registration_number`,
`financial_year_ending`). Pydantic v2 runs field validators in decoration order.
`_optional_trimmed` on a required field like `trust_name` will convert `""` →
`None`, and then `_required_non_empty` receives `None` and calls
`str(None or "").strip()` which is `""`, correctly raising. However, the intent
is confusing — two stacked validators partially overlap.

The same pattern is duplicated in `CompanyMetadata` (lines 301-327). A cleaner
approach would be a single validator per field group rather than stacking two
validators that partially overlap.

### 2. `_build_prompts` default parameter contradicts `__init__` default

**File:** `openrouter_document_extractor.py:138`

`_build_prompts` has `company_type: CompanyType = CompanyType.ACADEMY_TRUST` as
its default, while `OpenRouterDocumentExtractor.__init__` defaults to
`CompanyType.GENERIC`. This default is never actually used (the instance method
always passes `self.company_type`), but it's misleading. If `_build_prompts`
were ever called directly as a static method without an explicit `company_type`
argument, it would silently default to `ACADEMY_TRUST` rather than `GENERIC`.

The same issue exists for `_build_response_format` at line 241.

### 3. `company_registration_number` validator is too strict

**File:** `document_extraction_models.py:133-138`

The `_company_number_must_be_8_digits` validator requires exactly 8 digits
(`value.isdigit()`). UK Companies House numbers can have alphabetic prefixes
(e.g., `SC123456` for Scottish companies, `NI012345` for Northern Irish,
`OC300001` for LLPs). This will reject valid non-academy-trust company numbers
when the `CompanyMetadata` model is used for generic companies.

### 4. Balance sheet reconciliation formula is incomplete

**File:** `openrouter_document_extractor.py:940-964`

`_reconcile_balance_sheet` computes:
`fixed_assets + debtors + cash_at_bank - creditors_within_one_year - pension_scheme_liability`.
Real balance sheets can have additional current asset/liability lines not
captured in this model (e.g., investments, long-term creditors, provisions). The
model only captures a subset of what appears on typical balance sheets, so the
reconciliation will generate false-positive warnings when companies have items
in categories not modelled. The tolerance of `> 1` (£1) is very tight for an
inherently incomplete model.

### 5. `_is_full_accounts_filing` has fragile description matching

**File:** `batch_extract_companies.py:254-259`

The check `"full" in description` is overly broad. A description like
"Accounts with accounts type: unaudited full" or "Full list of members" could
false-positive. The more specific `"accounts-with-accounts-type-full"` check is
better, but the fallback to just `"full"` weakens it.

---

## Design Concerns

### 6. Massive code duplication between academy trust and generic models

`Metadata` vs `CompanyMetadata`, `TrusteeAttendance` vs `DirectorAttendance`,
`Governance` vs `CompanyGovernance`, `AcademyTrustAnnualReport` vs
`AnnualReport` — these are near-identical classes differing only in field names
(`trust_name` vs `company_name`, `trustees` vs `directors`). This doubles the
surface area for bugs and maintenance. A shared base class with a configurable
name field, or a factory pattern, would halve the model code.

### 7. JSON Schema builders are hand-maintained, divergence risk from Pydantic models

The `_schema_for_*` static methods in `openrouter_document_extractor.py`
(lines 349-596) manually construct JSON Schema dicts. These must be kept in sync
with the Pydantic models in `document_extraction_models.py`. There's no
automated check that they match. Pydantic v2 can generate JSON Schema via
`Model.model_json_schema()`. Using that as the source of truth (with any
OpenRouter-specific adjustments applied programmatically) would eliminate the
sync risk.

### 8. `_fund_breakdown` schema missing `required` array

**File:** `openrouter_document_extractor.py:350-360`

The `_schema_for_fund_breakdown()` returns a schema with
`additionalProperties: false` but no `required` list. With `strict: true` in
the outer `json_schema` config, some LLM providers require `required` to be
explicitly specified when `additionalProperties` is false. This could cause
schema validation failures with certain models.

### 9. `companies_house_client.py` re-exports too much

**File:** `companies_house_client.py:531-551`

The `__all__` in `companies_house_client.py` re-exports 20 symbols from
`document_extraction_models.py` and `openrouter_document_extractor.py`. The test
file imports them from `companies_house_client` rather than from their source
modules. This creates an unnecessary coupling layer. Imports should come from the
defining module.

### 10. Throttle implementation monkey-patches `session.request`

**File:** `batch_extract_companies.py:216-241`

`_install_companies_house_request_throttle` replaces `client.session.request` at
runtime. This is fragile — if the `CompaniesHouseClient` is refactored to use
the session differently, the throttle silently breaks. The throttle logic would
be better placed inside `_request_with_rate_limit` or as a `requests` Transport
Adapter.

### 11. `AnnualReport` and `AcademyTrustAnnualReport` prompt collision risk

**File:** `openrouter_document_extractor.py:167-170`

Both `AcademyTrustAnnualReport` and `AnnualReport` extraction types use
`profile['annual_report_prompt']` for their task line. If both were ever
requested simultaneously, the LLM prompt would contain two nearly identical
extraction instructions. There's no guard against requesting both.

---

## Test Coverage Gaps

### 12. No unit tests for `_coerce_accounting_float` / `_coerce_accounting_int` edge cases

These are critical normalization functions but are only tested indirectly through
full extraction round-trips. Direct unit tests for edge cases would be valuable:
negative numbers, percentage stripping, bool rejection, mixed currency symbols,
empty-after-cleaning strings, etc.

### 13. No tests for `batch_extract_companies.py`

The entire batch orchestrator — including XLSX parsing, schema profile
selection, model fallback, derived annual report construction, and SQLite
persistence — has zero unit test coverage.

### 14. No tests for `_parse_json_response` robustness

Only markdown code fences and json-repair are tested. Missing: nested braces in
strings, truncated JSON, non-dict JSON (arrays, strings), multiple JSON objects
in response.

### 15. No tests for generic company model validators

The generic company model variants (`CompanyMetadata`, `CompanyGovernance`,
`DirectorAttendance`) were added but only the academy trust `Metadata` model has
a direct extraction test. The generic `AnnualReport` test tests the extraction
path but not validator edge cases.

---

## Minor Issues

### 16. `max_document_chars` is accepted but never used

**File:** `openrouter_document_extractor.py:49,64`

The constructor validates and stores `max_document_chars` but no code ever reads
it. Either implement document truncation or remove the parameter.

### 17. `_utc_now()` truncates microseconds unnecessarily

**File:** `batch_extract_companies.py:183`

`.replace(microsecond=0)` discards useful precision for database timestamps.

### 18. `_load_dotenv_file` has a subtlety with value quoting

**File:** `batch_extract_companies.py:38`

`value.strip().strip("'").strip('"')` will incorrectly handle values like
`"it's complex"` by also stripping the internal apostrophe context. The double
`.strip()` chain removes quotes asymmetrically. A proper approach would check
for matching quotes at start/end.

---

## Recommendations (prioritized)

1. **Fix** the `_build_prompts`/`_build_response_format` default parameter to
   `CompanyType.GENERIC` to match the constructor default.
2. **Relax** the company registration number validator to accept alphanumeric
   prefixes for generic companies, or make the validation conditional on
   `CompanyType`.
3. **Add** a `required` list to the `_schema_for_fund_breakdown()` return value.
4. **Remove or implement** `max_document_chars`.
5. **Add** direct unit tests for `_coerce_accounting_float`/`_coerce_accounting_int`.
6. **Consider** generating JSON schemas from Pydantic models rather than
   maintaining parallel hand-written schemas.
7. **Consider** consolidating the duplicated academy trust / generic model pairs.
