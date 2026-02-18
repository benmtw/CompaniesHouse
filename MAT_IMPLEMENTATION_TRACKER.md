# MAT Implementation Tracker

## Scope
Implement MAT full report extraction defined in `MATFullTReportFormat.md` using phased delivery to avoid context exhaustion and reduce regression risk.

## Status Summary
- Session 1 status: Completed
- Session 2 status: Completed
- Session 3 status: Completed

## Phased Checklist
- [x] Phase 1: Expand extraction models/enums for MAT schema sections.
- [x] Phase 1: Wire extractor response schema generation for MAT section types.
- [x] Phase 1: Add/update unit tests for new schema/model wiring.
- [x] Phase 2: Add normalization/parsing rules for numeric and accounting formatting.
- [x] Phase 2: Add reconciliation checks and validation warning reporting.
- [x] Phase 2: Add client convenience wrapper(s) for full MAT report extraction flow.
- [x] Phase 3: Update `COMPANIES_HOUSE_API.md` and examples for new extraction types.
- [x] Phase 3: Final smoke validation guidance and known limitations.

## Session 1 Changes
- Added MAT extraction types in `document_extraction_models.py`:
  - `Metadata`
  - `Governance`
  - `StatementOfFinancialActivities`
  - `DetailedBalanceSheet`
  - `StaffingData`
  - `AcademyTrustAnnualReport`
- Added MAT Pydantic models in `document_extraction_models.py`:
  - `Metadata`, `TrusteeAttendance`, `Governance`
  - `FundBreakdown`
  - `StatementOfFinancialActivitiesIncome`, `StatementOfFinancialActivitiesExpenditure`, `StatementOfFinancialActivities`
  - `DetailedBalanceSheetCurrentAssets`, `DetailedBalanceSheetLiabilities`, `DetailedBalanceSheet`
  - `HighPayBand`, `StaffingData`
  - `AcademyTrustAnnualReport`
- Extended `ExtractionResult` with MAT outputs:
  - `metadata`, `governance`, `statement_of_financial_activities`
  - `detailed_balance_sheet`, `staffing_data`, `academy_trust_annual_report`
- Extended schema and parsing in `openrouter_document_extractor.py`:
  - Added prompt task lines for all new MAT extraction types.
  - Added strict response schema builders for each MAT section and full MAT report.
  - Added parse helpers for all new MAT payload objects.
- Updated exports in `companies_house_client.py` for new models.
- Added unit tests in `test_companies_house_client.py` for:
  - `Metadata` extraction path
  - `AcademyTrustAnnualReport` extraction path
  - Individual MAT section schema wiring

## Key Design Decisions
- Kept legacy `ExtractionType.BalanceSheet` unchanged as line-item array output (`balance_sheet`) for backward compatibility.
- Added `ExtractionType.DetailedBalanceSheet` using `detailed_balance_sheet` key to avoid collision with legacy `balance_sheet`.
- Full MAT object extraction uses `academy_trust_annual_report` with nested `balance_sheet` to align report structure.
- Session 1 intentionally excludes normalization/reconciliation logic (deferred to Phase 2).

## Verification
- Command run:
  - `.\.venv\Scripts\python -m unittest -v test_companies_house_client.py`
- Result:
  - Passed: 28
  - Skipped: 2 live smoke tests (missing `CH_API_KEY`)
  - Failed: 0

## Session 2 Changes
- Added accounting/numeric normalization in `document_extraction_models.py` for MAT fields:
  - Handles commas, currency symbols (`£/$/€`), parenthesized negatives, and null-like placeholders (`-`, `—`, `N/A`).
  - Applied to fund breakdowns, detailed balance sheet numeric fields, staffing totals/headcount, trustee attendance counts, and high-pay-band counts.
- Extended `ExtractionResult` with `validation_warnings: list[str]`.
- Added reconciliation checks in `openrouter_document_extractor.py`:
  - SOFA component sum vs `total` warnings at row level.
  - Detailed balance sheet computed net assets vs reported `net_assets` warning.
  - Cross-section warning when both top-level `detailed_balance_sheet` and `academy_trust_annual_report.balance_sheet` are present but differ.
- Added client convenience wrapper in `companies_house_client.py`:
  - `extract_latest_mat_annual_report(...)` delegates to `extract_latest_full_accounts(...)` with `ExtractionType.AcademyTrustAnnualReport`.
- Added/updated unit tests in `test_companies_house_client.py` for:
  - MAT accounting-format numeric normalization.
  - Reconciliation warning generation.
  - Wrapper method wiring.

## Session 3 Changes
- Updated `COMPANIES_HOUSE_API.md` Section 11 with MAT extraction coverage:
  - Added all MAT `ExtractionType` values and output field mapping.
  - Documented `extract_latest_mat_annual_report(...)` convenience wrapper.
  - Documented `ExtractionResult.validation_warnings` semantics and reconciliation checks.
  - Added explicit reconciliation tolerance note (`abs(diff) > 1`).
- Added MAT full-report PowerShell example using a specific lower-cost default model.
- Added final smoke-test and validation guidance in new `COMPANIES_HOUSE_API.md` Section 12:
  - Unit test command
  - Expected skip behavior for missing live credentials
  - Live smoke prerequisites and execution flow
  - Known limitations and warning behavior

## Final Validation
- Command run:
  - `.\.venv\Scripts\python -m unittest -v test_companies_house_client.py`
- Result:
  - Ran: 34
  - Passed: 32
  - Skipped: 2 live smoke tests (`CH_API_KEY` not set)
  - Failed: 0

## Known Limitations
- `validation_warnings` are non-fatal and currently informational only.
- Reconciliation checks intentionally use tolerance `abs(diff) > 1`.
- Reconciliation warning generation requires all relevant numeric components to be present.
- Extraction quality remains dependent on source document quality and model behavior.

## Final Status
- MAT implementation phases complete (Phase 1-3 all checked).
- Tracker closed; remaining work is optional model tuning and live-run operations with valid keys.
