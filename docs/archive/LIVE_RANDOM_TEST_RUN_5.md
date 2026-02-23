# Live Random Test Report (Run 5)

## Test Context

- Run ID: `5`
- Started (UTC): `2026-02-18T16:31:54+00:00`
- Finished (UTC): `2026-02-18T16:35:24+00:00`
- Source file: `SourceData/allgroupslinksdata20260217/Trusts.xlsx`
- Output directory: `output/trusts_extraction/run_20260218T163154Z`
- Database: `output/trusts_extraction/companies_house_extractions.db`
- Model: `google/gemini-2.5-flash-lite`
- Selection mode: random sample (`--random-sample-size 10`, no fixed seed)

## Outcome Summary

- Trusts tested: `10`
- Succeeded: `0`
- Failed: `10`

Failure categories observed:
- `OpenRouter response did not include text content`: `4`
- `OpenRouter HTTP 400 (schema nesting depth exceeded)`: `2`
- `Companies House API 429 (rate limited while fetching document metadata)`: `4`

## Trusts Checked And Outcomes

| # | Source Row | Group ID | Group Name | Company Number | Outcome | Failure Category |
|---|---:|---|---|---|---|---|
| 1 | 2166 | TR02402 | WATFORD GRAMMAR SCHOOL FOR BOYS | 07348288 | Failed | OpenRouter: no text content |
| 2 | 1031 | TR03753 | CENTRAL CO-OPERATIVE LEARNING TRUST | 10973765 | Failed | OpenRouter: no text content |
| 3 | 147 | TR00656 | RIDGEWAY EDUCATION TRUST | 08104201 | Failed | OpenRouter: HTTP 400 schema depth |
| 4 | 488 | TR02404 | THE WAVERLEY EDUCATION FOUNDATION LTD | 08331922 | Failed | OpenRouter: no text content |
| 5 | 403 | TR01916 | SIDNEY STRINGER MULTI ACADEMY TRUST | 06672920 | Failed | OpenRouter: no text content |
| 6 | 921 | TR03699 | ORWELL MULTI ACADEMY TRUST | 10650092 | Failed | OpenRouter: HTTP 400 schema depth |
| 7 | 580 | TR03241 | MAYFLOWER SPECIALIST SCHOOL ACADEMY TRUST | 09610951 | Failed | Companies House: HTTP 429 |
| 8 | 436 | TR02890 | THE STOUR FEDERATION | 09174628 | Failed | Companies House: HTTP 429 |
| 9 | 1779 | TR01227 | KING JAMES'S SCHOOL | 08164889 | Failed | Companies House: HTTP 429 |
| 10 | 2079 | TR02131 | ST. MICHAEL'S CATHOLIC COLLEGE | 08160034 | Failed | Companies House: HTTP 429 |

## Detailed Failure Information

### 07348288 - WATFORD GRAMMAR SCHOOL FOR BOYS

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter response did not include text content`
- Trace location:
  - `openrouter_document_extractor.py:528`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/07348288/documents/07348288_latest_full_accounts_dSJImN142jHB98wReCPgNz-n9B7F0NeslRabfp3RoZk.pdf`

### 10973765 - CENTRAL CO-OPERATIVE LEARNING TRUST

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter response did not include text content`
- Trace location:
  - `openrouter_document_extractor.py:528`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/10973765/documents/10973765_latest_full_accounts_-zv4bnYeGgdmFtzIVYBhVb8CvpleC7ZulbdwPzu48Sw.pdf`

### 08104201 - RIDGEWAY EDUCATION TRUST

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter request failed: OpenRouter HTTP 400`
  - Provider message: `A schema in GenerationConfig in the request exceeds the maximum allowed nesting depth.`
- Provider:
  - `Google AI Studio`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/08104201/documents/08104201_latest_full_accounts_cIYUPKWlrYlbf1_1Eae_9W1BQeZmwprB8zDsJGGuQqk.pdf`

### 08331922 - THE WAVERLEY EDUCATION FOUNDATION LTD

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter response did not include text content`
- Trace location:
  - `openrouter_document_extractor.py:528`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/08331922/documents/08331922_latest_full_accounts_AbAguQ6_8KZe-5wjH_Z67lYtnRK8oAvcUOcMG4FvC78.pdf`

### 06672920 - SIDNEY STRINGER MULTI ACADEMY TRUST

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter response did not include text content`
- Trace location:
  - `openrouter_document_extractor.py:528`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/06672920/documents/06672920_latest_full_accounts_d5uMA5pwv_Px9qmwLaFrTlrgvB89sYNygDOSYaD1Yog.pdf`

### 10650092 - ORWELL MULTI ACADEMY TRUST

- Error:
  - `openrouter_document_extractor.DocumentExtractionError: OpenRouter request failed: OpenRouter HTTP 400`
  - Provider message: `A schema in GenerationConfig in the request exceeds the maximum allowed nesting depth.`
- Provider:
  - `Google AI Studio`
- Downloaded PDF:
  - `output/trusts_extraction/run_20260218T163154Z/10650092/documents/10650092_latest_full_accounts_mUaS5biZ_WGRO3USgPDZtj51Ibf5FI6h8uLgVrCychA.pdf`

### 09610951 - MAYFLOWER SPECIALIST SCHOOL ACADEMY TRUST

- Error:
  - `companies_house_client.CompaniesHouseApiError: API request failed`
  - Status code: `429`
  - URL: `https://document-api.company-information.service.gov.uk/document/qErMyeMF0XEv09iHejlkJYiy_cDnbSOWVvWLYHGPNeo`

### 09174628 - THE STOUR FEDERATION

- Error:
  - `companies_house_client.CompaniesHouseApiError: API request failed`
  - Status code: `429`
  - URL: `https://document-api.company-information.service.gov.uk/document/C0IcJ9kaEUucadaFjfUhcNNVHTdWfoXWGRTeyoeSXdc`

### 08164889 - KING JAMES'S SCHOOL

- Error:
  - `companies_house_client.CompaniesHouseApiError: API request failed`
  - Status code: `429`
  - URL: `https://document-api.company-information.service.gov.uk/document/RLG0p2v_5h3Js0pIQ2zwSb_jUqxlPMBzWt7PBabioUQ`

### 08160034 - ST. MICHAEL'S CATHOLIC COLLEGE

- Error:
  - `companies_house_client.CompaniesHouseApiError: API request failed`
  - Status code: `429`
  - URL: `https://document-api.company-information.service.gov.uk/document/NV9Vj7RsV48S8mBnBEIpUbiqBjVsAlkg3ynEOBig0_Q`

