
Model
ThinkingThoughts
Expand to view model thoughts

chevron_right
Model
ThinkingThoughts
Expand to view model thoughts

chevron_right
Because the UK government (ESFA) requires this data to be submitted digitally as well as in PDF format, they actually use a standard called **SBR (Standard Business Reporting)** and **XBRL (eXtensible Business Reporting Language)**.

Below is a representation of that mandated structure in a **JSON Schema** format. This schema covers the core financial and governance data sets you see in the Acorn Multi Academy Trust report.

### JSON Schema: Academy Trust Annual Report

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "AcademyTrustAnnualReport",
  "description": "Standardized data structure for UK Academy Trust financial filings based on the Academies Accounts Direction.",
  "type": "object",
  "properties": {
    "metadata": {
      "type": "object",
      "properties": {
        "trust_name": { "type": "string" },
        "company_registration_number": { "type": "string", "pattern": "^[0-9]{8}$" },
        "financial_year_ending": { "type": "string", "format": "date" },
        "accounting_officer": { "type": "string" }
      },
      "required": ["trust_name", "company_registration_number", "financial_year_ending"]
    },
    "governance": {
      "type": "object",
      "properties": {
        "trustees": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "name": { "type": "string" },
              "meetings_attended": { "type": "integer" },
              "meetings_possible": { "type": "integer" }
            }
          }
        }
      }
    },
    "statement_of_financial_activities": {
      "type": "object",
      "description": "The SOFA table (Page 30)",
      "properties": {
        "income": {
          "type": "object",
          "properties": {
            "donations_and_capital_grants": { "$ref": "#/definitions/fund_breakdown" },
            "charitable_activities_education": { "$ref": "#/definitions/fund_breakdown" },
            "other_trading_activities": { "$ref": "#/definitions/fund_breakdown" },
            "investments": { "$ref": "#/definitions/fund_breakdown" }
          }
        },
        "expenditure": {
          "type": "object",
          "properties": {
            "charitable_activities_education": { "$ref": "#/definitions/fund_breakdown" }
          }
        }
      }
    },
    "balance_sheet": {
      "type": "object",
      "description": "The Balance Sheet (Page 31)",
      "properties": {
        "fixed_assets": { "type": "number" },
        "current_assets": {
          "type": "object",
          "properties": {
            "debtors": { "type": "number" },
            "cash_at_bank": { "type": "number" }
          }
        },
        "liabilities": {
          "type": "object",
          "properties": {
            "creditors_within_one_year": { "type": "number" },
            "pension_scheme_liability": { "type": "number" }
          }
        },
        "net_assets": { "type": "number" }
      }
    },
    "staffing_data": {
      "type": "object",
      "description": "Mandated staffing disclosures (Pages 46-47)",
      "properties": {
        "average_headcount_fte": { "type": "number" },
        "total_staff_costs": { "type": "number" },
        "high_pay_bands": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "band_range": { "type": "string", "example": "£70,001 - £80,000" },
              "count": { "type": "integer" }
            }
          }
        }
      }
    }
  },
  "definitions": {
    "fund_breakdown": {
      "type": "object",
      "properties": {
        "unrestricted_funds": { "type": "number" },
        "restricted_general_funds": { "type": "number" },
        "restricted_fixed_asset_funds": { "type": "number" },
        "total": { "type": "number" }
      }
    }
  }
}
```

### How this maps to the Acorn Report:

If you were to populate the above schema using the data from the screenshots provided, it would look like this (abbreviated):

```json
{
  "metadata": {
    "trust_name": "ACORN MULTI ACADEMY TRUST",
    "company_registration_number": "09253218",
    "financial_year_ending": "2024-08-31"
  },
  "statement_of_financial_activities": {
    "income": {
      "donations_and_capital_grants": {
        "unrestricted_funds": 23673,
        "restricted_general_funds": 214000,
        "restricted_fixed_asset_funds": 3101812,
        "total": 3339485
      }
    }
  },
  "balance_sheet": {
    "fixed_assets": 7530887,
    "current_assets": {
      "debtors": 300689,
      "cash_at_bank": 983289
    },
    "liabilities": {
      "pension_scheme_liability": 132000
    }
  },
  "staffing_data": {
    "average_headcount_fte": 101,
    "high_pay_bands": [
      { "band_range": "£70,001 - £80,000", "count": 1 },
      { "band_range": "£230,001 - £240,000", "count": 1 }
    ]
  }
}
```

### Why this structure is useful:
1.  **Note 18 (Fund Accounting):** In the JSON, the `fund_breakdown` definition is critical. Academy Trusts are legally required to show that they didn't use "Restricted" money (meant for books and teachers) to pay for "Fixed Assets" (like a new roof) without a formal transfer.
2.  **Audit Trail:** By having a mandated schema, auditors can verify that the "Total" on the SOFA matches the "Net Assets" on the Balance Sheet.
3.  **Benchmarking:** The ESFA uses this structured data to create the "Schools Financial Benchmarking" website, where you can compare Acorn's spending per pupil against any other trust in the country.