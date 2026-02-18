# Live Extraction Result

- Company number requested: `5670663`
- Company number used: `05670663`
- Company name: `ALDRIDGE EDUCATION`
- Latest accounts PDF: `SourceData\_tmp_live_tests\05670663_latest_full_accounts.pdf`

## Full Extracted Payload

```json
{
  "company_number_requested": "5670663",
  "company_number_used": "05670663",
  "company_name": "ALDRIDGE EDUCATION",
  "latest_accounts_pdf_path": "SourceData\\_tmp_live_tests\\05670663_latest_full_accounts.pdf",
  "extraction_result": {
    "source_path": "SourceData\\_tmp_live_tests\\05670663_latest_full_accounts.pdf",
    "model": "google/gemini-2.5-flash-lite",
    "requested_types": [
      "personnel_details",
      "balance_sheet",
      "metadata",
      "governance",
      "statement_of_financial_activities",
      "detailed_balance_sheet",
      "staffing_data",
      "academy_trust_annual_report"
    ],
    "personnel_details": [
      {
        "first_name": "Jane",
        "last_name": "Fletcher",
        "job_title": "Chief executive officer"
      },
      {
        "first_name": "Richard",
        "last_name": "Basset",
        "job_title": "Interim CEO"
      },
      {
        "first_name": "Jane",
        "last_name": "Fletcher",
        "job_title": "CEO"
      },
      {
        "first_name": "Kit",
        "last_name": "Lam",
        "job_title": "COO"
      },
      {
        "first_name": "Kelly",
        "last_name": "Lincoln",
        "job_title": "Commercial Director"
      },
      {
        "first_name": "Kerry",
        "last_name": "Birch",
        "job_title": "Director of Human Resources"
      },
      {
        "first_name": "Theresa",
        "last_name": "Palmer",
        "job_title": "Director of Governance and Compliance"
      },
      {
        "first_name": "Alan",
        "last_name": "Brooks",
        "job_title": "Director of Marketing, Communications and External Affairs"
      },
      {
        "first_name": "Brent",
        "last_name": "Thomas",
        "job_title": "Chair of Trustees"
      },
      {
        "first_name": "Jane",
        "last_name": "Fletcher",
        "job_title": "Accounting Officer"
      }
    ],
    "balance_sheet": [
      {
        "line_item": "Tangible assets",
        "value": "122,468",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Stocks",
        "value": "1,965",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Debtors",
        "value": "2,793",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Cash at bank and in hand",
        "value": "1,950",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Creditors: amounts falling due within one year",
        "value": "(2,808)",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Creditors: amounts falling due after more than one year",
        "value": "(1,193)",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Net assets excluding pension liability",
        "value": "123,225",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Defined benefit pension scheme liability",
        "value": "(14,971)",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Total net assets",
        "value": "108,254",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Restricted funds: Fixed asset funds",
        "value": "123,057",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Restricted funds: Restricted funds excluding pension liability",
        "value": "123,057",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Restricted funds: Pension reserve",
        "value": "(14,971)",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Total restricted funds",
        "value": "108,086",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Unrestricted income funds",
        "value": "168",
        "period": "31 August 2019",
        "currency": "£000"
      },
      {
        "line_item": "Total funds",
        "value": "108,254",
        "period": "31 August 2019",
        "currency": "£000"
      }
    ],
    "metadata": {
      "trust_name": "ALDRIDGE EDUCATION",
      "company_registration_number": "05670663",
      "financial_year_ending": "2019-08-31",
      "accounting_officer": "Jane Fletcher"
    },
    "governance": {
      "trustees": [
        {
          "name": "Brent Thomas",
          "meetings_attended": 5,
          "meetings_possible": 5
        },
        {
          "name": "Anand Aithal",
          "meetings_attended": 4,
          "meetings_possible": 5
        },
        {
          "name": "Sir Rod Aldridge OBE",
          "meetings_attended": 3,
          "meetings_possible": 5
        },
        {
          "name": "Tunde Banjoko OBE",
          "meetings_attended": 4,
          "meetings_possible": 5
        },
        {
          "name": "Richard Benton",
          "meetings_attended": 4,
          "meetings_possible": 5
        },
        {
          "name": "Janie Chesterton",
          "meetings_attended": 4,
          "meetings_possible": 5
        },
        {
          "name": "Caroline Sheridan",
          "meetings_attended": 5,
          "meetings_possible": 5
        },
        {
          "name": "Rob Wye",
          "meetings_attended": 2,
          "meetings_possible": 5
        },
        {
          "name": "Jane Waters",
          "meetings_attended": 5,
          "meetings_possible": 5
        }
      ]
    },
    "statement_of_financial_activities": {
      "income": {
        "donations_and_capital_grants": {
          "unrestricted_funds": 147.00000000000003,
          "restricted_general_funds": null,
          "restricted_fixed_asset_funds": null,
          "total": null
        },
        "charitable_activities_education": {
          "unrestricted_funds": 609.0000000000001,
          "restricted_general_funds": null,
          "restricted_fixed_asset_funds": null,
          "total": null
        },
        "other_trading_activities": {
          "unrestricted_funds": 1717.0000000000002,
          "restricted_general_funds": null,
          "restricted_fixed_asset_funds": null,
          "total": null
        },
        "investments": {
          "unrestricted_funds": 2.0000000000000004,
          "restricted_general_funds": null,
          "restricted_fixed_asset_funds": null,
          "total": null
        }
      },
      "expenditure": {
        "charitable_activities_education": {
          "unrestricted_funds": 913.0000000000001,
          "restricted_general_funds": null,
          "restricted_fixed_asset_funds": null,
          "total": null
        }
      }
    },
    "detailed_balance_sheet": {
      "fixed_assets": 122468.0,
      "current_assets": {
        "debtors": 2793.0,
        "cash_at_bank": 1950.0
      },
      "liabilities": {
        "creditors_within_one_year": -2808.0,
        "pension_scheme_liability": -14971.0
      },
      "net_assets": 108254.0
    },
    "staffing_data": {
      "average_headcount_fte": 789.0,
      "total_staff_costs": 32083.0,
      "high_pay_bands": [
        {
          "band_range": "£60,001 - £70,000",
          "count": 18
        },
        {
          "band_range": "£70,001 - £80,000",
          "count": 4
        },
        {
          "band_range": "£80,001 - £90,000",
          "count": 3
        },
        {
          "band_range": "£90,001 - £100,000",
          "count": 4
        },
        {
          "band_range": "£110,001 - £120,000",
          "count": 1
        },
        {
          "band_range": "£120,001 - £130,000",
          "count": 1
        },
        {
          "band_range": "£130,001 - £140,000",
          "count": 1
        },
        {
          "band_range": "£170,001 - £180,000",
          "count": 1
        }
      ]
    },
    "academy_trust_annual_report": {
      "metadata": {
        "trust_name": "ALDRIDGE EDUCATION",
        "company_registration_number": "05670663",
        "financial_year_ending": "2019-08-31",
        "accounting_officer": "Jane Fletcher"
      },
      "governance": {
        "trustees": [
          {
            "name": "Brent Thomas",
            "meetings_attended": 5,
            "meetings_possible": 5
          },
          {
            "name": "Anand Aithal",
            "meetings_attended": 4,
            "meetings_possible": 5
          },
          {
            "name": "Sir Rod Aldridge OBE",
            "meetings_attended": 3,
            "meetings_possible": 5
          },
          {
            "name": "Tunde Banjoko OBE",
            "meetings_attended": 4,
            "meetings_possible": 5
          },
          {
            "name": "Richard Benton",
            "meetings_attended": 4,
            "meetings_possible": 5
          },
          {
            "name": "Janie Chesterton",
            "meetings_attended": 4,
            "meetings_possible": 5
          },
          {
            "name": "Caroline Sheridan",
            "meetings_attended": 5,
            "meetings_possible": 5
          },
          {
            "name": "Rob Wye",
            "meetings_attended": 2,
            "meetings_possible": 5
          },
          {
            "name": "Jane Waters",
            "meetings_attended": 5,
            "meetings_possible": 5
          }
        ]
      },
      "statement_of_financial_activities": {
        "income": {
          "donations_and_capital_grants": {
            "unrestricted_funds": 147.00000000000003,
            "restricted_general_funds": null,
            "restricted_fixed_asset_funds": null,
            "total": null
          },
          "charitable_activities_education": {
            "unrestricted_funds": 609.0000000000001,
            "restricted_general_funds": null,
            "restricted_fixed_asset_funds": null,
            "total": null
          },
          "other_trading_activities": {
            "unrestricted_funds": 1717.0000000000002,
            "restricted_general_funds": null,
            "restricted_fixed_asset_funds": null,
            "total": null
          },
          "investments": {
            "unrestricted_funds": 2.0000000000000004,
            "restricted_general_funds": null,
            "restricted_fixed_asset_funds": null,
            "total": null
          }
        },
        "expenditure": {
          "charitable_activities_education": {
            "unrestricted_funds": 913.0000000000001,
            "restricted_general_funds": null,
            "restricted_fixed_asset_funds": null,
            "total": null
          }
        }
      },
      "balance_sheet": {
        "fixed_assets": 122468.0,
        "current_assets": {
          "debtors": 2793.0,
          "cash_at_bank": 1950.0
        },
        "liabilities": {
          "creditors_within_one_year": -2808.0,
          "pension_scheme_liability": -14971.0
        },
        "net_assets": 108254.0
      },
      "staffing_data": {
        "average_headcount_fte": 789.0,
        "total_staff_costs": 32083.0,
        "high_pay_bands": [
          {
            "band_range": "£60,001 - £70,000",
            "count": 18
          },
          {
            "band_range": "£70,001 - £80,000",
            "count": 4
          },
          {
            "band_range": "£80,001 - £90,000",
            "count": 3
          },
          {
            "band_range": "£90,001 - £100,000",
            "count": 4
          },
          {
            "band_range": "£110,001 - £120,000",
            "count": 1
          },
          {
            "band_range": "£120,001 - £130,000",
            "count": 1
          },
          {
            "band_range": "£130,001 - £140,000",
            "count": 1
          },
          {
            "band_range": "£170,001 - £180,000",
            "count": 1
          }
        ]
      }
    },
    "validation_warnings": [
      "detailed_balance_sheet net_assets mismatch: computed 144990.0 differs from reported 108254.0.",
      "academy_trust_annual_report.balance_sheet net_assets mismatch: computed 144990.0 differs from reported 108254.0."
    ]
  }
}
```
