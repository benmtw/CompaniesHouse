from enum import Enum


class CompanyType(str, Enum):
    """Identifies the type of company being processed.

    Controls which LLM prompt text, JSON schema field names, and
    Pydantic models are used during extraction.
    """

    GENERIC = "generic"
    ACADEMY_TRUST = "academy_trust"


PROMPT_PROFILES: dict[CompanyType, dict[str, str]] = {
    CompanyType.GENERIC: {
        "entity_label": "company",
        "name_field": "company_name",
        "governance_label": "directors",
        "governance_member_label": "director",
        "annual_report_key": "annual_report",
        "metadata_prompt": (
            "Extract report metadata with company_name, company_registration_number, "
            "financial_year_ending, and accounting_officer."
        ),
        "governance_prompt": (
            "Extract governance directors with name, meetings_attended, and "
            "meetings_possible."
        ),
        "annual_report_prompt": (
            "Extract annual_report with metadata, governance, "
            "statement_of_financial_activities, balance_sheet, and staffing_data."
        ),
        "optional_fields_hint": (
            "For annual report sections, use null or omit optional fields when "
            "the filing does not provide a value."
        ),
        "personnel_prompt": (
            "Extract personnel details with fields: first_name, last_name, job_title, "
            "standardised_job_title.\n"
            "last_name MUST be the person's actual surname (family name), never an initial. "
            "If the document shows initials before a surname (e.g. 'S J Bates'), "
            "first_name should contain all initials/given names ('S J') and last_name "
            "must be the surname ('Bates').\n"
            "Exclude anyone whose only role is 'Member' or 'Trustee'.\n"
            "If a person explicitly resigned from their role during the period, exclude them.\n"
            "For standardised_job_title, use one of the following values if the person's "
            "role clearly matches, otherwise use null:\n"
            "  - Chief Executive Officer\n"
            "  - Chief Financial Officer / Director of Finance\n"
            "  - Director of Operations\n"
            "  - Director of People / HR\n"
            "  - Director of Education\n"
            "  - Director of Standards / School Improvement\n"
            "  - Director of Safeguarding\n"
            "  - Director of SEND / Inclusion\n"
            "  - Director of Governance / Company Secretary\n"
            "  - Director of IT / Digital\n"
            "  - Director of Estates / Property\n"
            "  - Director of Data & Assessment\n"
            "  - Director of Communications / Marketing\n"
            "  - Director of Procurement\n"
            "  - Director of Compliance / Risk\n"
            "It is perfectly acceptable for standardised_job_title to be null when the "
            "role does not confidently map to one of the above."
        ),
    },
    CompanyType.ACADEMY_TRUST: {
        "entity_label": "academy trust",
        "name_field": "trust_name",
        "governance_label": "trustees",
        "governance_member_label": "trustee",
        "annual_report_key": "academy_trust_annual_report",
        "metadata_prompt": (
            "Extract report metadata with trust_name, company_registration_number, "
            "financial_year_ending, and accounting_officer."
        ),
        "governance_prompt": (
            "Extract governance trustees with name, meetings_attended, and "
            "meetings_possible."
        ),
        "annual_report_prompt": (
            "Extract academy_trust_annual_report with metadata, governance, "
            "statement_of_financial_activities, balance_sheet, and staffing_data."
        ),
        "optional_fields_hint": (
            "For academy trust report sections, use null or omit optional fields when "
            "the filing does not provide a value."
        ),
        "personnel_prompt": (
            "Extract personnel details with fields: first_name, last_name, job_title, "
            "organisation_name, organisation_type, standardised_job_title.\n"
            "last_name MUST be the person's actual surname (family name), never an initial. "
            "If the document shows initials before a surname (e.g. 'S J Bates'), "
            "first_name should contain all initials/given names ('S J') and last_name "
            "must be the surname ('Bates').\n"
            "organisation_name is the name of the trust or school the person works at.\n"
            "organisation_type must be either 'trust' or 'school', or null if unclear.\n"
            "Exclude anyone whose only role is 'Member' or 'Trustee'.\n"
            "If a person explicitly resigned from their role during the period, exclude them.\n"
            "For standardised_job_title, use one of the following values if the person's "
            "role clearly matches, otherwise use null:\n"
            "  - Chief Executive Officer\n"
            "  - Chief Financial Officer / Director of Finance\n"
            "  - Director of Operations\n"
            "  - Director of People / HR\n"
            "  - Director of Education\n"
            "  - Director of Standards / School Improvement\n"
            "  - Director of Safeguarding\n"
            "  - Director of SEND / Inclusion\n"
            "  - Director of Governance / Company Secretary\n"
            "  - Director of IT / Digital\n"
            "  - Director of Estates / Property\n"
            "  - Director of Data & Assessment\n"
            "  - Director of Communications / Marketing\n"
            "  - Director of Procurement\n"
            "  - Director of Compliance / Risk\n"
            "It is perfectly acceptable for standardised_job_title to be null when the "
            "role does not confidently map to one of the above."
        ),
    },
}


def get_prompt_profile(company_type: CompanyType) -> dict[str, str]:
    """Return the prompt profile for a given company type."""
    return PROMPT_PROFILES[company_type]


__all__ = [
    "CompanyType",
    "PROMPT_PROFILES",
    "get_prompt_profile",
]
