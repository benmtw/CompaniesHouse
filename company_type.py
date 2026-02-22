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
