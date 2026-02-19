from enum import Enum
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator


_NULL_NUMERIC_TOKENS = {"", "-", "—", "n/a", "na", "none", "null"}


def _coerce_accounting_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("invalid numeric value")
    if isinstance(value, (int, float)):
        return float(value)

    raw = str(value).strip()
    if raw.lower() in _NULL_NUMERIC_TOKENS:
        return None

    is_negative = raw.startswith("(") and raw.endswith(")")
    if is_negative:
        raw = raw[1:-1].strip()

    cleaned = raw.replace(",", "").replace(" ", "")
    cleaned = re.sub(r"[£$€]", "", cleaned)
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if not cleaned:
        return None

    try:
        parsed = float(cleaned)
    except ValueError as exc:
        raise ValueError(f"invalid numeric value: {value}") from exc
    return -parsed if is_negative else parsed


def _coerce_accounting_int(value: object) -> int | None:
    parsed = _coerce_accounting_float(value)
    if parsed is None:
        return None
    if parsed.is_integer():
        return int(parsed)
    raise ValueError("expected whole number")


class ExtractionType(Enum):
    """Extraction intents that can be requested independently."""

    PersonnelDetails = "personnel_details"
    BalanceSheet = "balance_sheet"
    Metadata = "metadata"
    Governance = "governance"
    StatementOfFinancialActivities = "statement_of_financial_activities"
    DetailedBalanceSheet = "detailed_balance_sheet"
    StaffingData = "staffing_data"
    AcademyTrustAnnualReport = "academy_trust_annual_report"


class PersonnelDetail(BaseModel):
    model_config = ConfigDict(extra="ignore")

    first_name: str
    last_name: str
    job_title: str

    @field_validator("first_name", "last_name", "job_title")
    @classmethod
    def _required_non_empty(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned


class BalanceSheetEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    line_item: str
    value: str
    period: str | None = None
    currency: str | None = None

    @field_validator("line_item", "value")
    @classmethod
    def _required_non_empty(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("period", "currency")
    @classmethod
    def _optional_trimmed(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None


class Metadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trust_name: str
    company_registration_number: str
    financial_year_ending: str
    accounting_officer: str | None = None

    @field_validator(
        "trust_name",
        "company_registration_number",
        "financial_year_ending",
        "accounting_officer",
    )
    @classmethod
    def _optional_trimmed(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("trust_name", "company_registration_number", "financial_year_ending")
    @classmethod
    def _required_non_empty(cls, value: str | None) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned

    @field_validator("company_registration_number")
    @classmethod
    def _company_number_must_be_8_digits(cls, value: str) -> str:
        normalized = value.strip()
        if normalized.isdigit() and len(normalized) == 7:
            normalized = normalized.zfill(8)
        if len(normalized) != 8 or not normalized.isdigit():
            raise ValueError("must be an 8-digit company number")
        return normalized


class TrusteeAttendance(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str | None = None
    meetings_attended: int | None = None
    meetings_possible: int | None = None

    @field_validator("name")
    @classmethod
    def _optional_name_trimmed(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("meetings_attended", "meetings_possible", mode="before")
    @classmethod
    def _optional_meeting_counts(cls, value: object) -> int | None:
        return _coerce_accounting_int(value)


class Governance(BaseModel):
    model_config = ConfigDict(extra="ignore")

    trustees: list[TrusteeAttendance] = Field(default_factory=list)


class FundBreakdown(BaseModel):
    model_config = ConfigDict(extra="ignore")

    unrestricted_funds: float | None = None
    restricted_general_funds: float | None = None
    restricted_fixed_asset_funds: float | None = None
    total: float | None = None

    @field_validator(
        "unrestricted_funds",
        "restricted_general_funds",
        "restricted_fixed_asset_funds",
        "total",
        mode="before",
    )
    @classmethod
    def _optional_amounts(cls, value: object) -> float | None:
        return _coerce_accounting_float(value)


class StatementOfFinancialActivitiesIncome(BaseModel):
    model_config = ConfigDict(extra="ignore")

    donations_and_capital_grants: FundBreakdown | None = None
    charitable_activities_education: FundBreakdown | None = None
    other_trading_activities: FundBreakdown | None = None
    investments: FundBreakdown | None = None


class StatementOfFinancialActivitiesExpenditure(BaseModel):
    model_config = ConfigDict(extra="ignore")

    charitable_activities_education: FundBreakdown | None = None


class StatementOfFinancialActivities(BaseModel):
    model_config = ConfigDict(extra="ignore")

    income: StatementOfFinancialActivitiesIncome | None = None
    expenditure: StatementOfFinancialActivitiesExpenditure | None = None


class DetailedBalanceSheetCurrentAssets(BaseModel):
    model_config = ConfigDict(extra="ignore")

    debtors: float | None = None
    cash_at_bank: float | None = None

    @field_validator("debtors", "cash_at_bank", mode="before")
    @classmethod
    def _optional_amounts(cls, value: object) -> float | None:
        return _coerce_accounting_float(value)


class DetailedBalanceSheetLiabilities(BaseModel):
    model_config = ConfigDict(extra="ignore")

    creditors_within_one_year: float | None = None
    pension_scheme_liability: float | None = None

    @field_validator("creditors_within_one_year", "pension_scheme_liability", mode="before")
    @classmethod
    def _optional_amounts(cls, value: object) -> float | None:
        return _coerce_accounting_float(value)


class DetailedBalanceSheet(BaseModel):
    model_config = ConfigDict(extra="ignore")

    fixed_assets: float | None = None
    current_assets: DetailedBalanceSheetCurrentAssets | None = None
    liabilities: DetailedBalanceSheetLiabilities | None = None
    net_assets: float | None = None

    @field_validator("fixed_assets", "net_assets", mode="before")
    @classmethod
    def _optional_amounts(cls, value: object) -> float | None:
        return _coerce_accounting_float(value)


class HighPayBand(BaseModel):
    model_config = ConfigDict(extra="ignore")

    band_range: str | None = None
    count: int | None = None

    @field_validator("band_range")
    @classmethod
    def _optional_band_trimmed(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("count", mode="before")
    @classmethod
    def _optional_count(cls, value: object) -> int | None:
        return _coerce_accounting_int(value)


class StaffingData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    average_headcount_fte: float | None = None
    total_staff_costs: float | None = None
    high_pay_bands: list[HighPayBand] = Field(default_factory=list)

    @field_validator("average_headcount_fte", "total_staff_costs", mode="before")
    @classmethod
    def _optional_amounts(cls, value: object) -> float | None:
        return _coerce_accounting_float(value)


class AcademyTrustAnnualReport(BaseModel):
    model_config = ConfigDict(extra="ignore")

    metadata: Metadata | None = None
    governance: Governance | None = None
    statement_of_financial_activities: StatementOfFinancialActivities | None = None
    balance_sheet: DetailedBalanceSheet | None = None
    staffing_data: StaffingData | None = None


class ExtractionResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_path: str
    model: str
    requested_types: list[ExtractionType]
    personnel_details: list[PersonnelDetail] | None = None
    balance_sheet: list[BalanceSheetEntry] | None = None
    metadata: Metadata | None = None
    governance: Governance | None = None
    statement_of_financial_activities: StatementOfFinancialActivities | None = None
    detailed_balance_sheet: DetailedBalanceSheet | None = None
    staffing_data: StaffingData | None = None
    academy_trust_annual_report: AcademyTrustAnnualReport | None = None
    validation_warnings: list[str] = Field(default_factory=list)

    @field_validator("source_path", "model")
    @classmethod
    def _required_non_empty(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned


__all__ = [
    "AcademyTrustAnnualReport",
    "BalanceSheetEntry",
    "DetailedBalanceSheet",
    "DetailedBalanceSheetCurrentAssets",
    "DetailedBalanceSheetLiabilities",
    "ExtractionResult",
    "ExtractionType",
    "FundBreakdown",
    "Governance",
    "HighPayBand",
    "Metadata",
    "PersonnelDetail",
    "StaffingData",
    "StatementOfFinancialActivities",
    "StatementOfFinancialActivitiesExpenditure",
    "StatementOfFinancialActivitiesIncome",
    "TrusteeAttendance",
]
