from enum import Enum

from pydantic import BaseModel, ConfigDict, field_validator


class ExtractionType(Enum):
    """Extraction intents that can be requested independently."""

    PersonnelDetails = "personnel_details"
    BalanceSheet = "balance_sheet"


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


class ExtractionResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_path: str
    model: str
    requested_types: list[ExtractionType]
    personnel_details: list[PersonnelDetail] | None = None
    balance_sheet: list[BalanceSheetEntry] | None = None

    @field_validator("source_path", "model")
    @classmethod
    def _required_non_empty(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("must not be empty")
        return cleaned


__all__ = [
    "BalanceSheetEntry",
    "ExtractionResult",
    "ExtractionType",
    "PersonnelDetail",
]
