import base64
import json
import os
import re
from pathlib import Path
from typing import Any, Callable

import requests
from pydantic import ValidationError
try:
    from json_repair import loads as json_repair_loads
except ImportError:
    json_repair_loads = None

from document_extraction_models import (
    AcademyTrustAnnualReport,
    BalanceSheetEntry,
    DetailedBalanceSheet,
    ExtractionResult,
    ExtractionType,
    Governance,
    Metadata,
    PersonnelDetail,
    StaffingData,
    StatementOfFinancialActivities,
)


class DocumentExtractionError(Exception):
    """Raised when a document cannot be parsed or validated for extraction."""

    def __init__(
        self,
        message: str,
        *,
        raw_response_text: str | None = None,
        raw_response_payload: Any = None,
    ) -> None:
        super().__init__(message)
        self.raw_response_text = raw_response_text
        self.raw_response_payload = raw_response_payload


class OpenRouterDocumentExtractor:
    """
    Extract structured information from downloaded Companies House documents.

    Supports independent extraction requests such as personnel details and
    balance sheet line items.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "openrouter/auto",
        max_document_chars: int = 35000,
        request_timeout_seconds: float = 180.0,
    ) -> None:
        resolved_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Missing OpenRouter API key. Provide api_key or set OPENROUTER_API_KEY."
            )
        if not model or not model.strip():
            raise ValueError("model must not be empty")
        if max_document_chars <= 0:
            raise ValueError("max_document_chars must be > 0")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be > 0")

        self.api_key = resolved_key
        self.model = model.strip()
        self.max_document_chars = max_document_chars
        self.request_timeout_seconds = float(request_timeout_seconds)

    def extract(
        self, document_path: str, extraction_types: list[ExtractionType]
    ) -> ExtractionResult:
        """Extract requested data types from a single document."""
        self._require_non_empty(document_path, "document_path")
        requested_types = self._normalize_extraction_types(extraction_types)
        source = Path(document_path)
        if not source.exists() or not source.is_file():
            raise DocumentExtractionError(f"Document path does not exist: {document_path}")

        payload = self._request_extraction_json(source, requested_types)
        try:
            return self._build_result(
                payload=payload,
                document_path=document_path,
                requested_types=requested_types,
            )
        except DocumentExtractionError as exc:
            if exc.raw_response_payload is not None or exc.raw_response_text is not None:
                raise
            try:
                payload_text = json.dumps(payload, ensure_ascii=True)
            except Exception:
                payload_text = None
            raise DocumentExtractionError(
                str(exc),
                raw_response_text=payload_text,
                raw_response_payload=payload,
            ) from exc

    def extract_full_accounts(
        self, document_path: str, extraction_types: list[ExtractionType] | None = None
    ) -> ExtractionResult:
        """
        Backward-compatible helper for full-accounts flows.
        """
        requested = extraction_types or [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
        ]
        return self.extract(document_path=document_path, extraction_types=requested)

    def _request_extraction_json(
        self, document_path: Path, requested_types: list[ExtractionType]
    ) -> dict[str, Any]:
        response_format = self._build_response_format(requested_types)
        system_prompt, user_prompt = self._build_prompts(requested_types)
        file_data = self._build_file_data_url(document_path)
        input_payload = self._build_input_payload(
            user_prompt=user_prompt,
            filename=document_path.name,
            file_data=file_data,
        )

        try:
            response = self._post_openrouter_chat_completion(
                payload={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        *input_payload,
                    ],
                    "provider": {"require_parameters": False},
                    "response_format": response_format,
                    "temperature": 0,
                }
            )
        except Exception as exc:
            raise DocumentExtractionError(f"OpenRouter request failed: {exc}") from exc

        try:
            response_text = self._response_text_from_completion(response)
        except DocumentExtractionError as exc:
            raise DocumentExtractionError(
                str(exc),
                raw_response_payload=response,
            ) from exc
        try:
            return self._parse_json_response(response_text)
        except DocumentExtractionError as exc:
            compact = re.sub(r"\s+", " ", response_text).strip()
            snippet = compact[:400]
            raise DocumentExtractionError(
                "OpenRouter response was not valid JSON "
                f"(chars={len(response_text)} snippet={snippet!r})",
                raw_response_text=response_text,
                raw_response_payload=response,
            ) from exc

    @staticmethod
    def _build_prompts(requested_types: list[ExtractionType]) -> tuple[str, str]:
        task_lines: list[str] = []
        if ExtractionType.PersonnelDetails in requested_types:
            task_lines.append(
                "- Extract personnel details with fields: first_name, last_name, job_title."
            )
        if ExtractionType.BalanceSheet in requested_types:
            task_lines.append("- Extract balance sheet values as line items.")
        if ExtractionType.Metadata in requested_types:
            task_lines.append(
                "- Extract report metadata with trust_name, company_registration_number, "
                "financial_year_ending, and accounting_officer."
            )
        if ExtractionType.Governance in requested_types:
            task_lines.append(
                "- Extract governance trustees with name, meetings_attended, and "
                "meetings_possible."
            )
        if ExtractionType.StatementOfFinancialActivities in requested_types:
            task_lines.append(
                "- Extract statement_of_financial_activities with income and expenditure "
                "fund breakdown rows."
            )
        if ExtractionType.DetailedBalanceSheet in requested_types:
            task_lines.append(
                "- Extract detailed_balance_sheet with fixed_assets, current_assets, "
                "liabilities, and net_assets."
            )
        if ExtractionType.StaffingData in requested_types:
            task_lines.append(
                "- Extract staffing_data with average_headcount_fte, total_staff_costs, "
                "and high_pay_bands."
            )
        if ExtractionType.AcademyTrustAnnualReport in requested_types:
            task_lines.append(
                "- Extract academy_trust_annual_report with metadata, governance, "
                "statement_of_financial_activities, balance_sheet, and staffing_data."
            )

        system_prompt = (
            "You extract structured data from UK company filing documents. "
            "The file will be provided as input. "
            "Return only JSON that matches the requested schema."
        )
        user_prompt = (
            "Extract the requested data from this Companies House filing document.\n"
            f"Requested extraction types: {[t.name for t in requested_types]}.\n"
            + "\n".join(task_lines)
            + "\n"
            "Do not emit a personnel row unless first_name, last_name, and job_title "
            "are all present and non-empty.\n"
            "Do not emit a balance_sheet row unless line_item and value are both "
            "present and non-empty.\n"
            "For optional fields like period and currency, use null when the value "
            "is missing.\n"
            "For academy trust report sections, use null or omit optional fields when "
            "the filing does not provide a value.\n"
            "Never use empty-string placeholders for missing values."
        )
        return system_prompt, user_prompt

    @staticmethod
    def _build_file_data_url(document_path: Path) -> str:
        try:
            encoded_file = base64.b64encode(document_path.read_bytes()).decode("ascii")
        except Exception as exc:
            raise DocumentExtractionError(
                f"Failed to read document file `{document_path}`: {exc}"
            ) from exc

        if not encoded_file:
            raise DocumentExtractionError(f"Document file was empty: {document_path}")

        suffix = document_path.suffix.lower()
        if suffix == ".pdf":
            mime_type = "application/pdf"
        elif suffix in {".xhtml", ".html", ".htm"}:
            mime_type = "text/html"
        else:
            mime_type = "application/octet-stream"
        return f"data:{mime_type};base64,{encoded_file}"

    @staticmethod
    def _build_input_payload(
        user_prompt: str, filename: str, file_data: str
    ) -> list[dict[str, Any]]:
        return [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": user_prompt,
                    },
                    {
                        "type": "file",
                        "file": {
                            "filename": filename,
                            "file_data": file_data,
                        },
                    },
                ],
            }
        ]

    @staticmethod
    def _build_response_format(
        requested_types: list[ExtractionType],
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        required: list[str] = []

        if ExtractionType.PersonnelDetails in requested_types:
            properties["personnel_details"] = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "first_name": {
                            "type": "string",
                            "description": "Person's first name.",
                        },
                        "last_name": {
                            "type": "string",
                            "description": "Person's last name.",
                        },
                        "job_title": {
                            "type": "string",
                            "description": "Role or title at the company.",
                        },
                    },
                    "required": ["first_name", "last_name", "job_title"],
                    "additionalProperties": False,
                },
            }
            required.append("personnel_details")

        if ExtractionType.BalanceSheet in requested_types:
            properties["balance_sheet"] = {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "line_item": {
                            "type": "string",
                            "description": "Balance sheet line item name.",
                        },
                        "value": {
                            "type": "string",
                            "description": "Extracted value for the line item.",
                        },
                        "period": {
                            "type": ["string", "null"],
                            "description": "Period/date context for the value.",
                        },
                        "currency": {
                            "type": ["string", "null"],
                            "description": "Currency code/symbol if present.",
                        },
                    },
                    "required": ["line_item", "value", "period", "currency"],
                    "additionalProperties": False,
                },
            }
            required.append("balance_sheet")

        if ExtractionType.Metadata in requested_types:
            properties["metadata"] = OpenRouterDocumentExtractor._schema_for_metadata()
            required.append("metadata")

        if ExtractionType.Governance in requested_types:
            properties["governance"] = OpenRouterDocumentExtractor._schema_for_governance()
            required.append("governance")

        if ExtractionType.StatementOfFinancialActivities in requested_types:
            properties[
                "statement_of_financial_activities"
            ] = OpenRouterDocumentExtractor._schema_for_statement_of_financial_activities()
            required.append("statement_of_financial_activities")

        if ExtractionType.DetailedBalanceSheet in requested_types:
            properties[
                "detailed_balance_sheet"
            ] = OpenRouterDocumentExtractor._schema_for_detailed_balance_sheet()
            required.append("detailed_balance_sheet")

        if ExtractionType.StaffingData in requested_types:
            properties["staffing_data"] = OpenRouterDocumentExtractor._schema_for_staffing_data()
            required.append("staffing_data")

        if ExtractionType.AcademyTrustAnnualReport in requested_types:
            properties[
                "academy_trust_annual_report"
            ] = OpenRouterDocumentExtractor._schema_for_academy_trust_annual_report()
            required.append("academy_trust_annual_report")

        return {
            "type": "json_schema",
            "json_schema": {
                "name": "companies_house_extraction",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": False,
                },
            },
        }

    @staticmethod
    def _schema_for_fund_breakdown() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "unrestricted_funds": {"type": ["number", "null"]},
                "restricted_general_funds": {"type": ["number", "null"]},
                "restricted_fixed_asset_funds": {"type": ["number", "null"]},
                "total": {"type": ["number", "null"]},
            },
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_metadata() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "trust_name": {"type": "string"},
                "company_registration_number": {
                    "type": "string",
                    "pattern": "^[0-9]{8}$",
                },
                "financial_year_ending": {"type": "string", "format": "date"},
                "accounting_officer": {"type": ["string", "null"]},
            },
            "required": [
                "trust_name",
                "company_registration_number",
                "financial_year_ending",
                "accounting_officer",
            ],
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_governance() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "trustees": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": ["string", "null"]},
                            "meetings_attended": {"type": ["integer", "null"]},
                            "meetings_possible": {"type": ["integer", "null"]},
                        },
                        "required": ["name", "meetings_attended", "meetings_possible"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["trustees"],
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_statement_of_financial_activities() -> dict[str, Any]:
        fund_breakdown = OpenRouterDocumentExtractor._schema_for_fund_breakdown()
        return {
            "type": "object",
            "properties": {
                "income": {
                    "type": "object",
                    "properties": {
                        "donations_and_capital_grants": {
                            "anyOf": [fund_breakdown, {"type": "null"}]
                        },
                        "charitable_activities_education": {
                            "anyOf": [fund_breakdown, {"type": "null"}]
                        },
                        "other_trading_activities": {
                            "anyOf": [fund_breakdown, {"type": "null"}]
                        },
                        "investments": {"anyOf": [fund_breakdown, {"type": "null"}]},
                    },
                    "required": [
                        "donations_and_capital_grants",
                        "charitable_activities_education",
                        "other_trading_activities",
                        "investments",
                    ],
                    "additionalProperties": False,
                },
                "expenditure": {
                    "type": "object",
                    "properties": {
                        "charitable_activities_education": {
                            "anyOf": [fund_breakdown, {"type": "null"}]
                        }
                    },
                    "required": ["charitable_activities_education"],
                    "additionalProperties": False,
                },
            },
            "required": ["income", "expenditure"],
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_detailed_balance_sheet() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "fixed_assets": {"type": ["number", "null"]},
                "current_assets": {
                    "type": "object",
                    "properties": {
                        "debtors": {"type": ["number", "null"]},
                        "cash_at_bank": {"type": ["number", "null"]},
                    },
                    "required": ["debtors", "cash_at_bank"],
                    "additionalProperties": False,
                },
                "liabilities": {
                    "type": "object",
                    "properties": {
                        "creditors_within_one_year": {"type": ["number", "null"]},
                        "pension_scheme_liability": {"type": ["number", "null"]},
                    },
                    "required": [
                        "creditors_within_one_year",
                        "pension_scheme_liability",
                    ],
                    "additionalProperties": False,
                },
                "net_assets": {"type": ["number", "null"]},
            },
            "required": ["fixed_assets", "current_assets", "liabilities", "net_assets"],
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_staffing_data() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "average_headcount_fte": {"type": ["number", "null"]},
                "total_staff_costs": {"type": ["number", "null"]},
                "high_pay_bands": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "band_range": {"type": ["string", "null"]},
                            "count": {"type": ["integer", "null"]},
                        },
                        "required": ["band_range", "count"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["average_headcount_fte", "total_staff_costs", "high_pay_bands"],
            "additionalProperties": False,
        }

    @staticmethod
    def _schema_for_academy_trust_annual_report() -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "metadata": OpenRouterDocumentExtractor._schema_for_metadata(),
                "governance": OpenRouterDocumentExtractor._schema_for_governance(),
                "statement_of_financial_activities": (
                    OpenRouterDocumentExtractor._schema_for_statement_of_financial_activities()
                ),
                "balance_sheet": OpenRouterDocumentExtractor._schema_for_detailed_balance_sheet(),
                "staffing_data": OpenRouterDocumentExtractor._schema_for_staffing_data(),
            },
            "required": [
                "metadata",
                "governance",
                "statement_of_financial_activities",
                "balance_sheet",
                "staffing_data",
            ],
            "additionalProperties": False,
        }

    @staticmethod
    def _response_text_from_completion(response: Any) -> str:
        content = OpenRouterDocumentExtractor._first_choice_message_content(response)
        text = OpenRouterDocumentExtractor._content_to_text(content)
        if text:
            return text

        raise DocumentExtractionError("OpenRouter response did not include text content")

    @staticmethod
    def _first_choice_message_content(response: Any) -> Any:
        choices = response.get("choices") if isinstance(response, dict) else getattr(
            response, "choices", None
        )
        if not choices:
            return None

        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                return message.get("content")
            return None

        message = getattr(first, "message", None)
        if message is None:
            return None
        if isinstance(message, dict):
            return message.get("content")
        return getattr(message, "content", None)

    @staticmethod
    def _content_to_text(content: Any) -> str | None:
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return None

        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
            else:
                text = getattr(item, "text", None)
            if isinstance(text, str):
                parts.append(text)

        if parts:
            return "\n".join(parts)
        return None

    def _post_openrouter_chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.request_timeout_seconds,
            )
        except requests.RequestException as exc:
            raise DocumentExtractionError(f"Request failed: {exc}") from exc

        if response.status_code != 200:
            body = response.text[:1200] if response.text else ""
            raise DocumentExtractionError(
                f"OpenRouter HTTP {response.status_code}: {body}"
            )

        try:
            parsed = response.json()
        except ValueError as exc:
            raise DocumentExtractionError(
                "OpenRouter response was not valid JSON payload"
            ) from exc

        if not isinstance(parsed, dict):
            raise DocumentExtractionError("OpenRouter response had unexpected shape")
        return parsed

    @staticmethod
    def _parse_json_response(text: str) -> dict[str, Any]:
        cleaned = text.strip()
        cleaned = re.sub(r"^\s*```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```\s*$", "", cleaned, flags=re.IGNORECASE)

        candidates = [cleaned]
        first_brace = cleaned.find("{")
        last_brace = cleaned.rfind("}")
        if first_brace != -1 and last_brace != -1 and first_brace < last_brace:
            candidates.append(cleaned[first_brace : last_brace + 1])

        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                continue

        if json_repair_loads is not None:
            for candidate in candidates:
                try:
                    parsed = json_repair_loads(candidate, skip_json_loads=True)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    continue

        raise DocumentExtractionError("OpenRouter response was not valid JSON")

    @staticmethod
    def _normalize_extraction_types(
        extraction_types: list[ExtractionType],
    ) -> list[ExtractionType]:
        if not extraction_types:
            raise ValueError("extraction_types must not be empty")
        seen: set[ExtractionType] = set()
        ordered: list[ExtractionType] = []
        for value in extraction_types:
            if not isinstance(value, ExtractionType):
                raise ValueError(f"Unsupported extraction type: {value}")
            if value not in seen:
                seen.add(value)
                ordered.append(value)
        return ordered

    def _build_result(
        self,
        payload: dict[str, Any],
        document_path: str,
        requested_types: list[ExtractionType],
    ) -> ExtractionResult:
        personnel_details: list[PersonnelDetail] | None = None
        balance_sheet: list[BalanceSheetEntry] | None = None
        metadata: Metadata | None = None
        governance: Governance | None = None
        statement_of_financial_activities: StatementOfFinancialActivities | None = None
        detailed_balance_sheet: DetailedBalanceSheet | None = None
        staffing_data: StaffingData | None = None
        academy_trust_annual_report: AcademyTrustAnnualReport | None = None
        validation_warnings: list[str] = []

        if ExtractionType.PersonnelDetails in requested_types:
            raw_personnel = payload.get("personnel_details", [])
            personnel_details = self._parse_personnel_details(raw_personnel)

        if ExtractionType.BalanceSheet in requested_types:
            raw_balance = payload.get("balance_sheet", [])
            balance_sheet = self._parse_balance_sheet(raw_balance)

        if ExtractionType.Metadata in requested_types:
            metadata = self._parse_optional_object_section(
                payload=payload,
                key="metadata",
                section_label="metadata",
                parser=self._parse_metadata,
                warnings=validation_warnings,
            )

        if ExtractionType.Governance in requested_types:
            governance = self._parse_optional_object_section(
                payload=payload,
                key="governance",
                section_label="governance",
                parser=self._parse_governance,
                warnings=validation_warnings,
            )

        if ExtractionType.StatementOfFinancialActivities in requested_types:
            statement_of_financial_activities = self._parse_optional_object_section(
                payload=payload,
                key="statement_of_financial_activities",
                section_label="statement_of_financial_activities",
                parser=self._parse_statement_of_financial_activities,
                warnings=validation_warnings,
            )

        if ExtractionType.DetailedBalanceSheet in requested_types:
            detailed_balance_sheet = self._parse_optional_object_section(
                payload=payload,
                key="detailed_balance_sheet",
                section_label="detailed_balance_sheet",
                parser=self._parse_detailed_balance_sheet,
                warnings=validation_warnings,
            )

        if ExtractionType.StaffingData in requested_types:
            staffing_data = self._parse_optional_object_section(
                payload=payload,
                key="staffing_data",
                section_label="staffing_data",
                parser=self._parse_staffing_data,
                warnings=validation_warnings,
            )

        if ExtractionType.AcademyTrustAnnualReport in requested_types:
            academy_trust_annual_report = self._parse_optional_object_section(
                payload=payload,
                key="academy_trust_annual_report",
                section_label="academy_trust_annual_report",
                parser=self._parse_academy_trust_annual_report,
                warnings=validation_warnings,
            )

        validation_warnings.extend(
            self._collect_validation_warnings(
                statement_of_financial_activities=statement_of_financial_activities,
                detailed_balance_sheet=detailed_balance_sheet,
                academy_trust_annual_report=academy_trust_annual_report,
            )
        )

        try:
            return ExtractionResult(
                source_path=document_path,
                model=self.model,
                requested_types=requested_types,
                personnel_details=personnel_details,
                balance_sheet=balance_sheet,
                metadata=metadata,
                governance=governance,
                statement_of_financial_activities=statement_of_financial_activities,
                detailed_balance_sheet=detailed_balance_sheet,
                staffing_data=staffing_data,
                academy_trust_annual_report=academy_trust_annual_report,
                validation_warnings=validation_warnings,
            )
        except ValidationError as exc:
            raise DocumentExtractionError(f"Invalid extraction result: {exc}") from exc

    @staticmethod
    def _parse_optional_object_section(
        payload: dict[str, Any],
        key: str,
        section_label: str,
        parser: Callable[[Any], Any],
        warnings: list[str],
    ) -> Any | None:
        raw_value = payload.get(key)
        if raw_value is None:
            warnings.append(f"Section `{section_label}` missing/null; set to null.")
            return None
        if isinstance(raw_value, str) and not raw_value.strip():
            warnings.append(f"Section `{section_label}` missing/null; set to null.")
            return None
        if not isinstance(raw_value, dict):
            warnings.append(
                f"Section `{section_label}` expected object but got "
                f"{type(raw_value).__name__}; set to null."
            )
            return None
        return parser(raw_value)

    @staticmethod
    def _collect_validation_warnings(
        statement_of_financial_activities: StatementOfFinancialActivities | None,
        detailed_balance_sheet: DetailedBalanceSheet | None,
        academy_trust_annual_report: AcademyTrustAnnualReport | None,
    ) -> list[str]:
        warnings: list[str] = []

        if statement_of_financial_activities is not None:
            warnings.extend(
                OpenRouterDocumentExtractor._reconcile_sofa(
                    statement_of_financial_activities, prefix="statement_of_financial_activities"
                )
            )
        if detailed_balance_sheet is not None:
            warnings.extend(
                OpenRouterDocumentExtractor._reconcile_balance_sheet(
                    detailed_balance_sheet, prefix="detailed_balance_sheet"
                )
            )
        if academy_trust_annual_report is not None:
            if academy_trust_annual_report.statement_of_financial_activities is not None:
                warnings.extend(
                    OpenRouterDocumentExtractor._reconcile_sofa(
                        academy_trust_annual_report.statement_of_financial_activities,
                        prefix="academy_trust_annual_report.statement_of_financial_activities",
                    )
                )
            if academy_trust_annual_report.balance_sheet is not None:
                warnings.extend(
                    OpenRouterDocumentExtractor._reconcile_balance_sheet(
                        academy_trust_annual_report.balance_sheet,
                        prefix="academy_trust_annual_report.balance_sheet",
                    )
                )
            if (
                detailed_balance_sheet is not None
                and academy_trust_annual_report.balance_sheet is not None
                and detailed_balance_sheet.model_dump() != academy_trust_annual_report.balance_sheet.model_dump()
            ):
                warnings.append(
                    "Detailed balance sheet differs between top-level `detailed_balance_sheet` "
                    "and `academy_trust_annual_report.balance_sheet`."
                )
        return warnings

    @staticmethod
    def _reconcile_sofa(
        statement_of_financial_activities: StatementOfFinancialActivities, prefix: str
    ) -> list[str]:
        warnings: list[str] = []
        income = statement_of_financial_activities.income
        expenditure = statement_of_financial_activities.expenditure
        if income is not None:
            warnings.extend(
                OpenRouterDocumentExtractor._check_fund_breakdown(
                    income.donations_and_capital_grants,
                    f"{prefix}.income.donations_and_capital_grants",
                )
            )
            warnings.extend(
                OpenRouterDocumentExtractor._check_fund_breakdown(
                    income.charitable_activities_education,
                    f"{prefix}.income.charitable_activities_education",
                )
            )
            warnings.extend(
                OpenRouterDocumentExtractor._check_fund_breakdown(
                    income.other_trading_activities,
                    f"{prefix}.income.other_trading_activities",
                )
            )
            warnings.extend(
                OpenRouterDocumentExtractor._check_fund_breakdown(
                    income.investments,
                    f"{prefix}.income.investments",
                )
            )
        if expenditure is not None:
            warnings.extend(
                OpenRouterDocumentExtractor._check_fund_breakdown(
                    expenditure.charitable_activities_education,
                    f"{prefix}.expenditure.charitable_activities_education",
                )
            )
        return warnings

    @staticmethod
    def _check_fund_breakdown(fund_breakdown: Any, label: str) -> list[str]:
        if fund_breakdown is None:
            return []
        unrestricted = fund_breakdown.unrestricted_funds
        restricted_general = fund_breakdown.restricted_general_funds
        restricted_fixed_asset = fund_breakdown.restricted_fixed_asset_funds
        total = fund_breakdown.total
        if None in (unrestricted, restricted_general, restricted_fixed_asset, total):
            return []

        components_sum = unrestricted + restricted_general + restricted_fixed_asset
        if abs(components_sum - total) > 1:
            return [
                f"{label} total mismatch: component sum {components_sum} differs from total {total}."
            ]
        return []

    @staticmethod
    def _reconcile_balance_sheet(balance_sheet: DetailedBalanceSheet, prefix: str) -> list[str]:
        if (
            balance_sheet.fixed_assets is None
            or balance_sheet.net_assets is None
            or balance_sheet.current_assets is None
            or balance_sheet.liabilities is None
            or balance_sheet.current_assets.debtors is None
            or balance_sheet.current_assets.cash_at_bank is None
            or balance_sheet.liabilities.creditors_within_one_year is None
            or balance_sheet.liabilities.pension_scheme_liability is None
        ):
            return []

        computed_net_assets = (
            balance_sheet.fixed_assets
            + balance_sheet.current_assets.debtors
            + balance_sheet.current_assets.cash_at_bank
            - balance_sheet.liabilities.creditors_within_one_year
            - balance_sheet.liabilities.pension_scheme_liability
        )
        if abs(computed_net_assets - balance_sheet.net_assets) > 1:
            return [
                f"{prefix} net_assets mismatch: computed {computed_net_assets} differs from reported {balance_sheet.net_assets}."
            ]
        return []

    @staticmethod
    def _parse_personnel_details(raw_personnel: Any) -> list[PersonnelDetail]:
        if not isinstance(raw_personnel, list):
            raise DocumentExtractionError("Expected `personnel_details` to be a list")
        personnel: list[PersonnelDetail] = []
        for index, row in enumerate(raw_personnel):
            try:
                personnel.append(PersonnelDetail.model_validate(row))
            except ValidationError as exc:
                raise DocumentExtractionError(
                    f"Invalid personnel_details row at index {index}: {exc}"
                ) from exc
        return personnel

    @staticmethod
    def _parse_balance_sheet(raw_balance: Any) -> list[BalanceSheetEntry]:
        if not isinstance(raw_balance, list):
            raise DocumentExtractionError("Expected `balance_sheet` to be a list")
        balance_sheet: list[BalanceSheetEntry] = []
        for index, row in enumerate(raw_balance):
            try:
                balance_sheet.append(BalanceSheetEntry.model_validate(row))
            except ValidationError as exc:
                raise DocumentExtractionError(
                    f"Invalid balance_sheet row at index {index}: {exc}"
                ) from exc
        return balance_sheet

    @staticmethod
    def _parse_metadata(raw_metadata: Any) -> Metadata:
        if not isinstance(raw_metadata, dict):
            raise DocumentExtractionError("Expected `metadata` to be an object")
        try:
            return Metadata.model_validate(raw_metadata)
        except ValidationError as exc:
            raise DocumentExtractionError(f"Invalid metadata payload: {exc}") from exc

    @staticmethod
    def _parse_governance(raw_governance: Any) -> Governance:
        if not isinstance(raw_governance, dict):
            raise DocumentExtractionError("Expected `governance` to be an object")
        try:
            return Governance.model_validate(raw_governance)
        except ValidationError as exc:
            raise DocumentExtractionError(f"Invalid governance payload: {exc}") from exc

    @staticmethod
    def _parse_statement_of_financial_activities(
        raw_sofa: Any,
    ) -> StatementOfFinancialActivities:
        if not isinstance(raw_sofa, dict):
            raise DocumentExtractionError(
                "Expected `statement_of_financial_activities` to be an object"
            )
        try:
            return StatementOfFinancialActivities.model_validate(raw_sofa)
        except ValidationError as exc:
            raise DocumentExtractionError(
                f"Invalid statement_of_financial_activities payload: {exc}"
            ) from exc

    @staticmethod
    def _parse_detailed_balance_sheet(raw_balance_sheet: Any) -> DetailedBalanceSheet:
        if not isinstance(raw_balance_sheet, dict):
            raise DocumentExtractionError("Expected `detailed_balance_sheet` to be an object")
        try:
            return DetailedBalanceSheet.model_validate(raw_balance_sheet)
        except ValidationError as exc:
            raise DocumentExtractionError(
                f"Invalid detailed_balance_sheet payload: {exc}"
            ) from exc

    @staticmethod
    def _parse_staffing_data(raw_staffing_data: Any) -> StaffingData:
        if not isinstance(raw_staffing_data, dict):
            raise DocumentExtractionError("Expected `staffing_data` to be an object")
        try:
            return StaffingData.model_validate(raw_staffing_data)
        except ValidationError as exc:
            raise DocumentExtractionError(
                f"Invalid staffing_data payload: {exc}"
            ) from exc

    @staticmethod
    def _parse_academy_trust_annual_report(raw_annual_report: Any) -> AcademyTrustAnnualReport:
        if not isinstance(raw_annual_report, dict):
            raise DocumentExtractionError(
                "Expected `academy_trust_annual_report` to be an object"
            )
        try:
            return AcademyTrustAnnualReport.model_validate(raw_annual_report)
        except ValidationError as exc:
            raise DocumentExtractionError(
                f"Invalid academy_trust_annual_report payload: {exc}"
            ) from exc

    @staticmethod
    def _require_non_empty(value: str, name: str) -> None:
        if not value or not str(value).strip():
            raise ValueError(f"{name} must not be empty")


__all__ = ["DocumentExtractionError", "OpenRouterDocumentExtractor"]
