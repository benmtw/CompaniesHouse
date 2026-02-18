import base64
import json
import os
import re
from pathlib import Path
from typing import Any

import requests
from pydantic import ValidationError

from document_extraction_models import (
    BalanceSheetEntry,
    ExtractionResult,
    ExtractionType,
    PersonnelDetail,
)


class DocumentExtractionError(Exception):
    """Raised when a document cannot be parsed or validated for extraction."""


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

        self.api_key = resolved_key
        self.model = model.strip()
        self.max_document_chars = max_document_chars

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
        return self._build_result(
            payload=payload,
            document_path=document_path,
            requested_types=requested_types,
        )

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

        response_text = self._response_text_from_completion(response)
        return self._parse_json_response(response_text)

    @staticmethod
    def _build_prompts(requested_types: list[ExtractionType]) -> tuple[str, str]:
        task_lines: list[str] = []
        if ExtractionType.PersonnelDetails in requested_types:
            task_lines.append(
                "- Extract personnel details with fields: first_name, last_name, job_title."
            )
        if ExtractionType.BalanceSheet in requested_types:
            task_lines.append("- Extract balance sheet values as line items.")

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
            response = requests.post(url, headers=headers, json=payload, timeout=180)
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

        if ExtractionType.PersonnelDetails in requested_types:
            raw_personnel = payload.get("personnel_details", [])
            personnel_details = self._parse_personnel_details(raw_personnel)

        if ExtractionType.BalanceSheet in requested_types:
            raw_balance = payload.get("balance_sheet", [])
            balance_sheet = self._parse_balance_sheet(raw_balance)

        try:
            return ExtractionResult(
                source_path=document_path,
                model=self.model,
                requested_types=requested_types,
                personnel_details=personnel_details,
                balance_sheet=balance_sheet,
            )
        except ValidationError as exc:
            raise DocumentExtractionError(f"Invalid extraction result: {exc}") from exc

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
    def _require_non_empty(value: str, name: str) -> None:
        if not value or not str(value).strip():
            raise ValueError(f"{name} must not be empty")


__all__ = ["DocumentExtractionError", "OpenRouterDocumentExtractor"]
