import argparse
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from companies_house_client import CompaniesHouseClient
from document_extraction_models import ExtractionType
from openrouter_document_extractor import DocumentExtractionError, OpenRouterDocumentExtractor


def load_dotenv_file(env_path: Path) -> None:
    if not env_path.exists() or not env_path.is_file():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


def _column_label_to_index(label: str) -> int:
    value = 0
    for char in label:
        if not ("A" <= char <= "Z"):
            break
        value = (value * 26) + (ord(char) - ord("A") + 1)
    return max(value - 1, 0)


def _cell_ref_to_col_index(cell_ref: str | None, fallback_index: int) -> int:
    if not cell_ref:
        return fallback_index
    letters = []
    for char in cell_ref:
        if char.isalpha():
            letters.append(char.upper())
        else:
            break
    if not letters:
        return fallback_index
    return _column_label_to_index("".join(letters))


def _xlsx_cell_text(cell: ET.Element, ns: dict[str, str], shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        parts = [node.text or "" for node in cell.findall(".//a:is/a:t", ns)]
        return "".join(parts).strip()
    value_node = cell.find("a:v", ns)
    if value_node is None or value_node.text is None:
        return ""
    raw = value_node.text.strip()
    if cell_type == "s":
        try:
            idx = int(raw)
            if 0 <= idx < len(shared_strings):
                return shared_strings[idx].strip()
            return ""
        except ValueError:
            return ""
    return raw


def read_xlsx_rows(xlsx_path: Path) -> list[dict[str, str]]:
    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    with ZipFile(xlsx_path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in shared_root.findall("a:si", ns):
                parts = [node.text or "" for node in si.findall(".//a:t", ns)]
                shared_strings.append("".join(parts))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheets = workbook.findall(".//a:sheets/a:sheet", ns)
        if not sheets:
            return []

        first_sheet = sheets[0]
        rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        if not rel_id:
            return []

        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rel_root:
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib.get("Target")
                break
        if not target:
            return []

        worksheet = ET.fromstring(zf.read(f"xl/{target}"))
        row_nodes = worksheet.findall(".//a:sheetData/a:row", ns)
        if not row_nodes:
            return []

        parsed_rows: list[list[str]] = []
        max_cols = 0
        for row in row_nodes:
            values: list[str] = []
            fallback_index = 0
            for cell in row.findall("a:c", ns):
                col_index = _cell_ref_to_col_index(cell.attrib.get("r"), fallback_index)
                while len(values) <= col_index:
                    values.append("")
                values[col_index] = _xlsx_cell_text(cell, ns, shared_strings)
                fallback_index = col_index + 1
            max_cols = max(max_cols, len(values))
            parsed_rows.append(values)

        if not parsed_rows:
            return []

        headers = [h.strip() for h in parsed_rows[0]]
        if len(headers) < max_cols:
            headers.extend([""] * (max_cols - len(headers)))

        normalized_headers = []
        for idx, header in enumerate(headers):
            normalized_headers.append(header if header else f"column_{idx}")

        records: list[dict[str, str]] = []
        for row_values in parsed_rows[1:]:
            if len(row_values) < len(normalized_headers):
                row_values = row_values + ([""] * (len(normalized_headers) - len(row_values)))
            record = {
                normalized_headers[idx]: str(row_values[idx]).strip()
                for idx in range(len(normalized_headers))
            }
            records.append(record)
        return records


def normalize_company_number(raw_value: str) -> str | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    if len(digits) > 8:
        return None
    return digits.zfill(8)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def utc_now_precise() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def parse_fallback_models(raw_models: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_models.split(","):
        model = part.strip()
        if not model or model in seen:
            continue
        seen.add(model)
        out.append(model)
    return out


def resolve_cached_pdf_for_company(
    company_number: str,
    cache_dir: Path,
    accept: str = "application/pdf",
) -> tuple[Path | None, str | None]:
    index_path = cache_dir / "cache_index.jsonl"
    if not index_path.is_file():
        return None, None

    accept_normalized = accept.strip().lower()
    candidates: list[tuple[Path, str | None]] = []
    with index_path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue

            if str(payload.get("company_number") or "").strip() != company_number:
                continue
            if str(payload.get("accept") or "").strip().lower() != accept_normalized:
                continue

            cache_path_raw = str(payload.get("cache_path") or "").strip()
            if not cache_path_raw:
                continue
            document_id = str(payload.get("document_id") or "").strip() or None
            candidates.append((Path(cache_path_raw), document_id))

    for cache_path, document_id in reversed(candidates):
        if cache_path.is_file() and cache_path.stat().st_size > 0:
            return cache_path, document_id
    return None, None


def _is_full_accounts_filing(item: dict[str, Any]) -> bool:
    filing_type = str(item.get("type") or "").upper()
    description = str(item.get("description") or "").lower()
    if filing_type != "AA":
        return False
    return "accounts-with-accounts-type-full" in description or "full" in description


def latest_full_accounts_document_id_from_filing_history(
    filings: list[dict[str, Any]],
) -> str | None:
    candidates: list[dict[str, Any]] = []
    for item in filings:
        if not _is_full_accounts_filing(item):
            continue
        links = item.get("links") or {}
        metadata_url = str(links.get("document_metadata") or "").strip()
        if not metadata_url:
            continue
        candidates.append(item)

    if not candidates:
        return None

    latest_item = sorted(candidates, key=lambda i: (str(i.get("date") or "")), reverse=True)[0]
    metadata_url = str((latest_item.get("links") or {}).get("document_metadata") or "").strip()
    return CompaniesHouseClient._extract_document_id(metadata_url)


def extraction_types_for_schema_profile(schema_profile: str) -> list[ExtractionType]:
    if schema_profile == "full_legacy":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
            ExtractionType.StatementOfFinancialActivities,
            ExtractionType.DetailedBalanceSheet,
            ExtractionType.StaffingData,
            ExtractionType.AcademyTrustAnnualReport,
        ]
    if schema_profile == "compact_single_call":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
            ExtractionType.StatementOfFinancialActivities,
            ExtractionType.DetailedBalanceSheet,
            ExtractionType.StaffingData,
        ]
    if schema_profile == "light_core":
        return [
            ExtractionType.PersonnelDetails,
            ExtractionType.BalanceSheet,
            ExtractionType.Metadata,
            ExtractionType.Governance,
        ]
    if schema_profile == "personnel_only":
        return [ExtractionType.PersonnelDetails]
    raise ValueError(f"Unsupported schema_profile: {schema_profile}")


def derive_annual_report_from_component_sections(
    extraction_payload: dict[str, Any],
) -> dict[str, Any] | None:
    metadata = extraction_payload.get("metadata")
    governance = extraction_payload.get("governance")
    sofa = extraction_payload.get("statement_of_financial_activities")
    balance_sheet = extraction_payload.get("detailed_balance_sheet")
    staffing_data = extraction_payload.get("staffing_data")
    if (
        metadata is None
        and governance is None
        and sofa is None
        and balance_sheet is None
        and staffing_data is None
    ):
        return None
    return {
        "metadata": metadata,
        "governance": governance,
        "statement_of_financial_activities": sofa,
        "balance_sheet": balance_sheet,
        "staffing_data": staffing_data,
    }


def is_file_not_supported_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not support file content types" in message
        or "invalid value: 'file'" in message
        or "messages[1].content[1].type" in message
    )


def is_invalid_json_error(exc: Exception) -> bool:
    return "response was not valid json" in str(exc).lower()


def extract_with_model_fallback(
    api_key: str,
    model_candidates: list[str],
    document_path: str,
    extraction_types: list[ExtractionType],
    retries_on_invalid_json: int = 2,
    openrouter_timeout_seconds: float = 180.0,
    openrouter_debug_dir: Path | None = None,
) -> tuple[dict[str, Any], list[str], str]:
    errors: list[str] = []
    for model in model_candidates:
        attempts = retries_on_invalid_json + 1
        for attempt in range(1, attempts + 1):
            extractor = OpenRouterDocumentExtractor(
                api_key=api_key,
                model=model,
                request_timeout_seconds=openrouter_timeout_seconds,
            )
            if openrouter_debug_dir is not None:
                safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model).strip("_") or "model"
                attempt_dir = openrouter_debug_dir / f"{safe_model}_attempt{attempt}"
                attempt_dir.mkdir(parents=True, exist_ok=True)
                original_post = extractor._post_openrouter_chat_completion

                def _capture_post(payload: dict[str, Any]) -> dict[str, Any]:
                    write_json(attempt_dir / "openrouter_request_payload.json", payload)
                    schema = (
                        (payload.get("response_format") or {})
                        .get("json_schema", {})
                        .get("schema", {})
                    )
                    write_json(
                        attempt_dir / "response_format.json_schema.schema.json",
                        schema,
                    )
                    response = original_post(payload)
                    write_json(attempt_dir / "raw_openrouter_response.json", response)
                    return response

                extractor._post_openrouter_chat_completion = _capture_post
            try:
                result = extractor.extract(
                    document_path=document_path,
                    extraction_types=extraction_types,
                )
                return result.model_dump(mode="json"), result.validation_warnings or [], model
            except DocumentExtractionError as exc:
                if is_file_not_supported_error(exc):
                    errors.append(f"{model}: {exc}")
                    break
                if is_invalid_json_error(exc) and attempt < attempts:
                    errors.append(f"{model} attempt {attempt}/{attempts}: {exc}")
                    continue
                raise
    if errors:
        raise DocumentExtractionError(
            "All configured models failed file-input support checks: " + " | ".join(errors)
        )
    raise DocumentExtractionError("No models configured for extraction")


def add_common_extraction_cli_args(
    parser: argparse.ArgumentParser,
    *,
    output_root_default: str,
    db_help: str,
    output_root_help: str | None = None,
    model_help: str | None = None,
) -> None:
    parser.add_argument("--output-root", default=output_root_default, help=output_root_help)
    parser.add_argument(
        "--db-path",
        default="",
        help=db_help,
    )
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--max-companies", type=int, default=0)
    parser.add_argument("--random-sample-size", type=int, default=0)
    parser.add_argument("--random-seed", type=int, default=0)
    parser.add_argument("--filing-history-items-per-page", type=int, default=100)
    parser.add_argument(
        "--ch-min-request-interval-seconds",
        type=float,
        default=float(os.getenv("CH_MIN_REQUEST_INTERVAL_SECONDS", "2.0")),
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENROUTER_MODEL", "").strip(),
        help=model_help,
    )
    parser.add_argument(
        "--fallback-models",
        default=os.getenv("OPENROUTER_FALLBACK_MODELS", ""),
    )
    parser.add_argument(
        "--schema-profile",
        choices=["compact_single_call", "full_legacy", "light_core", "personnel_only"],
        default=os.getenv("BATCH_SCHEMA_PROFILE", "compact_single_call"),
    )
    parser.add_argument("--retries-on-invalid-json", type=int, default=2)
    parser.add_argument(
        "--openrouter-timeout-seconds",
        type=float,
        default=float(os.getenv("OPENROUTER_TIMEOUT_SECONDS", "180")),
    )
    parser.add_argument("--write-openrouter-debug-artifacts", action="store_true")
    parser.add_argument("--write-summary-json", action="store_true")
    parser.add_argument("--summary-json-path", default="")


def install_request_throttle(
    client: CompaniesHouseClient,
    min_interval_seconds: float,
    *,
    lock: Any | None = None,
    shared_state: dict[str, Any] | None = None,
    shared_state_path: str | None = None,
    shared_lock_path: str | None = None,
) -> dict[str, Any]:
    state = shared_state if shared_state is not None else {}
    state["enabled"] = min_interval_seconds > 0
    state["min_interval_seconds"] = min_interval_seconds
    state.setdefault("last_request_ts", 0.0)
    state.setdefault("request_count", 0)
    if min_interval_seconds <= 0:
        return state

    throttle_lock = lock if lock is not None else threading.Lock()
    original_request = client.session.request

    def _update_file_backed_state() -> None:
        if not shared_state_path or not shared_lock_path:
            return

        lock_path = Path(shared_lock_path)
        state_path = Path(shared_state_path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.parent.mkdir(parents=True, exist_ok=True)

        lock_fd: int | None = None
        while lock_fd is None:
            try:
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileExistsError:
                time.sleep(0.005)

        try:
            now = time.monotonic()
            file_state: dict[str, Any] = {
                "last_request_ts": 0.0,
                "request_count": 0,
            }
            if state_path.is_file():
                try:
                    file_state.update(
                        json.loads(state_path.read_text(encoding="utf-8"))
                    )
                except json.JSONDecodeError:
                    pass

            elapsed = now - float(file_state.get("last_request_ts", 0.0))
            wait = min_interval_seconds - elapsed
            if wait > 0:
                time.sleep(wait)

            file_state["last_request_ts"] = time.monotonic()
            file_state["request_count"] = int(file_state.get("request_count", 0)) + 1
            state["last_request_ts"] = float(file_state["last_request_ts"])
            state["request_count"] = int(file_state["request_count"])
            state_path.write_text(json.dumps(file_state), encoding="utf-8")
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    def throttled_request(*args: Any, **kwargs: Any) -> Any:
        with throttle_lock:
            if shared_state_path and shared_lock_path:
                _update_file_backed_state()
            else:
                now = time.monotonic()
                elapsed = now - float(state["last_request_ts"])
                wait = min_interval_seconds - elapsed
                if wait > 0:
                    time.sleep(wait)
                state["last_request_ts"] = time.monotonic()
                state["request_count"] = int(state["request_count"]) + 1
        return original_request(*args, **kwargs)

    client.session.request = throttled_request
    return state


__all__ = [
    "append_jsonl",
    "add_common_extraction_cli_args",
    "derive_annual_report_from_component_sections",
    "ensure_parent",
    "extract_with_model_fallback",
    "extraction_types_for_schema_profile",
    "latest_full_accounts_document_id_from_filing_history",
    "load_dotenv_file",
    "normalize_company_number",
    "parse_fallback_models",
    "read_xlsx_rows",
    "resolve_cached_pdf_for_company",
    "install_request_throttle",
    "utc_now",
    "utc_now_precise",
    "write_json",
]
