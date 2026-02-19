import argparse
import json
import math
import os
import random
import re
import sqlite3
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from companies_house_client import CompaniesHouseClient
from document_extraction_models import ExtractionType
from openrouter_document_extractor import DocumentExtractionError, OpenRouterDocumentExtractor


DEFAULT_INPUT_XLSX = "SourceData/allgroupslinksdata20260217/Trusts.xlsx"
DEFAULT_OUTPUT_ROOT = "output/trusts_extraction"
DEFAULT_DB_NAME = "companies_house_extractions.db"
MAX_ERROR_TRACEBACK_CHARS = 4000


def _load_dotenv_file(env_path: Path) -> None:
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


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any) -> None:
    _ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _utc_now_precise() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    _ensure_parent(path)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _parse_fallback_models(raw_models: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in raw_models.split(","):
        model = part.strip()
        if not model or model in seen:
            continue
        seen.add(model)
        out.append(model)
    return out


def _is_file_not_supported_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "does not support file content types" in message
        or "invalid value: 'file'" in message
        or "messages[1].content[1].type" in message
    )


def _is_invalid_json_error(exc: Exception) -> bool:
    return "response was not valid json" in str(exc).lower()


def _is_schema_depth_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "maximum allowed nesting depth" in message or "schema-depth" in message


def _install_companies_house_request_throttle(
    client: CompaniesHouseClient, min_interval_seconds: float
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "enabled": min_interval_seconds > 0,
        "min_interval_seconds": min_interval_seconds,
        "request_count": 0,
    }
    if min_interval_seconds <= 0:
        return state

    original_request = client.session.request
    request_state = {"last_request_ts": 0.0}

    def throttled_request(*args: Any, **kwargs: Any) -> Any:
        now = time.monotonic()
        elapsed = now - request_state["last_request_ts"]
        wait = min_interval_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        request_state["last_request_ts"] = time.monotonic()
        state["request_count"] += 1
        return original_request(*args, **kwargs)

    client.session.request = throttled_request
    return state


def _estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes: int) -> int:
    """
    Coarse heuristic only.
    Uses 4 bytes/token as a generic approximation for reporting.
    """
    if pdf_size_bytes <= 0:
        return 0
    return int(math.ceil(pdf_size_bytes / 4.0))


def _is_full_accounts_filing(item: dict[str, Any]) -> bool:
    filing_type = str(item.get("type") or "").upper()
    description = str(item.get("description") or "").lower()
    if filing_type != "AA":
        return False
    return "accounts-with-accounts-type-full" in description or "full" in description


def _latest_full_accounts_document_id_from_filing_history(
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


def _extraction_types_for_schema_profile(schema_profile: str) -> list[ExtractionType]:
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
    raise ValueError(f"Unsupported schema_profile: {schema_profile}")


def _derive_annual_report_from_component_sections(
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


def _extract_with_model_fallback(
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
                # Persist full OpenRouter request/response artifacts for this exact model/attempt.
                # This is intentionally verbose and includes file_data base64 so debugging can
                # reconstruct the exact payload that was sent to the provider.
                safe_model = re.sub(r"[^A-Za-z0-9._-]+", "_", model).strip("_") or "model"
                attempt_dir = openrouter_debug_dir / f"{safe_model}_attempt{attempt}"
                attempt_dir.mkdir(parents=True, exist_ok=True)
                original_post = extractor._post_openrouter_chat_completion

                def _capture_post(payload: dict[str, Any]) -> dict[str, Any]:
                    # Full outbound payload (includes prompts, response_format, and file data URI).
                    write_json(attempt_dir / "openrouter_request_payload.json", payload)
                    # Convenience copy of just the schema body used for structured output.
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
                    # Raw provider response before text parsing / JSON parsing.
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
                if _is_file_not_supported_error(exc):
                    errors.append(f"{model}: {exc}")
                    break
                if _is_invalid_json_error(exc) and attempt < attempts:
                    errors.append(f"{model} attempt {attempt}/{attempts}: {exc}")
                    continue
                raise
    if errors:
        raise DocumentExtractionError(
            "All configured models failed file-input support checks: "
            + " | ".join(errors)
        )
    raise DocumentExtractionError("No models configured for extraction")


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            input_xlsx_path TEXT NOT NULL,
            output_run_dir TEXT NOT NULL,
            model TEXT NOT NULL,
            extraction_types_json TEXT NOT NULL,
            total_companies INTEGER NOT NULL DEFAULT 0,
            processed INTEGER NOT NULL DEFAULT 0,
            succeeded INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            source_row_index INTEGER NOT NULL,
            group_uid TEXT,
            group_id TEXT,
            group_name TEXT,
            company_number TEXT NOT NULL,
            company_name TEXT,
            status TEXT NOT NULL,
            document_id TEXT,
            pdf_path TEXT,
            profile_json_path TEXT,
            filing_history_json_path TEXT,
            extraction_json_path TEXT,
            warnings_json_path TEXT,
            profile_json TEXT,
            filing_history_json TEXT,
            extraction_json TEXT,
            warnings_json TEXT,
            model_used TEXT,
            pdf_size_bytes INTEGER,
            approx_llm_tokens INTEGER,
            error_message TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )
        """
    )
    cols = {
        row[1]
        for row in conn.execute("PRAGMA table_info(company_reports)").fetchall()
    }
    if "model_used" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN model_used TEXT")
    if "pdf_size_bytes" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN pdf_size_bytes INTEGER")
    if "approx_llm_tokens" not in cols:
        conn.execute("ALTER TABLE company_reports ADD COLUMN approx_llm_tokens INTEGER")
    conn.commit()


def _insert_run(
    conn: sqlite3.Connection,
    input_xlsx_path: str,
    output_run_dir: str,
    model: str,
    extraction_types: list[ExtractionType],
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO runs (
            started_at,
            input_xlsx_path,
            output_run_dir,
            model,
            extraction_types_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            _utc_now(),
            input_xlsx_path,
            output_run_dir,
            model,
            json.dumps([e.value for e in extraction_types]),
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def _finalize_run(
    conn: sqlite3.Connection,
    run_id: int,
    total_companies: int,
    processed: int,
    succeeded: int,
    failed: int,
) -> None:
    conn.execute(
        """
        UPDATE runs
        SET
            finished_at = ?,
            total_companies = ?,
            processed = ?,
            succeeded = ?,
            failed = ?
        WHERE run_id = ?
        """,
        (_utc_now(), total_companies, processed, succeeded, failed, run_id),
    )
    conn.commit()


def _insert_company_row(
    conn: sqlite3.Connection,
    payload: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO company_reports (
            run_id,
            source_row_index,
            group_uid,
            group_id,
            group_name,
            company_number,
            company_name,
            status,
            document_id,
            pdf_path,
            profile_json_path,
            filing_history_json_path,
            extraction_json_path,
            warnings_json_path,
            profile_json,
            filing_history_json,
            extraction_json,
            warnings_json,
            model_used,
            pdf_size_bytes,
            approx_llm_tokens,
            error_message,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["run_id"],
            payload["source_row_index"],
            payload.get("group_uid"),
            payload.get("group_id"),
            payload.get("group_name"),
            payload["company_number"],
            payload.get("company_name"),
            payload["status"],
            payload.get("document_id"),
            payload.get("pdf_path"),
            payload.get("profile_json_path"),
            payload.get("filing_history_json_path"),
            payload.get("extraction_json_path"),
            payload.get("warnings_json_path"),
            payload.get("profile_json"),
            payload.get("filing_history_json"),
            payload.get("extraction_json"),
            payload.get("warnings_json"),
            payload.get("model_used"),
            payload.get("pdf_size_bytes"),
            payload.get("approx_llm_tokens"),
            payload.get("error_message"),
            _utc_now(),
        ),
    )
    conn.commit()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch extract Companies House full account reports from Trusts.xlsx."
    )
    parser.add_argument(
        "--input-xlsx",
        default=DEFAULT_INPUT_XLSX,
        help="Path to Trusts.xlsx file",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Root directory to store PDFs and JSON outputs",
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="SQLite file path (defaults to <output-root>/companies_house_extractions.db)",
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=0,
        help="Optional limit for number of companies to process (0 means no limit)",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Start offset into deduplicated company list",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENROUTER_MODEL", "").strip(),
        help="OpenRouter model (defaults to OPENROUTER_MODEL env var)",
    )
    parser.add_argument(
        "--random-sample-size",
        type=int,
        default=0,
        help="Randomly sample this many companies from the selected batch (0 disables)",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=0,
        help="Optional seed for random sampling (0 uses non-deterministic sampling)",
    )
    parser.add_argument(
        "--fallback-models",
        default=os.getenv(
            "OPENROUTER_FALLBACK_MODELS",
            "",
        ),
        help="Optional comma-separated fallback models when primary model rejects file inputs",
    )
    parser.add_argument(
        "--ch-min-request-interval-seconds",
        type=float,
        default=float(os.getenv("CH_MIN_REQUEST_INTERVAL_SECONDS", "2.0")),
        help=(
            "Minimum spacing between Companies House HTTP requests. "
            "Default 2.0 sec (~25%% of documented 600/5min limit). Use 0 to disable."
        ),
    )
    parser.add_argument(
        "--write-summary-json",
        action="store_true",
        help="Write a run-level summary.json file with per-company outcomes and metrics",
    )
    parser.add_argument(
        "--filing-history-items-per-page",
        type=int,
        default=100,
        help="Filing history page size used for latest full-accounts selection (default 100)",
    )
    parser.add_argument(
        "--summary-json-path",
        default="",
        help="Optional explicit path for summary JSON (defaults to <run_dir>/summary.json when enabled)",
    )
    parser.add_argument(
        "--retries-on-invalid-json",
        type=int,
        default=2,
        help="Number of same-model retries when extraction output is malformed JSON (default 2)",
    )
    parser.add_argument(
        "--openrouter-timeout-seconds",
        type=float,
        default=float(os.getenv("OPENROUTER_TIMEOUT_SECONDS", "180")),
        help="HTTP timeout for each OpenRouter extraction call in seconds (default 180)",
    )
    parser.add_argument(
        "--write-openrouter-debug-artifacts",
        action="store_true",
        help=(
            "Write full per-attempt OpenRouter request/response artifacts under "
            "<run_dir>/<company>/extraction/openrouter_debug/. Includes full payload "
            "(with file_data URI), extracted schema, and raw provider response."
        ),
    )
    parser.add_argument(
        "--schema-profile",
        choices=["compact_single_call", "full_legacy", "light_core"],
        default=os.getenv("BATCH_SCHEMA_PROFILE", "compact_single_call"),
        help=(
            "Extraction schema profile. "
            "compact_single_call minimizes nesting while keeping broad coverage."
        ),
    )
    return parser


def main() -> int:
    _load_dotenv_file(Path(".env"))
    args = build_parser().parse_args()

    input_xlsx = Path(args.input_xlsx)
    if not input_xlsx.exists() or not input_xlsx.is_file():
        raise FileNotFoundError(f"Input xlsx file not found: {input_xlsx}")

    ch_api_key = os.getenv("CH_API_KEY")
    if not ch_api_key:
        raise ValueError("Missing CH_API_KEY. Set in environment or .env.")
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
    if not openrouter_api_key:
        raise ValueError("Missing OPENROUTER_API_KEY. Set in environment or .env.")
    if not str(args.model or "").strip():
        raise ValueError(
            "Missing model. Set OPENROUTER_MODEL in .env or pass --model explicitly."
        )
    if args.ch_min_request_interval_seconds < 0:
        raise ValueError("ch_min_request_interval_seconds must be >= 0")
    if args.filing_history_items_per_page <= 0:
        raise ValueError("filing_history_items_per_page must be > 0")
    if args.retries_on_invalid_json < 0:
        raise ValueError("retries_on_invalid_json must be >= 0")
    if args.openrouter_timeout_seconds <= 0:
        raise ValueError("openrouter_timeout_seconds must be > 0")

    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_run_dir = output_root / f"run_{run_stamp}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path) if args.db_path else output_root / DEFAULT_DB_NAME
    _ensure_parent(db_path)
    conn = sqlite3.connect(db_path)
    _create_tables(conn)

    extraction_types = _extraction_types_for_schema_profile(args.schema_profile)

    run_id = _insert_run(
        conn=conn,
        input_xlsx_path=str(input_xlsx),
        output_run_dir=str(output_run_dir),
        model=args.model,
        extraction_types=extraction_types,
    )
    events_jsonl_path = output_run_dir / "events.jsonl"

    def emit_event(event_type: str, **fields: Any) -> None:
        payload: dict[str, Any] = {
            "ts_utc": _utc_now_precise(),
            "run_id": run_id,
            "event": event_type,
        }
        payload.update(fields)
        _append_jsonl(events_jsonl_path, payload)

    print(f"[run {run_id}] output_dir={output_run_dir}")
    print(f"[run {run_id}] db_path={db_path}")

    rows = read_xlsx_rows(input_xlsx)
    seen: set[str] = set()
    batch: list[dict[str, Any]] = []
    for source_index, row in enumerate(rows, start=2):
        normalized = normalize_company_number(row.get("Companies House Number", ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        batch.append(
            {
                "source_row_index": source_index,
                "group_uid": row.get("Group UID"),
                "group_id": row.get("Group ID"),
                "group_name": row.get("Group Name"),
                "company_number": normalized,
            }
        )

    if args.start_index > 0:
        batch = batch[args.start_index :]
    if args.max_companies > 0:
        batch = batch[: args.max_companies]
    if args.random_sample_size > 0 and len(batch) > args.random_sample_size:
        rng = random.Random(None if args.random_seed == 0 else args.random_seed)
        batch = rng.sample(batch, args.random_sample_size)

    total_companies = len(batch)
    model_candidates = [args.model] + _parse_fallback_models(args.fallback_models)
    deduped_models: list[str] = []
    seen_models: set[str] = set()
    for model in model_candidates:
        if model in seen_models:
            continue
        seen_models.add(model)
        deduped_models.append(model)
    model_candidates = deduped_models

    print(f"[run {run_id}] companies_to_process={total_companies}")
    print(f"[run {run_id}] models={model_candidates}")
    print(
        "[run {}] companies_house_min_request_interval_seconds={}".format(
            run_id, args.ch_min_request_interval_seconds
        )
    )
    print(
        "[run {}] filing_history_items_per_page={}".format(
            run_id, args.filing_history_items_per_page
        )
    )
    print(
        "[run {}] retries_on_invalid_json={}".format(
            run_id, args.retries_on_invalid_json
        )
    )
    print(
        "[run {}] openrouter_timeout_seconds={}".format(
            run_id, args.openrouter_timeout_seconds
        )
    )
    print(
        "[run {}] write_openrouter_debug_artifacts={}".format(
            run_id, args.write_openrouter_debug_artifacts
        )
    )
    print("[run {}] schema_profile={}".format(run_id, args.schema_profile))
    emit_event(
        "run_started",
        output_run_dir=str(output_run_dir),
        input_xlsx_path=str(input_xlsx),
        model=args.model,
        model_candidates=model_candidates,
        schema_profile=args.schema_profile,
        total_companies=total_companies,
        ch_min_request_interval_seconds=args.ch_min_request_interval_seconds,
        filing_history_items_per_page=args.filing_history_items_per_page,
        retries_on_invalid_json=args.retries_on_invalid_json,
        openrouter_timeout_seconds=args.openrouter_timeout_seconds,
        write_openrouter_debug_artifacts=args.write_openrouter_debug_artifacts,
    )

    client = CompaniesHouseClient(api_key=ch_api_key)
    throttle_state = _install_companies_house_request_throttle(
        client=client,
        min_interval_seconds=args.ch_min_request_interval_seconds,
    )

    processed = 0
    succeeded = 0
    failed = 0
    company_summaries: list[dict[str, Any]] = []

    for item in batch:
        processed += 1
        company_number = item["company_number"]
        prefix = f"[run {run_id}] [{processed}/{total_companies}] {company_number}"
        print(f"{prefix} start")
        company_started_monotonic = time.monotonic()
        company_dir = output_run_dir / company_number
        api_dir = company_dir / "api"
        doc_dir = company_dir / "documents"
        extraction_dir = company_dir / "extraction"
        api_dir.mkdir(parents=True, exist_ok=True)
        doc_dir.mkdir(parents=True, exist_ok=True)
        extraction_dir.mkdir(parents=True, exist_ok=True)
        stage = "start"
        summary_row: dict[str, Any] = {
            "source_row_index": item["source_row_index"],
            "group_uid": item.get("group_uid"),
            "group_id": item.get("group_id"),
            "group_name": item.get("group_name"),
            "company_number": company_number,
            "status": "failed",
            "companies_house_stage": stage,
            "document_id": None,
            "pdf_path": None,
            "pdf_size_bytes": None,
            "approx_llm_tokens": None,
            "model_used": None,
            "error": None,
        }
        current_stage_started_monotonic = company_started_monotonic

        def stage_start(stage_name: str) -> None:
            nonlocal stage, current_stage_started_monotonic
            stage = stage_name
            current_stage_started_monotonic = time.monotonic()
            emit_event(
                "company_stage_start",
                index=processed,
                total=total_companies,
                company_number=company_number,
                stage=stage_name,
            )

        def stage_end(status: str, **fields: Any) -> None:
            emit_event(
                "company_stage_end",
                index=processed,
                total=total_companies,
                company_number=company_number,
                stage=stage,
                status=status,
                duration_seconds=round(
                    time.monotonic() - current_stage_started_monotonic, 3
                ),
                **fields,
            )

        emit_event(
            "company_start",
            index=processed,
            total=total_companies,
            company_number=company_number,
            source_row_index=item["source_row_index"],
            group_uid=item.get("group_uid"),
            group_id=item.get("group_id"),
            group_name=item.get("group_name"),
        )

        try:
            stage_start("get_company_profile")
            profile = client.get_company_profile(company_number)
            stage_end("ok")
            stage_start("get_filing_history")
            filing_history_page = client.get_filing_history(
                company_number=company_number,
                items_per_page=args.filing_history_items_per_page,
                start_index=0,
            )
            filing_history = filing_history_page.get("items") or []
            stage_end("ok", filing_items=len(filing_history))
            stage_start("select_latest_full_accounts")
            document_id = _latest_full_accounts_document_id_from_filing_history(filing_history)
            if not document_id:
                raise ValueError("No full accounts document found")
            stage_end("ok", document_id=document_id)
            summary_row["document_id"] = document_id

            stage_start("download_document")
            pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
            downloaded_path = client.download_document(
                document_id=document_id,
                output_path=str(pdf_path),
                accept="application/pdf",
                company_number=company_number,
            )
            stage_end(
                "ok",
                pdf_path=str(pdf_path),
                cache_hit=client.last_download_cache_hit,
            )
            pdf_size_bytes = Path(downloaded_path).stat().st_size
            approx_llm_tokens = _estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes)
            summary_row["pdf_path"] = str(pdf_path)
            summary_row["pdf_size_bytes"] = pdf_size_bytes
            summary_row["approx_llm_tokens"] = approx_llm_tokens

            extraction_schema_profile = args.schema_profile
            had_schema_depth_error = False
            run_extraction_types = extraction_types

            stage_start("openrouter_extract")
            try:
                openrouter_debug_dir = (
                    extraction_dir / "openrouter_debug"
                    if args.write_openrouter_debug_artifacts
                    else None
                )
                extraction_payload, warnings_payload, model_used = _extract_with_model_fallback(
                    api_key=openrouter_api_key,
                    model_candidates=model_candidates,
                    document_path=downloaded_path,
                    extraction_types=run_extraction_types,
                    retries_on_invalid_json=args.retries_on_invalid_json,
                    openrouter_timeout_seconds=args.openrouter_timeout_seconds,
                    openrouter_debug_dir=openrouter_debug_dir,
                )
            except DocumentExtractionError as exc:
                if (
                    args.schema_profile == "compact_single_call"
                    and _is_schema_depth_error(exc)
                ):
                    had_schema_depth_error = True
                    extraction_schema_profile = "light_core"
                    run_extraction_types = _extraction_types_for_schema_profile(
                        extraction_schema_profile
                    )
                    extraction_payload, warnings_payload, model_used = _extract_with_model_fallback(
                        api_key=openrouter_api_key,
                        model_candidates=model_candidates,
                        document_path=downloaded_path,
                        extraction_types=run_extraction_types,
                        retries_on_invalid_json=args.retries_on_invalid_json,
                        openrouter_timeout_seconds=args.openrouter_timeout_seconds,
                        openrouter_debug_dir=openrouter_debug_dir,
                    )
                else:
                    raise
            stage_end(
                "ok",
                model_used=model_used,
                schema_profile=extraction_schema_profile,
                schema_profile_fallback_applied=had_schema_depth_error,
            )

            if extraction_payload.get("academy_trust_annual_report") is None:
                derived = _derive_annual_report_from_component_sections(extraction_payload)
                if derived is not None:
                    extraction_payload["academy_trust_annual_report"] = derived
            stage = "extraction_ok"

            profile_path = api_dir / "profile.json"
            filing_history_path = api_dir / "filing_history.json"
            extraction_path = extraction_dir / "extraction_result.json"
            warnings_path = extraction_dir / "validation_warnings.json"
            run_report_path = extraction_dir / "run_report.json"

            run_report = {
                "run_id": run_id,
                "source_row_index": item["source_row_index"],
                "group_uid": item.get("group_uid"),
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "company_number": company_number,
                "company_name": profile.get("company_name"),
                "document_id": document_id,
                "pdf_path": str(pdf_path),
                "pdf_size_bytes": pdf_size_bytes,
                "approx_llm_tokens": approx_llm_tokens,
                "model": args.model,
                "model_used": model_used,
                "schema_profile": extraction_schema_profile,
                "requested_types": [t.value for t in run_extraction_types],
                "schema_profile_fallback_applied": had_schema_depth_error,
                "extraction_result": extraction_payload,
            }

            write_json(profile_path, profile)
            write_json(filing_history_path, filing_history)
            write_json(extraction_path, extraction_payload)
            write_json(warnings_path, warnings_payload)
            write_json(run_report_path, run_report)

            _insert_company_row(
                conn,
                {
                    "run_id": run_id,
                    "source_row_index": item["source_row_index"],
                    "group_uid": item.get("group_uid"),
                    "group_id": item.get("group_id"),
                    "group_name": item.get("group_name"),
                    "company_number": company_number,
                    "company_name": profile.get("company_name"),
                    "status": "success",
                    "document_id": document_id,
                    "pdf_path": str(pdf_path),
                    "profile_json_path": str(profile_path),
                    "filing_history_json_path": str(filing_history_path),
                    "extraction_json_path": str(extraction_path),
                    "warnings_json_path": str(warnings_path),
                    "profile_json": json.dumps(profile),
                    "filing_history_json": json.dumps(filing_history),
                    "extraction_json": json.dumps(extraction_payload),
                    "warnings_json": json.dumps(warnings_payload),
                    "model_used": model_used,
                    "pdf_size_bytes": pdf_size_bytes,
                    "approx_llm_tokens": approx_llm_tokens,
                    "error_message": None,
                },
            )
            succeeded += 1
            summary_row.update(
                {
                    "status": "success",
                    "companies_house_stage": stage,
                    "company_name": profile.get("company_name"),
                    "document_id": document_id,
                    "pdf_path": str(pdf_path),
                    "pdf_size_bytes": pdf_size_bytes,
                    "approx_llm_tokens": approx_llm_tokens,
                    "model_used": model_used,
                    "schema_profile": extraction_schema_profile,
                    "schema_profile_fallback_applied": had_schema_depth_error,
                    "error": None,
                }
            )
            emit_event(
                "company_success",
                index=processed,
                total=total_companies,
                company_number=company_number,
                company_name=profile.get("company_name"),
                document_id=document_id,
                model_used=model_used,
                pdf_size_bytes=pdf_size_bytes,
                approx_llm_tokens=approx_llm_tokens,
                total_duration_seconds=round(
                    time.monotonic() - company_started_monotonic, 3
                ),
            )
            print(f"{prefix} success document_id={document_id} model={model_used}")
        except Exception as exc:
            failed += 1
            raw_response_json_path: Path | None = None
            raw_response_text_path: Path | None = None
            raw_payload = getattr(exc, "raw_response_payload", None)
            raw_text = getattr(exc, "raw_response_text", None)
            if raw_payload is not None:
                raw_response_json_path = extraction_dir / "raw_openrouter_response.json"
                write_json(raw_response_json_path, raw_payload)
            if raw_text:
                raw_response_text_path = extraction_dir / "raw_openrouter_response_text.txt"
                raw_response_text_path.write_text(str(raw_text), encoding="utf-8")
            stage_end("error", error=str(exc))
            error_message = "".join(
                traceback.format_exception(exc.__class__, exc, exc.__traceback__)
            )[:MAX_ERROR_TRACEBACK_CHARS]
            if raw_response_json_path is not None:
                error_message = (
                    error_message
                    + f"\nraw_openrouter_response_json={raw_response_json_path}"
                )[:MAX_ERROR_TRACEBACK_CHARS]
            if raw_response_text_path is not None:
                error_message = (
                    error_message
                    + f"\nraw_openrouter_response_text={raw_response_text_path}"
                )[:MAX_ERROR_TRACEBACK_CHARS]
            _insert_company_row(
                conn,
                {
                    "run_id": run_id,
                    "source_row_index": item["source_row_index"],
                    "group_uid": item.get("group_uid"),
                    "group_id": item.get("group_id"),
                    "group_name": item.get("group_name"),
                    "company_number": company_number,
                    "company_name": None,
                    "status": "failed",
                    "document_id": None,
                    "pdf_path": None,
                    "profile_json_path": None,
                    "filing_history_json_path": None,
                    "extraction_json_path": None,
                    "warnings_json_path": None,
                    "profile_json": None,
                    "filing_history_json": None,
                    "extraction_json": None,
                    "warnings_json": None,
                    "model_used": None,
                    "pdf_size_bytes": None,
                    "approx_llm_tokens": None,
                    "error_message": error_message,
                },
            )
            summary_row.update(
                {
                    "status": "failed",
                    "companies_house_stage": stage,
                    "error": str(exc),
                    "raw_openrouter_response_json": (
                        str(raw_response_json_path) if raw_response_json_path else None
                    ),
                    "raw_openrouter_response_text": (
                        str(raw_response_text_path) if raw_response_text_path else None
                    ),
                }
            )
            emit_event(
                "company_failed",
                index=processed,
                total=total_companies,
                company_number=company_number,
                stage=stage,
                error=str(exc),
                total_duration_seconds=round(
                    time.monotonic() - company_started_monotonic, 3
                ),
            )
            print(f"{prefix} failed error={exc}")
        company_summaries.append(summary_row)

    _finalize_run(
        conn=conn,
        run_id=run_id,
        total_companies=total_companies,
        processed=processed,
        succeeded=succeeded,
        failed=failed,
    )
    conn.close()

    if args.write_summary_json or args.summary_json_path:
        summary_path = (
            Path(args.summary_json_path)
            if args.summary_json_path
            else output_run_dir / "summary.json"
        )
        summary_payload = {
            "run_type": "batch_extract_trusts",
            "run_id": run_id,
            "timestamp_utc": _utc_now(),
            "input_xlsx_path": str(input_xlsx),
            "output_run_dir": str(output_run_dir),
            "model": args.model,
            "model_candidates": model_candidates,
            "schema_profile": args.schema_profile,
            "requested_types": [t.value for t in extraction_types],
            "companies_house_min_request_interval_seconds": args.ch_min_request_interval_seconds,
            "estimated_companies_house_rate_requests_per_second": (
                (1.0 / args.ch_min_request_interval_seconds)
                if args.ch_min_request_interval_seconds > 0
                else None
            ),
            "companies_house_request_count": throttle_state["request_count"],
            "total_companies": total_companies,
            "processed": processed,
            "succeeded": succeeded,
            "failed": failed,
            "companies": company_summaries,
        }
        write_json(summary_path, summary_payload)
        emit_event("run_summary_written", summary_json_path=str(summary_path))
        print(f"[run {run_id}] summary_json_path={summary_path}")

    emit_event(
        "run_completed",
        processed=processed,
        succeeded=succeeded,
        failed=failed,
        companies_house_request_count=throttle_state["request_count"],
    )
    print(
        f"[run {run_id}] complete processed={processed} succeeded={succeeded} failed={failed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
