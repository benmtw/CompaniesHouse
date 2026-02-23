import argparse
import json
import os
import random
import sqlite3
import time
import traceback
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from openrouter_document_extractor import DocumentExtractionError
from shared import (
    DEFAULT_DB_NAME,
    DEFAULT_INPUT_XLSX,
    DEFAULT_OUTPUT_ROOT,
    MAX_ERROR_TRACEBACK_CHARS,
    create_tables,
    deduplicate_ordered,
    derive_annual_report_from_component_sections,
    ensure_parent,
    estimate_llm_tokens_for_pdf_bytes,
    extract_with_model_fallback,
    extraction_types_for_schema_profile,
    finalize_run,
    insert_company_row,
    insert_run,
    is_schema_depth_error,
    latest_full_accounts_document_id,
    parse_fallback_models,
    utc_now_iso,
    write_json,
)


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
    alnum = "".join(ch for ch in text.upper() if ch.isalnum())
    if not alnum:
        return None

    # Prefix-based company numbers such as SC123456 and NI123456 are valid and
    # must be preserved. We only left-pad when the input is purely numeric.
    if alnum.isdigit():
        if len(alnum) > 8:
            return None
        return alnum.zfill(8)

    if len(alnum) > 8:
        return None
    return alnum


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch extract Companies House full account reports."
    )
    parser.add_argument(
        "--input-xlsx",
        default=DEFAULT_INPUT_XLSX,
        help="Path to input XLSX file containing company numbers",
    )
    parser.add_argument(
        "--company-type",
        choices=[ct.value for ct in CompanyType],
        default=CompanyType.GENERIC.value,
        help=(
            "Type of company being processed. Controls LLM prompt text and "
            "extraction model fields (default: generic)"
        ),
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
        "--schema-profile",
        choices=["compact_single_call", "full_legacy", "light_core"],
        default=os.getenv("BATCH_SCHEMA_PROFILE", "compact_single_call"),
        help=(
            "Extraction schema profile. "
            "compact_single_call minimizes nesting while keeping broad coverage."
        ),
    )
    parser.add_argument(
        "--use-prefect",
        action="store_true",
        help="Run as a Prefect flow instead of the legacy sequential script",
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

    company_type = CompanyType(args.company_type)

    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_run_dir = output_root / f"run_{run_stamp}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path) if args.db_path else output_root / DEFAULT_DB_NAME
    ensure_parent(db_path)
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    extraction_types = extraction_types_for_schema_profile(args.schema_profile, company_type)

    run_id = insert_run(
        conn=conn,
        input_xlsx_path=str(input_xlsx),
        output_run_dir=str(output_run_dir),
        model=args.model,
        extraction_types=extraction_types,
    )
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
    model_candidates = deduplicate_ordered(
        [args.model] + parse_fallback_models(args.fallback_models)
    )

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
    print("[run {}] schema_profile={}".format(run_id, args.schema_profile))
    print("[run {}] company_type={}".format(run_id, company_type.value))

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

        try:
            profile = client.get_company_profile(company_number)
            stage = "profile_ok"
            filing_history_page = client.get_filing_history(
                company_number=company_number,
                items_per_page=args.filing_history_items_per_page,
                start_index=0,
            )
            filing_history = filing_history_page.get("items") or []
            stage = "filing_history_ok"
            document_id = latest_full_accounts_document_id(filing_history)
            if not document_id:
                raise ValueError("No full accounts document found")
            stage = "latest_document_ok"
            summary_row["document_id"] = document_id

            pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
            downloaded_path = client.download_document(
                document_id=document_id,
                output_path=str(pdf_path),
                accept="application/pdf",
            )
            stage = "download_ok"
            pdf_size_bytes = Path(downloaded_path).stat().st_size
            approx_llm_tokens = estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes)
            summary_row["pdf_path"] = str(pdf_path)
            summary_row["pdf_size_bytes"] = pdf_size_bytes
            summary_row["approx_llm_tokens"] = approx_llm_tokens

            extraction_schema_profile = args.schema_profile
            had_schema_depth_error = False
            run_extraction_types = extraction_types

            try:
                extraction_payload, warnings_payload, model_used = extract_with_model_fallback(
                    api_key=openrouter_api_key,
                    model_candidates=model_candidates,
                    document_path=downloaded_path,
                    extraction_types=run_extraction_types,
                    retries_on_invalid_json=args.retries_on_invalid_json,
                    company_type=company_type,
                )
            except DocumentExtractionError as exc:
                if (
                    args.schema_profile == "compact_single_call"
                    and is_schema_depth_error(exc)
                ):
                    had_schema_depth_error = True
                    extraction_schema_profile = "light_core"
                    run_extraction_types = extraction_types_for_schema_profile(
                        extraction_schema_profile, company_type
                    )
                    extraction_payload, warnings_payload, model_used = extract_with_model_fallback(
                        api_key=openrouter_api_key,
                        model_candidates=model_candidates,
                        document_path=downloaded_path,
                        extraction_types=run_extraction_types,
                        retries_on_invalid_json=args.retries_on_invalid_json,
                        company_type=company_type,
                    )
                else:
                    raise

            annual_report_key = (
                "academy_trust_annual_report"
                if company_type == CompanyType.ACADEMY_TRUST
                else "annual_report"
            )
            if extraction_payload.get(annual_report_key) is None:
                derived = derive_annual_report_from_component_sections(extraction_payload)
                if derived is not None:
                    extraction_payload[annual_report_key] = derived
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

            insert_company_row(
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
            print(f"{prefix} success document_id={document_id} model={model_used}")
        except Exception as exc:
            failed += 1
            error_message = "".join(
                traceback.format_exception(exc.__class__, exc, exc.__traceback__)
            )[:MAX_ERROR_TRACEBACK_CHARS]
            insert_company_row(
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
                }
            )
            print(f"{prefix} failed error={exc}")
        company_summaries.append(summary_row)

    finalize_run(
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
            "run_type": "batch_extract_companies",
            "company_type": company_type.value,
            "run_id": run_id,
            "timestamp_utc": utc_now_iso(),
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
        print(f"[run {run_id}] summary_json_path={summary_path}")

    print(
        f"[run {run_id}] complete processed={processed} succeeded={succeeded} failed={failed}"
    )
    return 0


def main_prefect() -> int:
    """Entry point that delegates to the Prefect flow."""
    _load_dotenv_file(Path(".env"))
    args = build_parser().parse_args()

    from flows.batch_extract import batch_extract_companies_flow

    result = batch_extract_companies_flow(
        input_xlsx=args.input_xlsx,
        output_root=args.output_root,
        model=args.model,
        max_companies=args.max_companies,
        start_index=args.start_index,
        schema_profile=args.schema_profile,
        company_type=args.company_type,
        db_path=args.db_path,
        fallback_models=args.fallback_models,
        write_summary_json=args.write_summary_json,
        summary_json_path=args.summary_json_path,
        filing_history_items_per_page=args.filing_history_items_per_page,
        retries_on_invalid_json=args.retries_on_invalid_json,
        random_sample_size=args.random_sample_size,
        random_seed=args.random_seed,
    )
    return 0 if result.get("failed", 0) == 0 else 1


if __name__ == "__main__":
    args = build_parser().parse_args()
    if args.use_prefect:
        raise SystemExit(main_prefect())
    raise SystemExit(main())
