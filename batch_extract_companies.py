import argparse
import json
import logging
import os
import queue
import random
import shutil
import sqlite3
import threading
import time
import traceback
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZipFile

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log = logging.getLogger("batch_extract")
log.setLevel(logging.DEBUG)

# File handler — always writes to logs/batch_extract.log
_fh = logging.FileHandler(LOG_DIR / "batch_extract.log", encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
log.addHandler(_fh)

# Console handler — mirrors to stdout
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_ch)

from companies_house_client import CompaniesHouseClient
from company_type import CompanyType
from name_enrichment import enrich_personnel_names
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

        # Handle both relative (worksheets/sheet1.xml) and absolute (/xl/worksheets/sheet1.xml) targets
        target = target.lstrip("/")
        if target.startswith("xl/"):
            target = target[3:]
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
        "--mode",
        choices=["extract", "personnel"],
        default="extract",
        help=(
            "Pipeline mode. 'extract' (default) downloads filings and runs LLM extraction. "
            "'personnel' fetches current officers from the Companies House API only (no OpenRouter needed)."
        ),
    )
    parser.add_argument(
        "--company-type",
        choices=[ct.value for ct in CompanyType],
        default=CompanyType.ACADEMY_TRUST.value,
        help=(
            "Type of company being processed. Controls LLM prompt text and "
            "extraction model fields (default: academy_trust)"
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
        choices=["compact_single_call", "full_legacy", "light_core", "personnel_only"],
        default=os.getenv("BATCH_SCHEMA_PROFILE", "personnel_only"),
        help=(
            "Extraction schema profile. "
            "compact_single_call minimizes nesting while keeping broad coverage."
        ),
    )
    parser.add_argument(
        "--personnel-cache-dir",
        default="output/personnel_cache",
        help="Directory for cached personnel lookups (default: %(default)s)",
    )
    parser.add_argument(
        "--personnel-cache-ttl-days",
        type=float,
        default=7.0,
        help="Days before a cached personnel lookup is considered stale (default: %(default)s, 0 to disable cache)",
    )
    parser.add_argument(
        "--extraction-workers",
        type=int,
        default=5,
        help="Number of concurrent LLM extraction threads (default: 5)",
    )
    parser.add_argument(
        "--no-name-enrichment",
        action="store_true",
        help="Disable Gemini-based name enrichment for incomplete trust personnel names",
    )
    parser.add_argument(
        "--use-prefect",
        action="store_true",
        help="Run as a Prefect flow instead of the legacy sequential script",
    )
    return parser


def _read_personnel_cache(
    cache_dir: Path, company_number: str, ttl_days: float
) -> list[dict[str, Any]] | None:
    """Return cached officers list if the cache file exists and is within TTL, else None."""
    if ttl_days <= 0:
        return None
    cache_file = cache_dir / f"{company_number}.json"
    if not cache_file.is_file():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    fetched_at = data.get("fetched_at")
    if not fetched_at:
        return None
    try:
        fetched_dt = datetime.fromisoformat(fetched_at)
    except ValueError:
        return None
    age_days = (datetime.now(UTC) - fetched_dt).total_seconds() / 86400
    if age_days > ttl_days:
        return None
    return data.get("officers")


def _write_personnel_cache(
    cache_dir: Path, company_number: str, officers: list[dict[str, Any]]
) -> None:
    """Write officers list to the cache directory with a fetched_at timestamp."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{company_number}.json"
    payload = {
        "company_number": company_number,
        "fetched_at": datetime.now(UTC).isoformat(),
        "officers": officers,
    }
    write_json(cache_file, payload)


def _run_personnel_mode(args: argparse.Namespace, input_xlsx: Path, ch_api_key: str) -> int:
    """Fetch current officers for each company and write JSON output. No OpenRouter needed."""
    if args.ch_min_request_interval_seconds < 0:
        raise ValueError("ch_min_request_interval_seconds must be >= 0")

    cache_dir = Path(args.personnel_cache_dir)
    ttl_days = args.personnel_cache_ttl_days

    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_run_dir = output_root / f"run_{run_stamp}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    rows = read_xlsx_rows(input_xlsx)
    seen: set[str] = set()
    batch: list[dict[str, Any]] = []
    for source_index, row in enumerate(rows, start=2):
        normalized = normalize_company_number(row.get("Companies House Number", ""))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        batch.append({
            "source_row_index": source_index,
            "group_uid": row.get("Group UID"),
            "group_id": row.get("Group ID"),
            "group_name": row.get("Group Name"),
            "company_number": normalized,
        })

    if args.start_index > 0:
        batch = batch[args.start_index:]
    if args.max_companies > 0:
        batch = batch[:args.max_companies]
    if args.random_sample_size > 0 and len(batch) > args.random_sample_size:
        rng = random.Random(None if args.random_seed == 0 else args.random_seed)
        batch = rng.sample(batch, args.random_sample_size)

    total = len(batch)
    client = CompaniesHouseClient(api_key=ch_api_key)
    _install_companies_house_request_throttle(
        client=client,
        min_interval_seconds=args.ch_min_request_interval_seconds,
    )

    log.info(f"[personnel] output_dir={output_run_dir}")
    log.info(f"[personnel] cache_dir={cache_dir}")
    log.info(f"[personnel] cache_ttl_days={ttl_days}")
    log.info(f"[personnel] companies_to_process={total}")

    succeeded = 0
    failed = 0
    cache_hits = 0
    all_results: list[dict[str, Any]] = []

    for idx, item in enumerate(batch, start=1):
        company_number = item["company_number"]
        prefix = f"[personnel] [{idx}/{total}] {company_number}"
        try:
            cached = _read_personnel_cache(cache_dir, company_number, ttl_days)
            if cached is not None:
                officers = cached
                source = "cache"
                cache_hits += 1
            else:
                officers = client.get_current_officers(company_number)
                _write_personnel_cache(cache_dir, company_number, officers)
                source = "api"

            result = {
                "company_number": company_number,
                "group_uid": item.get("group_uid"),
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "status": "success",
                "source": source,
                "officers": officers,
            }
            company_dir = output_run_dir / company_number
            company_dir.mkdir(parents=True, exist_ok=True)
            officers_path = company_dir / "officers.json"
            write_json(officers_path, officers)
            succeeded += 1
            log.info(f"{prefix} success officers={len(officers)} source={source}")
        except Exception as exc:
            result = {
                "company_number": company_number,
                "group_uid": item.get("group_uid"),
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "status": "failed",
                "officers": [],
                "error": str(exc),
            }
            failed += 1
            log.error(f"{prefix} failed error={exc}")
        all_results.append(result)

    summary_path = output_run_dir / "personnel_summary.json"
    write_json(summary_path, {
        "run_type": "personnel",
        "timestamp_utc": utc_now_iso(),
        "input_xlsx_path": str(input_xlsx),
        "output_run_dir": str(output_run_dir),
        "cache_dir": str(cache_dir),
        "cache_ttl_days": ttl_days,
        "total_companies": total,
        "succeeded": succeeded,
        "failed": failed,
        "cache_hits": cache_hits,
        "api_calls": succeeded - cache_hits,
        "companies": all_results,
    })

    log.info(f"[personnel] complete processed={total} succeeded={succeeded} failed={failed} cache_hits={cache_hits}")
    log.info(f"[personnel] summary={summary_path}")
    return 0


def main() -> int:
    _load_dotenv_file(Path(".env"))
    args = build_parser().parse_args()

    input_xlsx = Path(args.input_xlsx)
    if not input_xlsx.exists() or not input_xlsx.is_file():
        raise FileNotFoundError(f"Input xlsx file not found: {input_xlsx}")

    ch_api_key = os.getenv("CH_API_KEY")
    if not ch_api_key:
        raise ValueError("Missing CH_API_KEY. Set in environment or .env.")

    if args.mode == "personnel":
        return _run_personnel_mode(args, input_xlsx, ch_api_key)

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

    if not args.no_name_enrichment and company_type != CompanyType.ACADEMY_TRUST:
        log.warning(
            "Name enrichment is enabled but --company-type is not 'academy_trust'. "
            "The academy_trust prompts extract organisation_type which enrichment uses. "
            "Consider using --company-type academy_trust or --no-name-enrichment."
        )

    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_run_dir = output_root / f"run_{run_stamp}"
    output_run_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db_path) if args.db_path else output_root / DEFAULT_DB_NAME
    ensure_parent(db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    create_tables(conn)

    extraction_types = extraction_types_for_schema_profile(args.schema_profile, company_type)

    run_id = insert_run(
        conn=conn,
        input_xlsx_path=str(input_xlsx),
        output_run_dir=str(output_run_dir),
        model=args.model,
        extraction_types=extraction_types,
    )
    log.info(f"[run {run_id}] output_dir={output_run_dir}")
    log.info(f"[run {run_id}] db_path={db_path}")

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

    log.info(f"[run {run_id}] companies_to_process={total_companies}")
    log.info(f"[run {run_id}] models={model_candidates}")
    log.info(f"[run {run_id}] ch_min_request_interval_seconds={args.ch_min_request_interval_seconds}")
    log.info(f"[run {run_id}] filing_history_items_per_page={args.filing_history_items_per_page}")
    log.info(f"[run {run_id}] retries_on_invalid_json={args.retries_on_invalid_json}")
    log.info(f"[run {run_id}] schema_profile={args.schema_profile}")
    log.info(f"[run {run_id}] company_type={company_type.value}")

    if args.extraction_workers < 1:
        raise ValueError("extraction-workers must be >= 1")

    client = CompaniesHouseClient(api_key=ch_api_key)
    throttle_state = _install_companies_house_request_throttle(
        client=client,
        min_interval_seconds=args.ch_min_request_interval_seconds,
    )

    extraction_workers = args.extraction_workers
    log.info(f"[run {run_id}] extraction_workers={extraction_workers}")

    # Pre-configure DSPy on the main thread so worker threads can use it
    if not args.no_name_enrichment:
        gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
        if gemini_key:
            from name_enrichment import configure_dspy

            try:
                configure_dspy(gemini_key)
            except Exception as exc:
                log.warning(f"[run {run_id}] failed to configure name enrichment: {exc}")

    # -- shared mutable state protected by locks --
    counters_lock = threading.Lock()
    db_lock = threading.Lock()
    processed = 0
    succeeded = 0
    failed = 0
    company_summaries: list[dict[str, Any]] = []

    download_queue: queue.Queue[tuple[int, dict[str, Any], dict[str, Any]] | None] = (
        queue.Queue(maxsize=extraction_workers * 2)
    )

    def _download_company(
        item: dict[str, Any],
        seq_num: int,
    ) -> dict[str, Any]:
        """Run CH API calls + PDF download for one company. Returns result dict."""
        company_number = item["company_number"]
        prefix = f"[run {run_id}] [{seq_num}/{total_companies}] {company_number}"
        log.info(f"{prefix} download start")

        company_dir = output_run_dir / company_number
        api_dir = company_dir / "api"
        doc_dir = company_dir / "documents"
        api_dir.mkdir(parents=True, exist_ok=True)
        doc_dir.mkdir(parents=True, exist_ok=True)

        result: dict[str, Any] = {"stage": "start", "ok": False}

        try:
            # Check previous runs for cached profile and filing history
            cached_profile = None
            cached_filing_history = None
            for prev_run_dir in sorted(Path(output_root).glob("run_*"), reverse=True):
                if prev_run_dir == output_run_dir:
                    continue
                prev_api_dir = prev_run_dir / company_number / "api"
                prev_profile = prev_api_dir / "profile.json"
                prev_fh = prev_api_dir / "filing_history.json"
                if prev_profile.exists() and prev_fh.exists():
                    try:
                        cached_profile = json.loads(prev_profile.read_text(encoding="utf-8"))
                        cached_filing_history = json.loads(prev_fh.read_text(encoding="utf-8"))
                        log.debug(f"{prefix} using cached profile+filing_history from: {prev_run_dir.name}")
                        break
                    except (json.JSONDecodeError, OSError):
                        cached_profile = None
                        cached_filing_history = None

            if cached_profile is not None:
                profile = cached_profile
            else:
                profile = client.get_company_profile(company_number)
            result["profile"] = profile
            result["stage"] = "profile_ok"

            if cached_filing_history is not None:
                filing_history = cached_filing_history
            else:
                filing_history_page = client.get_filing_history(
                    company_number=company_number,
                    items_per_page=args.filing_history_items_per_page,
                    start_index=0,
                )
                filing_history = filing_history_page.get("items") or []
            result["filing_history"] = filing_history
            result["stage"] = "filing_history_ok"

            document_id = latest_full_accounts_document_id(filing_history)
            if not document_id:
                raise ValueError("No full accounts document found")
            result["document_id"] = document_id
            result["stage"] = "latest_document_ok"

            pdf_path = doc_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"

            # Check for existing PDF in any previous run for this document_id
            existing_pdf = None
            for prev_run_dir in sorted(Path(output_root).glob("run_*"), reverse=True):
                if prev_run_dir == output_run_dir:
                    continue
                candidate = (
                    prev_run_dir
                    / company_number
                    / "documents"
                    / f"{company_number}_latest_full_accounts_{document_id}.pdf"
                )
                if candidate.exists():
                    existing_pdf = candidate
                    break

            if existing_pdf:
                doc_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(existing_pdf, pdf_path)
                downloaded_path = str(pdf_path)
                log.debug(f"{prefix} using cached PDF from: {existing_pdf}")
            else:
                downloaded_path = client.download_document(
                    document_id=document_id,
                    output_path=str(pdf_path),
                    accept="application/pdf",
                )

            result["stage"] = "download_ok"
            pdf_size_bytes = Path(downloaded_path).stat().st_size
            approx_llm_tokens = estimate_llm_tokens_for_pdf_bytes(pdf_size_bytes)
            result["pdf_path"] = str(pdf_path)
            result["downloaded_path"] = downloaded_path
            result["pdf_size_bytes"] = pdf_size_bytes
            result["approx_llm_tokens"] = approx_llm_tokens
            result["api_dir"] = str(api_dir)
            result["ok"] = True
            log.info(f"{prefix} download ok document_id={document_id}")
        except Exception as exc:
            result["error"] = exc
            log.error(f"{prefix} download failed stage={result['stage']} error={exc}")

        return result

    def _extract_company(
        item: dict[str, Any],
        seq_num: int,
        dl: dict[str, Any],
    ) -> None:
        """Run LLM extraction + persistence for one company."""
        nonlocal processed, succeeded, failed

        company_number = item["company_number"]
        prefix = f"[run {run_id}] [{seq_num}/{total_companies}] {company_number}"

        company_dir = output_run_dir / company_number
        api_dir = company_dir / "api"
        extraction_dir = company_dir / "extraction"
        extraction_dir.mkdir(parents=True, exist_ok=True)

        stage = dl["stage"]
        summary_row: dict[str, Any] = {
            "source_row_index": item["source_row_index"],
            "group_uid": item.get("group_uid"),
            "group_id": item.get("group_id"),
            "group_name": item.get("group_name"),
            "company_number": company_number,
            "status": "failed",
            "companies_house_stage": stage,
            "document_id": dl.get("document_id"),
            "pdf_path": dl.get("pdf_path"),
            "pdf_size_bytes": dl.get("pdf_size_bytes"),
            "approx_llm_tokens": dl.get("approx_llm_tokens"),
            "model_used": None,
            "error": None,
        }

        try:
            if not dl["ok"]:
                raise dl["error"]

            downloaded_path = dl["downloaded_path"]
            document_id = dl["document_id"]
            profile = dl["profile"]
            filing_history = dl["filing_history"]
            pdf_path = dl["pdf_path"]
            pdf_size_bytes = dl["pdf_size_bytes"]
            approx_llm_tokens = dl["approx_llm_tokens"]

            extraction_schema_profile = args.schema_profile
            had_schema_depth_error = False
            run_extraction_types = extraction_types

            try:
                extraction_payload, warnings_payload, model_used = (
                    extract_with_model_fallback(
                        api_key=openrouter_api_key,
                        model_candidates=model_candidates,
                        document_path=downloaded_path,
                        extraction_types=run_extraction_types,
                        retries_on_invalid_json=args.retries_on_invalid_json,
                        company_type=company_type,
                    )
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
                    extraction_payload, warnings_payload, model_used = (
                        extract_with_model_fallback(
                            api_key=openrouter_api_key,
                            model_candidates=model_candidates,
                            document_path=downloaded_path,
                            extraction_types=run_extraction_types,
                            retries_on_invalid_json=args.retries_on_invalid_json,
                            company_type=company_type,
                        )
                    )
                else:
                    raise

            annual_report_key = (
                "academy_trust_annual_report"
                if company_type == CompanyType.ACADEMY_TRUST
                else "annual_report"
            )
            if extraction_payload.get(annual_report_key) is None:
                derived = derive_annual_report_from_component_sections(
                    extraction_payload
                )
                if derived is not None:
                    extraction_payload[annual_report_key] = derived

            if not args.no_name_enrichment:
                pd_list = extraction_payload.get("personnel_details")
                if pd_list:
                    extraction_payload["personnel_details"] = enrich_personnel_names(
                        personnel=pd_list,
                        company_name=profile.get("company_name", ""),
                    )

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
                "pdf_path": pdf_path,
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

            with db_lock:
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
                        "pdf_path": pdf_path,
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

            with counters_lock:
                succeeded += 1

            summary_row.update(
                {
                    "status": "success",
                    "companies_house_stage": stage,
                    "company_name": profile.get("company_name"),
                    "document_id": document_id,
                    "pdf_path": pdf_path,
                    "pdf_size_bytes": pdf_size_bytes,
                    "approx_llm_tokens": approx_llm_tokens,
                    "model_used": model_used,
                    "schema_profile": extraction_schema_profile,
                    "schema_profile_fallback_applied": had_schema_depth_error,
                    "error": None,
                }
            )
            log.info(f"{prefix} extraction success document_id={document_id} model={model_used}")
        except Exception as exc:
            error_message = "".join(
                traceback.format_exception(exc.__class__, exc, exc.__traceback__)
            )[:MAX_ERROR_TRACEBACK_CHARS]
            with db_lock:
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

            with counters_lock:
                failed += 1

            summary_row.update(
                {
                    "status": "failed",
                    "companies_house_stage": stage,
                    "error": str(exc),
                }
            )
            log.error(f"{prefix} extraction failed error={exc}")

        with counters_lock:
            processed += 1
            company_summaries.append(summary_row)

    # -- Producer: single thread doing CH API downloads --
    def producer() -> None:
        for seq_num, item in enumerate(batch, start=1):
            dl_result = _download_company(item, seq_num)
            download_queue.put((seq_num, item, dl_result))
        # Send sentinel for each consumer
        for _ in range(extraction_workers):
            download_queue.put(None)

    # -- Consumer: extraction worker pulling from queue --
    def consumer() -> None:
        while True:
            msg = download_queue.get()
            if msg is None:
                break
            seq_num, item, dl_result = msg
            _extract_company(item, seq_num, dl_result)

    producer_thread = threading.Thread(target=producer, name="ch-download-producer")
    producer_thread.start()

    with ThreadPoolExecutor(
        max_workers=extraction_workers,
        thread_name_prefix="llm-extract",
    ) as pool:
        futures = [pool.submit(consumer) for _ in range(extraction_workers)]
        # Wait for all consumers to finish
        for f in futures:
            f.result()

    producer_thread.join()

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
            "extraction_workers": extraction_workers,
            "total_companies": total_companies,
            "processed": processed,
            "succeeded": succeeded,
            "failed": failed,
            "companies": company_summaries,
        }
        write_json(summary_path, summary_payload)
        log.info(f"[run {run_id}] summary_json_path={summary_path}")

    log.info(f"[run {run_id}] complete processed={processed} succeeded={succeeded} failed={failed}")
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
