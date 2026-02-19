import argparse
import json
import os
import random
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from batch_extract_trusts import (
    _install_companies_house_request_throttle,
    _latest_full_accounts_document_id_from_filing_history,
    _load_dotenv_file,
    normalize_company_number,
    read_xlsx_rows,
)
from companies_house_client import CompaniesHouseClient


DEFAULT_INPUT_XLSX = "SourceData/allgroupslinksdata20260217/Trusts.xlsx"
DEFAULT_OUTPUT_ROOT = "output/trusts_documents_only"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _utc_now_precise() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download latest Companies House full-accounts PDFs for trusts listed "
            "in an XLSX file (no OpenRouter extraction)."
        )
    )
    parser.add_argument(
        "--input-xlsx",
        default=DEFAULT_INPUT_XLSX,
        help="Path to Trusts.xlsx file",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Root output directory for downloaded PDFs and run artifacts",
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
        "--random-sample-size",
        type=int,
        default=0,
        help="Randomly sample this many companies from selected batch (0 disables)",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=0,
        help="Optional seed for random sampling (0 uses non-deterministic sampling)",
    )
    parser.add_argument(
        "--filing-history-items-per-page",
        type=int,
        default=100,
        help="Filing history page size used for latest full-accounts selection",
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
        help="Write run-level summary.json in the run output folder",
    )
    return parser


def main() -> int:
    _load_dotenv_file(Path(".env"))
    args = _build_parser().parse_args()

    if args.start_index < 0:
        raise ValueError("start_index must be >= 0")
    if args.max_companies < 0:
        raise ValueError("max_companies must be >= 0")
    if args.random_sample_size < 0:
        raise ValueError("random_sample_size must be >= 0")
    if args.filing_history_items_per_page <= 0:
        raise ValueError("filing_history_items_per_page must be > 0")
    if args.ch_min_request_interval_seconds < 0:
        raise ValueError("ch_min_request_interval_seconds must be >= 0")

    input_xlsx = Path(args.input_xlsx)
    if not input_xlsx.exists() or not input_xlsx.is_file():
        raise FileNotFoundError(f"Input xlsx file not found: {input_xlsx}")
    if input_xlsx.suffix.lower() != ".xlsx":
        raise ValueError("Only .xlsx is supported by this downloader")

    ch_api_key = os.getenv("CH_API_KEY")
    if not ch_api_key:
        raise ValueError("Missing CH_API_KEY. Set in environment or .env.")

    rows = read_xlsx_rows(input_xlsx)
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, row in enumerate(rows):
        company_number = normalize_company_number(row.get("Companies House Number", ""))
        if not company_number or company_number in seen:
            continue
        seen.add(company_number)
        deduped.append(
            {
                "source_row_index": idx + 2,
                "company_number": company_number,
                "group_id": row.get("Group ID", ""),
                "group_name": row.get("Group Name", ""),
            }
        )

    selected = deduped[args.start_index :]
    if args.max_companies > 0:
        selected = selected[: args.max_companies]
    if args.random_sample_size > 0:
        if args.random_seed:
            random.seed(args.random_seed)
        sample_size = min(args.random_sample_size, len(selected))
        selected = random.sample(selected, sample_size)

    output_root = Path(args.output_root)
    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = output_root / f"run_{run_stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    events_path = run_dir / "events.jsonl"

    client = CompaniesHouseClient(api_key=ch_api_key)
    throttle_state = _install_companies_house_request_throttle(
        client=client,
        min_interval_seconds=args.ch_min_request_interval_seconds,
    )

    total = len(selected)
    print(f"[run {run_stamp}] selected_companies={total}")
    print(
        f"[run {run_stamp}] ch_min_request_interval_seconds="
        f"{args.ch_min_request_interval_seconds}"
    )
    print(f"[run {run_stamp}] output_dir={run_dir}")

    summary_rows: list[dict[str, Any]] = []
    counters = {"success": 0, "failed": 0}

    for index, item in enumerate(selected, start=1):
        company_number = item["company_number"]
        company_dir = run_dir / company_number
        docs_dir = company_dir / "documents"
        api_dir = company_dir / "api"
        docs_dir.mkdir(parents=True, exist_ok=True)
        api_dir.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()

        _append_jsonl(
            events_path,
            {
                "ts_utc": _utc_now_precise(),
                "event": "company_start",
                "index": index,
                "total": total,
                "company_number": company_number,
                "source_row_index": item["source_row_index"],
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
            },
        )

        row_summary: dict[str, Any] = {
            "company_number": company_number,
            "source_row_index": item["source_row_index"],
            "group_id": item.get("group_id"),
            "group_name": item.get("group_name"),
            "status": "failed",
            "company_name": None,
            "document_id": None,
            "pdf_path": None,
            "cache_hit": None,
            "error": None,
        }

        try:
            profile = client.get_company_profile(company_number)
            row_summary["company_name"] = profile.get("company_name")
            _write_json(api_dir / "profile.json", profile)

            filing_page = client.get_filing_history(
                company_number=company_number,
                items_per_page=args.filing_history_items_per_page,
                start_index=0,
            )
            filing_items = filing_page.get("items") or []
            _write_json(api_dir / "filing_history.json", filing_items)

            document_id = _latest_full_accounts_document_id_from_filing_history(filing_items)
            if not document_id:
                raise ValueError("No full accounts document found")

            pdf_path = docs_dir / f"{company_number}_latest_full_accounts_{document_id}.pdf"
            client.download_document(
                document_id=document_id,
                output_path=str(pdf_path),
                accept="application/pdf",
                company_number=company_number,
            )

            row_summary["document_id"] = document_id
            row_summary["pdf_path"] = str(pdf_path)
            row_summary["cache_hit"] = client.last_download_cache_hit
            row_summary["status"] = "success"
            counters["success"] += 1
        except Exception as exc:
            row_summary["error"] = str(exc)
            counters["failed"] += 1

        row_summary["duration_seconds"] = round(time.monotonic() - started, 3)
        summary_rows.append(row_summary)
        _append_jsonl(
            events_path,
            {
                "ts_utc": _utc_now_precise(),
                "event": "company_end",
                "index": index,
                "total": total,
                "company_number": company_number,
                "status": row_summary["status"],
                "cache_hit": row_summary["cache_hit"],
                "document_id": row_summary["document_id"],
                "duration_seconds": row_summary["duration_seconds"],
                "error": row_summary["error"],
            },
        )
        print(
            f"[{index}/{total}] {company_number} {row_summary['status']}"
            + (
                f" doc={row_summary['document_id']} cache_hit={row_summary['cache_hit']}"
                if row_summary["status"] == "success"
                else f" error={row_summary['error']}"
            )
        )

    summary = {
        "started_at": _utc_now(),
        "input_xlsx": str(input_xlsx),
        "run_dir": str(run_dir),
        "total": total,
        "success": counters["success"],
        "failed": counters["failed"],
        "ch_request_throttle": throttle_state,
        "results": summary_rows,
    }

    if args.write_summary_json:
        _write_json(run_dir / "summary.json", summary)
    print(
        f"[run {run_stamp}] complete total={total} success={counters['success']} "
        f"failed={counters['failed']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
