"""Post-processing enrichment for incomplete personnel names using Gemini grounded search via DSPy."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from nameparser import HumanName

logger = logging.getLogger(__name__)

DEFAULT_ENRICHMENT_CACHE_DIR = "output/enrichment_cache"


def _is_incomplete_first_name(first_name: str) -> bool:
    """Return True if the first name looks like an initial (e.g. 'J', 'J.', 'AJ')."""
    stripped = re.sub(r"[.\s]", "", first_name)
    return len(stripped) <= 2 and stripped.isalpha()


def _cache_key(company_name: str, first_name: str, last_name: str, job_title: str) -> str:
    """Deterministic cache key from lookup inputs."""
    raw = f"{company_name.strip().lower()}|{first_name.strip().lower()}|{last_name.strip().lower()}|{job_title.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _read_cache(cache_dir: Path, key: str) -> dict[str, Any] | None:
    cache_file = cache_dir / f"{key}.json"
    if not cache_file.is_file():
        return None
    try:
        return json.loads(cache_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(cache_dir: Path, key: str, data: dict[str, Any]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{key}.json"
    cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def enrich_personnel_names(
    personnel: list[dict[str, Any]],
    company_name: str,
    gemini_api_key: str | None = None,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Enrich incomplete first names for trust-level personnel using Gemini grounded search.

    Only personnel with ``organisation_type == "trust"`` and short/initial first names
    are looked up.  School-level personnel and those with complete names are returned
    unchanged.  Results are cached to disk so repeat lookups are free.

    Args:
        personnel: List of personnel dicts (as produced by LLM extraction).
        company_name: The company/trust name used as search context.
        gemini_api_key: Google Gemini API key.  Falls back to ``GEMINI_API_KEY`` env var.
        cache_dir: Directory for enrichment cache files.  Defaults to
            ``output/enrichment_cache``.

    Returns:
        The full personnel list with enriched entries where applicable.
    """
    api_key = gemini_api_key or os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        logger.warning("No GEMINI_API_KEY provided — skipping name enrichment")
        return personnel

    cache_path = Path(cache_dir) if cache_dir else Path(DEFAULT_ENRICHMENT_CACHE_DIR)

    # Identify indices that need enrichment — any personnel with incomplete first names
    indices_to_enrich: list[int] = []
    for idx, person in enumerate(personnel):
        first_name = (person.get("first_name") or "").strip()
        if first_name and _is_incomplete_first_name(first_name):
            indices_to_enrich.append(idx)

    if not indices_to_enrich:
        logger.info("No incomplete names to enrich")
        return personnel

    logger.info(
        "Enriching %d incomplete name(s) for '%s'",
        len(indices_to_enrich),
        company_name,
    )

    # Check how many are already cached before importing dspy
    cache_hits = 0
    api_calls_needed = 0
    for idx in indices_to_enrich:
        person = personnel[idx]
        key = _cache_key(
            company_name,
            (person.get("first_name") or "").strip(),
            (person.get("last_name") or "").strip(),
            (person.get("job_title") or "").strip(),
        )
        cached = _read_cache(cache_path, key)
        if cached is not None:
            cache_hits += 1
        else:
            api_calls_needed += 1

    logger.info("Cache hits: %d, API calls needed: %d", cache_hits, api_calls_needed)

    # Only import and configure dspy if we actually need API calls
    predict = None
    if api_calls_needed > 0:
        try:
            import dspy
        except ImportError:
            logger.error("dspy is not installed — run: pip install 'dspy>=2.6.0'")
            return personnel

        lm = dspy.LM(
            model="gemini/gemini-flash-lite-latest",
            api_key=api_key,
        )
        dspy.configure(lm=lm)

        class PersonLookup(dspy.Signature):
            """Find the full first name and email of a person at a company."""

            company_name = dspy.InputField()
            first_name_initial = dspy.InputField()
            last_name = dspy.InputField()
            job_title = dspy.InputField()
            full_first_name: str = dspy.OutputField(
                desc="the person's full first name, or 'UNKNOWN' if not found"
            )
            email: str = dspy.OutputField(
                desc="the person's work email address, or 'UNKNOWN' if not found"
            )

        predict = dspy.Predict(PersonLookup, tools=[{"googleSearch": {}}])

    # --- Resolve all uncached lookups concurrently ---
    def _lookup(idx: int) -> tuple[int, str | None, str | None, str]:
        """Return (idx, full_first_name, email, source)."""
        person = personnel[idx]
        first_name = (person.get("first_name") or "").strip()
        last_name = (person.get("last_name") or "").strip()
        job_title = (person.get("job_title") or "").strip()

        key = _cache_key(company_name, first_name, last_name, job_title)
        cached = _read_cache(cache_path, key)

        if cached is not None:
            return idx, cached.get("full_first_name"), cached.get("email"), "cache"

        try:
            result = predict(
                company_name=company_name,
                first_name_initial=first_name,
                last_name=last_name,
                job_title=job_title,
            )
            full_first = getattr(result, "full_first_name", "UNKNOWN") or "UNKNOWN"
            email_val = getattr(result, "email", "UNKNOWN") or "UNKNOWN"

            if full_first.upper() == "UNKNOWN":
                full_first = None
            if email_val.upper() == "UNKNOWN":
                email_val = None

            _write_cache(cache_path, key, {
                "company_name": company_name,
                "first_name_initial": first_name,
                "last_name": last_name,
                "job_title": job_title,
                "full_first_name": full_first,
                "email": email_val,
            })
            return idx, full_first, email_val, "api"
        except Exception:
            logger.warning(
                "Failed to enrich '%s %s' — keeping original",
                first_name,
                last_name,
                exc_info=True,
            )
            return idx, None, None, "error"

    from concurrent.futures import ThreadPoolExecutor, as_completed

    max_workers = 10
    results: dict[int, tuple[str | None, str | None, str]] = {}
    total = len(indices_to_enrich)
    done_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_lookup, idx): idx for idx in indices_to_enrich}
        for future in as_completed(futures):
            idx, full_first, email_val, source = future.result()
            results[idx] = (full_first, email_val, source)
            done_count += 1
            person = personnel[idx]
            fn = (person.get("first_name") or "").strip()
            ln = (person.get("last_name") or "").strip()
            resolved = full_first or "—"
            logger.info(
                "[%d/%d] %s %s -> %s (%s)",
                done_count, total, fn, ln, resolved, source,
            )

    # --- Apply results back to personnel list ---
    for idx in indices_to_enrich:
        person = personnel[idx]
        first_name = (person.get("first_name") or "").strip()

        # Preserve the original LLM-extracted name
        person["first_name_extracted"] = first_name

        full_first, email_val, source = results[idx]

        if source == "error":
            person["first_name_enriched"] = None
            continue

        if full_first and len(full_first) > len(re.sub(r"[.\s]", "", first_name)):
            # Parse the enriched name to separate first from middle names
            parsed = HumanName(f"{full_first} {person.get('last_name', '')}")
            parsed_first = str(parsed.first).strip()
            parsed_middle = str(parsed.middle).strip()

            logger.info(
                "Resolved '%s %s' -> '%s %s' [%s]",
                first_name,
                person.get("last_name", ""),
                parsed_first,
                person.get("last_name", ""),
                source,
            )
            person["first_name_enriched"] = parsed_first
            person["first_name"] = parsed_first
            if parsed_middle:
                person["middle_names"] = parsed_middle
        else:
            person["first_name_enriched"] = None

        if email_val:
            person["email"] = email_val
        else:
            person.setdefault("email", None)

    return personnel
