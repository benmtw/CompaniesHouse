"""Post-processing enrichment for incomplete personnel names using Gemini grounded search via DSPy."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)


def _is_incomplete_first_name(first_name: str) -> bool:
    """Return True if the first name looks like an initial (e.g. 'J', 'J.', 'AJ')."""
    stripped = re.sub(r"[.\s]", "", first_name)
    return len(stripped) <= 2 and stripped.isalpha()


def enrich_personnel_names(
    personnel: list[dict[str, Any]],
    company_name: str,
    gemini_api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Enrich incomplete first names for trust-level personnel using Gemini grounded search.

    Only personnel with ``organisation_type == "trust"`` and short/initial first names
    are looked up.  School-level personnel and those with complete names are returned
    unchanged.

    Args:
        personnel: List of personnel dicts (as produced by LLM extraction).
        company_name: The company/trust name used as search context.
        gemini_api_key: Google Gemini API key.  Falls back to ``GEMINI_API_KEY`` env var.

    Returns:
        The full personnel list with enriched entries where applicable.
    """
    api_key = gemini_api_key or os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        logger.warning("No GEMINI_API_KEY provided — skipping name enrichment")
        return personnel

    # Identify indices that need enrichment
    indices_to_enrich: list[int] = []
    for idx, person in enumerate(personnel):
        org_type = (person.get("organisation_type") or "").strip().lower()
        if org_type != "trust":
            continue
        first_name = (person.get("first_name") or "").strip()
        if first_name and _is_incomplete_first_name(first_name):
            indices_to_enrich.append(idx)

    if not indices_to_enrich:
        logger.info("No incomplete trust-level names to enrich")
        return personnel

    logger.info(
        "Enriching %d incomplete name(s) for '%s'",
        len(indices_to_enrich),
        company_name,
    )

    try:
        import dspy
    except ImportError:
        logger.error("dspy is not installed — run: pip install 'dspy>=2.6.0'")
        return personnel

    # Configure DSPy with Gemini + Google Search grounding
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

    for idx in indices_to_enrich:
        person = personnel[idx]
        first_name = (person.get("first_name") or "").strip()
        last_name = (person.get("last_name") or "").strip()
        job_title = (person.get("job_title") or "").strip()

        # Preserve the original LLM-extracted name
        person["first_name_extracted"] = first_name

        try:
            result = predict(
                company_name=company_name,
                first_name_initial=first_name,
                last_name=last_name,
                job_title=job_title,
            )

            full_first = getattr(result, "full_first_name", "UNKNOWN") or "UNKNOWN"
            email = getattr(result, "email", "UNKNOWN") or "UNKNOWN"

            if full_first.upper() != "UNKNOWN" and len(full_first) > len(first_name):
                logger.info(
                    "Resolved '%s %s' -> '%s %s'",
                    first_name,
                    last_name,
                    full_first,
                    last_name,
                )
                person["first_name_enriched"] = full_first
                person["first_name"] = full_first
            else:
                person["first_name_enriched"] = None

            if email.upper() != "UNKNOWN":
                person["email"] = email
            else:
                person.setdefault("email", None)

        except Exception:
            logger.warning(
                "Failed to enrich '%s %s' — keeping original",
                first_name,
                last_name,
                exc_info=True,
            )
            person["first_name_enriched"] = None

    return personnel
