import base64
import json
import os
import queue
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from companies_house_full_reports_extraction_pipeline import (
    GlobalCHThrottle,
    _create_tables,
    _enqueue_with_backpressure,
    _insert_job,
    _insert_run,
    _install_global_throttle_on_client,
    _parse_company_numbers_csv,
    _update_download_state,
    _update_extract_state,
    _update_final_state,
    _validate_input_xor,
    _validate_worker_settings,
)
from companies_house_client import (
    AcademyTrustAnnualReport,
    CompaniesHouseApiError,
    CompaniesHouseClient,
    DetailedBalanceSheet,
    DocumentExtractionError,
    ExtractionResult,
    ExtractionType,
    FilingDocumentType,
    Metadata,
    OpenRouterDocumentExtractor,
    PersonnelDetail,
)


class DummyResponse:
    def __init__(
        self,
        ok=True,
        status_code=200,
        json_data=None,
        text="",
        chunks=None,
        headers=None,
    ):
        self.ok = ok
        self.status_code = status_code
        self._json_data = {} if json_data is None else json_data
        self.text = text
        self._chunks = chunks or []
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def iter_content(self, chunk_size=8192):
        del chunk_size
        for c in self._chunks:
            yield c


class CompaniesHouseClientTests(unittest.TestCase):
    def test_constructor_uses_explicit_key(self):
        client = CompaniesHouseClient(api_key="abc")
        expected = base64.b64encode(b"abc:").decode("ascii")
        self.assertEqual(client._headers["Authorization"], f"Basic {expected}")

    def test_constructor_uses_env_key(self):
        with patch.dict(os.environ, {"CH_API_KEY": "env_key"}, clear=False):
            client = CompaniesHouseClient()
        expected = base64.b64encode(b"env_key:").decode("ascii")
        self.assertEqual(client._headers["Authorization"], f"Basic {expected}")

    def test_constructor_missing_key_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                CompaniesHouseClient()

    def test_search_companies_sends_expected_params(self):
        client = CompaniesHouseClient(api_key="k")
        mock_response = DummyResponse(json_data={"items": []})
        client.session.request = Mock(return_value=mock_response)

        client.search_companies("tesco", items_per_page=5, start_index=10)

        _, kwargs = client.session.request.call_args
        self.assertEqual(kwargs["url"], "https://api.company-information.service.gov.uk/search/companies")
        self.assertEqual(kwargs["params"]["q"], "tesco")
        self.assertEqual(kwargs["params"]["items_per_page"], 5)
        self.assertEqual(kwargs["params"]["start_index"], 10)
        self.assertIn("Authorization", kwargs["headers"])

    def test_get_all_filing_history_paginates(self):
        client = CompaniesHouseClient(api_key="k")
        page1 = {"total_count": 3, "items": [{"id": 1}, {"id": 2}]}
        page2 = {"total_count": 3, "items": [{"id": 3}]}
        client.get_filing_history = Mock(side_effect=[page1, page2])

        items = client.get_all_filing_history("09618502", page_size=2)

        self.assertEqual(len(items), 3)
        self.assertEqual(items[0]["id"], 1)
        self.assertEqual(items[2]["id"], 3)
        self.assertEqual(client.get_filing_history.call_count, 2)

    def test_list_filing_documents_normalizes_metadata(self):
        client = CompaniesHouseClient(api_key="k")
        client.get_all_filing_history = Mock(
            return_value=[
                {
                    "date": "2020-01-01",
                    "type": "AA",
                    "description": "accounts-with-accounts-type-full",
                    "links": {
                        "document_metadata": "https://document-api.company-information.service.gov.uk/document/abc123"
                    },
                },
                {"date": "2020-01-02", "type": "CS01", "description": "confirmation"},
            ]
        )
        client._request_json = Mock(
            return_value={
                "resources": {"application/pdf": {}, "application/xhtml+xml": {}}
            }
        )

        docs = client.list_filing_documents("09618502")

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["document_id"], "abc123")
        self.assertEqual(
            docs[0]["content_types"], ["application/pdf", "application/xhtml+xml"]
        )
        self.assertIn("Full accounts", docs[0]["friendly_types"])

    def test_get_latest_document_without_filter(self):
        client = CompaniesHouseClient(api_key="k")
        client.list_filing_documents = Mock(
            return_value=[
                {
                    "document_id": "older",
                    "date": "2024-01-01",
                    "friendly_types": ["Confirmation statement"],
                },
                {
                    "document_id": "newer",
                    "date": "2025-01-01",
                    "friendly_types": ["Full accounts"],
                },
            ]
        )

        latest = client.get_latest_document("11124272")
        self.assertEqual(latest["document_id"], "newer")

    def test_get_latest_document_with_friendly_filter(self):
        client = CompaniesHouseClient(api_key="k")
        client.list_filing_documents = Mock(
            return_value=[
                {
                    "document_id": "cs01-doc",
                    "date": "2025-01-01",
                    "friendly_types": ["Confirmation statement"],
                },
                {
                    "document_id": "aa-doc",
                    "date": "2024-01-01",
                    "friendly_types": ["Full accounts"],
                },
            ]
        )

        latest_full = client.get_latest_document(
            "11124272", FilingDocumentType.FULL_ACCOUNTS
        )
        self.assertEqual(latest_full["document_id"], "aa-doc")

    def test_get_latest_document_returns_none_when_no_match(self):
        client = CompaniesHouseClient(api_key="k")
        client.list_filing_documents = Mock(
            return_value=[
                {
                    "document_id": "cs01-doc",
                    "date": "2025-01-01",
                    "friendly_types": ["Confirmation statement"],
                }
            ]
        )

        latest_full = client.get_latest_document(
            "11124272", FilingDocumentType.FULL_ACCOUNTS
        )
        self.assertIsNone(latest_full)

    def test_friendly_mapping_covers_cross_checked_codes(self):
        self.assertIn(
            FilingDocumentType.APPOINTMENT,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "AP01"}),
        )
        self.assertIn(
            FilingDocumentType.APPOINTMENT,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "AP03"}),
        )
        self.assertIn(
            FilingDocumentType.TERMINATION_OF_APPOINTMENT,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "TM01"}),
        )
        self.assertIn(
            FilingDocumentType.TERMINATION_OF_APPOINTMENT,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "TM02"}),
        )
        self.assertIn(
            FilingDocumentType.DIRECTOR_DETAILS_CHANGED,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "CH01"}),
        )
        self.assertIn(
            FilingDocumentType.CURRENT_ACCOUNTING_PERIOD_SHORTENED,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "AA01"}),
        )
        self.assertIn(
            FilingDocumentType.REGISTERED_OFFICE_ADDRESS_CHANGED,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "AD01"}),
        )
        self.assertIn(
            FilingDocumentType.INCORPORATION,
            CompaniesHouseClient._friendly_document_types_for_item({"type": "NEWINC"}),
        )

    def test_get_latest_document_for_new_friendly_type(self):
        client = CompaniesHouseClient(api_key="k")
        client.list_filing_documents = Mock(
            return_value=[
                {
                    "document_id": "old-appt",
                    "date": "2022-01-01",
                    "friendly_types": ["Appointment"],
                },
                {
                    "document_id": "new-appt",
                    "date": "2024-10-02",
                    "friendly_types": ["Appointment"],
                },
                {
                    "document_id": "other",
                    "date": "2025-01-01",
                    "friendly_types": ["Confirmation statement"],
                },
            ]
        )

        latest_appt = client.get_latest_document(
            "11124272", FilingDocumentType.APPOINTMENT
        )
        self.assertEqual(latest_appt["document_id"], "new-appt")

    def test_download_document_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = CompaniesHouseClient(api_key="k", cache_dir=os.path.join(tmp, "cache"))
            client.session.request = Mock(
                return_value=DummyResponse(ok=True, chunks=[b"hello", b"world"])
            )
            path = os.path.join(tmp, "doc.pdf")
            out = client.download_document("abc123", path)
            self.assertEqual(out, path)
            self.assertEqual(client.last_download_cache_hit, False)
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"helloworld")

    def test_download_document_error_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = CompaniesHouseClient(api_key="k", cache_dir=os.path.join(tmp, "cache"))
            client.session.request = Mock(
                return_value=DummyResponse(ok=False, status_code=406, text="not acceptable")
            )

            with self.assertRaises(CompaniesHouseApiError) as ctx:
                client.download_document("abc123", os.path.join(tmp, "x.pdf"))
            self.assertEqual(ctx.exception.status_code, 406)

    def test_extract_document_id_from_full_url(self):
        self.assertEqual(
            CompaniesHouseClient._extract_document_id(
                "https://document-api.company-information.service.gov.uk/document/abc123"
            ),
            "abc123",
        )

    def test_extract_document_id_invalid_raises(self):
        with self.assertRaises(ValueError):
            CompaniesHouseClient._extract_document_id(
                "https://document-api.company-information.service.gov.uk/not-a-doc/abc123"
            )

    @patch("companies_house_client.time.sleep", return_value=None)
    def test_request_json_retries_on_429_then_succeeds(self, _sleep_mock):
        client = CompaniesHouseClient(api_key="k", max_retries_on_429=1)
        r1 = DummyResponse(ok=False, status_code=429, text="too many")
        r2 = DummyResponse(ok=True, status_code=200, json_data={"ok": True})
        client.session.request = Mock(side_effect=[r1, r2])

        out = client.search_companies("tesco")

        self.assertEqual(out["ok"], True)
        self.assertEqual(client.session.request.call_count, 2)

    @patch("companies_house_client.time.sleep", return_value=None)
    def test_request_json_raises_after_429_retries_exhausted(self, _sleep_mock):
        client = CompaniesHouseClient(api_key="k", max_retries_on_429=1)
        r1 = DummyResponse(ok=False, status_code=429, text="too many")
        r2 = DummyResponse(ok=False, status_code=429, text="too many")
        client.session.request = Mock(side_effect=[r1, r2])

        with self.assertRaises(CompaniesHouseApiError) as ctx:
            client.search_companies("tesco")

        self.assertEqual(ctx.exception.status_code, 429)
        self.assertEqual(client.session.request.call_count, 2)

    @patch("companies_house_client.time.sleep", return_value=None)
    def test_download_document_retries_on_429(self, _sleep_mock):
        with tempfile.TemporaryDirectory() as tmp:
            client = CompaniesHouseClient(
                api_key="k",
                max_retries_on_429=1,
                cache_dir=os.path.join(tmp, "cache"),
            )
            r1 = DummyResponse(ok=False, status_code=429, text="too many")
            r2 = DummyResponse(ok=True, status_code=200, chunks=[b"file"])
            client.session.request = Mock(side_effect=[r1, r2])
            path = os.path.join(tmp, "doc.pdf")
            out = client.download_document("abc123", path)
            self.assertEqual(out, path)
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"file")
        self.assertEqual(client.session.request.call_count, 2)

    def test_download_document_cache_hit_skips_http(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            cache_path = client._cache_file_path("abc123", "application/pdf")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(b"cached")
            client.session.request = Mock()

            out_path = os.path.join(tmp, "out", "doc.pdf")
            out = client.download_document("abc123", out_path)

            self.assertEqual(out, out_path)
            self.assertEqual(client.last_download_cache_hit, True)
            with open(out_path, "rb") as fh:
                self.assertEqual(fh.read(), b"cached")
            client.session.request.assert_not_called()

    def test_download_document_cache_miss_populates_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            client.session.request = Mock(
                return_value=DummyResponse(ok=True, status_code=200, chunks=[b"filedata"])
            )

            out_path = os.path.join(tmp, "out", "doc.pdf")
            client.download_document("abc123", out_path)

            cache_path = client._cache_file_path("abc123", "application/pdf")
            self.assertEqual(client.last_download_cache_hit, False)
            self.assertTrue(cache_path.exists())
            self.assertEqual(cache_path.read_bytes(), b"filedata")
            self.assertEqual(Path(out_path).read_bytes(), b"filedata")
            client.session.request.assert_called_once()

    def test_download_document_writes_cache_index_when_company_number_provided(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            client.session.request = Mock(
                return_value=DummyResponse(ok=True, status_code=200, chunks=[b"filedata"])
            )

            out_path = os.path.join(tmp, "out", "doc.pdf")
            client.download_document(
                "abc123",
                out_path,
                company_number="09618502",
            )

            index_path = Path(cache_dir) / "cache_index.jsonl"
            self.assertTrue(index_path.exists())
            rows = [json.loads(line) for line in index_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["document_id"], "abc123")
            self.assertEqual(rows[0]["company_number"], "09618502")
            self.assertEqual(rows[0]["accept"], "application/pdf")
            self.assertEqual(rows[0]["cache_hit"], False)

    def test_download_document_cache_hit_writes_index_entry(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            cache_path = client._cache_file_path("abc123", "application/pdf")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(b"cached")
            client.session.request = Mock()

            out_path = os.path.join(tmp, "out", "doc.pdf")
            client.download_document(
                "abc123",
                out_path,
                company_number="09618502",
            )

            index_path = Path(cache_dir) / "cache_index.jsonl"
            rows = [json.loads(line) for line in index_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["cache_hit"], True)
            client.session.request.assert_not_called()

    def test_download_document_uses_cache_on_second_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            client.session.request = Mock(
                return_value=DummyResponse(ok=True, status_code=200, chunks=[b"filedata"])
            )

            first_out = os.path.join(tmp, "out1", "doc.pdf")
            second_out = os.path.join(tmp, "out2", "doc.pdf")

            client.download_document("abc123", first_out)
            self.assertEqual(client.last_download_cache_hit, False)
            client.session.request.reset_mock()
            client.download_document("abc123", second_out)

            self.assertEqual(client.last_download_cache_hit, True)
            client.session.request.assert_not_called()
            self.assertEqual(Path(second_out).read_bytes(), b"filedata")

    def test_download_document_empty_cache_file_triggers_redownload(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = os.path.join(tmp, "cache")
            client = CompaniesHouseClient(api_key="k", cache_dir=cache_dir)
            cache_path = client._cache_file_path("abc123", "application/pdf")
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(b"")

            client.session.request = Mock(
                return_value=DummyResponse(ok=True, status_code=200, chunks=[b"fresh"])
            )

            out_path = os.path.join(tmp, "out", "doc.pdf")
            client.download_document("abc123", out_path)

            client.session.request.assert_called_once()
            self.assertEqual(cache_path.read_bytes(), b"fresh")
            self.assertEqual(Path(out_path).read_bytes(), b"fresh")

    @patch("companies_house_client.time.sleep", return_value=None)
    def test_retry_after_header_is_preferred(self, sleep_mock):
        client = CompaniesHouseClient(
            api_key="k", max_retries_on_429=1, retry_backoff_seconds=9.0
        )
        r1 = DummyResponse(
            ok=False,
            status_code=429,
            text="too many",
            headers={"Retry-After": "1"},
        )
        r2 = DummyResponse(ok=True, status_code=200, json_data={"items": []})
        client.session.request = Mock(side_effect=[r1, r2])

        client.search_companies("tesco")

        sleep_mock.assert_called_once_with(1.0)

    def test_extract_latest_full_accounts_downloads_and_extracts(self):
        client = CompaniesHouseClient(api_key="k")
        client.get_latest_document = Mock(return_value={"document_id": "abc123"})
        client.download_document = Mock(return_value="C:\\tmp\\full_accounts.pdf")

        expected = ExtractionResult(
            source_path="C:\\tmp\\full_accounts.pdf",
            model="openai/gpt-4o-mini",
            requested_types=[ExtractionType.PersonnelDetails],
            personnel_details=[
                PersonnelDetail(
                    first_name="A",
                    last_name="B",
                    job_title="Director",
                )
            ],
        )
        extractor = Mock()
        extractor.extract = Mock(return_value=expected)

        out = client.extract_latest_full_accounts(
            company_number="11124272",
            output_path="C:\\tmp\\full_accounts.pdf",
            extractor=extractor,
            extraction_types=[ExtractionType.PersonnelDetails],
        )

        self.assertEqual(out, expected)
        client.get_latest_document.assert_called_once_with(
            company_number="11124272",
            document_type=FilingDocumentType.FULL_ACCOUNTS,
        )
        client.download_document.assert_called_once_with(
            document_id="abc123",
            output_path="C:\\tmp\\full_accounts.pdf",
            accept="application/pdf",
        )
        extractor.extract.assert_called_once_with(
            document_path="C:\\tmp\\full_accounts.pdf",
            extraction_types=[ExtractionType.PersonnelDetails],
        )

    def test_extract_latest_full_accounts_missing_doc_raises(self):
        client = CompaniesHouseClient(api_key="k")
        client.get_latest_document = Mock(return_value=None)
        extractor = Mock()

        with self.assertRaises(ValueError):
            client.extract_latest_full_accounts(
                company_number="11124272",
                output_path="C:\\tmp\\full_accounts.pdf",
                extractor=extractor,
                extraction_types=[ExtractionType.BalanceSheet],
            )

    def test_extract_latest_mat_annual_report_wrapper_uses_expected_type(self):
        client = CompaniesHouseClient(api_key="k")
        expected = ExtractionResult(
            source_path="C:\\tmp\\full_accounts.pdf",
            model="openai/gpt-4o-mini",
            requested_types=[ExtractionType.AcademyTrustAnnualReport],
        )
        client.extract_latest_full_accounts = Mock(return_value=expected)
        extractor = Mock()

        out = client.extract_latest_mat_annual_report(
            company_number="11124272",
            output_path="C:\\tmp\\full_accounts.pdf",
            extractor=extractor,
        )

        self.assertEqual(out, expected)
        client.extract_latest_full_accounts.assert_called_once_with(
            company_number="11124272",
            output_path="C:\\tmp\\full_accounts.pdf",
            extractor=extractor,
            extraction_types=[ExtractionType.AcademyTrustAnnualReport],
            accept="application/pdf",
        )


class OpenRouterDocumentExtractorTests(unittest.TestCase):
    def test_extract_personnel_only(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key", model="openrouter/auto")
        llm_json = """
        {
          "personnel_details": [
            {"first_name": "Ada", "last_name": "Lovelace", "job_title": "Director"}
          ]
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.PersonnelDetails],
            )

        self.assertEqual(len(result.personnel_details or []), 1)
        self.assertEqual(result.personnel_details[0].first_name, "Ada")
        self.assertEqual(result.personnel_details[0].last_name, "Lovelace")
        self.assertEqual(result.personnel_details[0].job_title, "Director")
        self.assertIsNone(result.balance_sheet)
        self.assertEqual(result.model, "openrouter/auto")
        self.assertEqual(result.requested_types, [ExtractionType.PersonnelDetails])
        extractor._post_openrouter_chat_completion.assert_called_once()
        payload = extractor._post_openrouter_chat_completion.call_args[1]["payload"]
        self.assertEqual(payload["provider"]["require_parameters"], False)
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        schema = payload["response_format"]["json_schema"]["schema"]
        self.assertIn("personnel_details", schema["required"])
        self.assertNotIn("balance_sheet", schema["required"])
        fields = schema["properties"]["personnel_details"]["items"]["properties"]
        self.assertEqual(fields["first_name"]["description"], "Person's first name.")
        messages = payload["messages"]
        self.assertEqual(messages[0]["role"], "system")
        self.assertEqual(messages[1]["role"], "user")
        content = messages[1]["content"]
        self.assertEqual(content[0]["type"], "text")
        self.assertEqual(content[1]["type"], "file")
        self.assertIn("file", content[1])
        self.assertTrue(content[1]["file"]["file_data"].startswith("data:"))
        self.assertIn(";base64,", content[1]["file"]["file_data"])

    def test_extract_balance_sheet_only(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "balance_sheet": [
            {"line_item": "Total assets", "value": "150000", "period": "2025", "currency": "GBP"}
          ]
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.BalanceSheet],
            )

        self.assertIsNone(result.personnel_details)
        self.assertEqual(len(result.balance_sheet or []), 1)
        self.assertEqual(result.balance_sheet[0].line_item, "Total assets")
        self.assertEqual(result.balance_sheet[0].value, "150000")
        self.assertEqual(result.requested_types, [ExtractionType.BalanceSheet])
        payload = extractor._post_openrouter_chat_completion.call_args[1]["payload"]
        self.assertEqual(payload["provider"]["require_parameters"], False)
        self.assertEqual(payload["response_format"]["type"], "json_schema")
        schema = payload["response_format"]["json_schema"]["schema"]
        self.assertIn("balance_sheet", schema["required"])
        self.assertNotIn("personnel_details", schema["required"])
        fields = schema["properties"]["balance_sheet"]["items"]["properties"]
        self.assertEqual(
            fields["line_item"]["description"], "Balance sheet line item name."
        )

    def test_extract_metadata_only(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "metadata": {
            "trust_name": "ACORN MULTI ACADEMY TRUST",
            "company_registration_number": "09253218",
            "financial_year_ending": "2024-08-31",
            "accounting_officer": "Jane Smith"
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.Metadata],
            )

        self.assertIsNone(result.personnel_details)
        self.assertIsNone(result.balance_sheet)
        self.assertIsNotNone(result.metadata)
        self.assertIsInstance(result.metadata, Metadata)
        self.assertEqual(result.metadata.trust_name, "ACORN MULTI ACADEMY TRUST")
        self.assertEqual(result.metadata.company_registration_number, "09253218")
        self.assertEqual(result.metadata.financial_year_ending, "2024-08-31")
        payload = extractor._post_openrouter_chat_completion.call_args[1]["payload"]
        schema = payload["response_format"]["json_schema"]["schema"]
        self.assertIn("metadata", schema["required"])
        metadata_fields = schema["properties"]["metadata"]["properties"]
        self.assertEqual(metadata_fields["company_registration_number"]["pattern"], "^[0-9]{8}$")

    def test_extract_metadata_pads_7_digit_company_number(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "metadata": {
            "trust_name": "LIFT SCHOOLS",
            "company_registration_number": "6625091",
            "financial_year_ending": "2025-08-31",
            "accounting_officer": "Example Officer"
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.Metadata],
            )

        self.assertIsNotNone(result.metadata)
        self.assertEqual(result.metadata.company_registration_number, "06625091")

    def test_extract_academy_trust_annual_report_only(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "academy_trust_annual_report": {
            "metadata": {
              "trust_name": "ACORN MULTI ACADEMY TRUST",
              "company_registration_number": "09253218",
              "financial_year_ending": "2024-08-31",
              "accounting_officer": "Jane Smith"
            },
            "governance": {
              "trustees": [
                {"name": "Ada Lovelace", "meetings_attended": 5, "meetings_possible": 6}
              ]
            },
            "statement_of_financial_activities": {
              "income": {
                "donations_and_capital_grants": {
                  "unrestricted_funds": 23673,
                  "restricted_general_funds": 214000,
                  "restricted_fixed_asset_funds": 3101812,
                  "total": 3339485
                },
                "charitable_activities_education": null,
                "other_trading_activities": null,
                "investments": null
              },
              "expenditure": {
                "charitable_activities_education": null
              }
            },
            "balance_sheet": {
              "fixed_assets": 7530887,
              "current_assets": {
                "debtors": 300689,
                "cash_at_bank": 983289
              },
              "liabilities": {
                "creditors_within_one_year": 200000,
                "pension_scheme_liability": 132000
              },
              "net_assets": 8187176
            },
            "staffing_data": {
              "average_headcount_fte": 101,
              "total_staff_costs": 4500000,
              "high_pay_bands": [
                {"band_range": "£70,001 - £80,000", "count": 1}
              ]
            }
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.AcademyTrustAnnualReport],
            )

        self.assertIsNotNone(result.academy_trust_annual_report)
        self.assertIsInstance(result.academy_trust_annual_report, AcademyTrustAnnualReport)
        self.assertIsInstance(result.academy_trust_annual_report.balance_sheet, DetailedBalanceSheet)
        self.assertEqual(
            result.academy_trust_annual_report.metadata.company_registration_number,
            "09253218",
        )
        self.assertEqual(
            result.academy_trust_annual_report.balance_sheet.current_assets.cash_at_bank,
            983289,
        )
        payload = extractor._post_openrouter_chat_completion.call_args[1]["payload"]
        schema = payload["response_format"]["json_schema"]["schema"]
        self.assertIn("academy_trust_annual_report", schema["required"])
        annual_report_props = schema["properties"]["academy_trust_annual_report"]["properties"]
        self.assertIn("statement_of_financial_activities", annual_report_props)
        self.assertIn("balance_sheet", annual_report_props)

    def test_mat_numeric_normalization_parses_accounting_formats(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "academy_trust_annual_report": {
            "metadata": {
              "trust_name": "ACORN MULTI ACADEMY TRUST",
              "company_registration_number": "09253218",
              "financial_year_ending": "2024-08-31",
              "accounting_officer": "Jane Smith"
            },
            "governance": {
              "trustees": [
                {"name": "Ada Lovelace", "meetings_attended": "6", "meetings_possible": "6"}
              ]
            },
            "statement_of_financial_activities": {
              "income": {
                "donations_and_capital_grants": {
                  "unrestricted_funds": "£23,673",
                  "restricted_general_funds": "214,000",
                  "restricted_fixed_asset_funds": "(3,101,812)",
                  "total": "-2,864,139"
                },
                "charitable_activities_education": null,
                "other_trading_activities": null,
                "investments": null
              },
              "expenditure": {
                "charitable_activities_education": null
              }
            },
            "balance_sheet": {
              "fixed_assets": "7,530,887",
              "current_assets": {
                "debtors": "300,689",
                "cash_at_bank": "983,289"
              },
              "liabilities": {
                "creditors_within_one_year": "200,000",
                "pension_scheme_liability": "132,000"
              },
              "net_assets": "8,482,865"
            },
            "staffing_data": {
              "average_headcount_fte": "101",
              "total_staff_costs": "£4,500,000",
              "high_pay_bands": [
                {"band_range": "£70,001 - £80,000", "count": "1"}
              ]
            }
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[ExtractionType.AcademyTrustAnnualReport],
            )

        report = result.academy_trust_annual_report
        self.assertIsNotNone(report)
        self.assertEqual(report.governance.trustees[0].meetings_attended, 6)
        self.assertEqual(
            report.statement_of_financial_activities.income.donations_and_capital_grants.unrestricted_funds,
            23673.0,
        )
        self.assertEqual(
            report.statement_of_financial_activities.income.donations_and_capital_grants.restricted_fixed_asset_funds,
            -3101812.0,
        )
        self.assertEqual(report.staffing_data.high_pay_bands[0].count, 1)

    def test_reconciliation_warnings_for_sofa_and_balance_sheet(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "statement_of_financial_activities": {
            "income": {
              "donations_and_capital_grants": {
                "unrestricted_funds": 100,
                "restricted_general_funds": 50,
                "restricted_fixed_asset_funds": 25,
                "total": 100
              },
              "charitable_activities_education": null,
              "other_trading_activities": null,
              "investments": null
            },
            "expenditure": {
              "charitable_activities_education": null
            }
          },
          "detailed_balance_sheet": {
            "fixed_assets": 1000,
            "current_assets": {
              "debtors": 200,
              "cash_at_bank": 300
            },
            "liabilities": {
              "creditors_within_one_year": 100,
              "pension_scheme_liability": 50
            },
            "net_assets": 1200
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[
                    ExtractionType.StatementOfFinancialActivities,
                    ExtractionType.DetailedBalanceSheet,
                ],
            )

        self.assertEqual(len(result.validation_warnings), 2)
        self.assertIn(
            "statement_of_financial_activities.income.donations_and_capital_grants total mismatch",
            result.validation_warnings[0],
        )
        self.assertIn(
            "detailed_balance_sheet net_assets mismatch",
            result.validation_warnings[1],
        )

    def test_reconciliation_warning_for_detailed_balance_sheet_mismatch_between_sections(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "detailed_balance_sheet": {
            "fixed_assets": 1000,
            "current_assets": {
              "debtors": 200,
              "cash_at_bank": 300
            },
            "liabilities": {
              "creditors_within_one_year": 100,
              "pension_scheme_liability": 50
            },
            "net_assets": 1350
          },
          "academy_trust_annual_report": {
            "metadata": {
              "trust_name": "ACORN MULTI ACADEMY TRUST",
              "company_registration_number": "09253218",
              "financial_year_ending": "2024-08-31",
              "accounting_officer": "Jane Smith"
            },
            "governance": {"trustees": []},
            "statement_of_financial_activities": {
              "income": {
                "donations_and_capital_grants": null,
                "charitable_activities_education": null,
                "other_trading_activities": null,
                "investments": null
              },
              "expenditure": {"charitable_activities_education": null}
            },
            "balance_sheet": {
              "fixed_assets": 1000,
              "current_assets": {
                "debtors": 200,
                "cash_at_bank": 300
              },
              "liabilities": {
                "creditors_within_one_year": 100,
                "pension_scheme_liability": 50
              },
              "net_assets": 1200
            },
            "staffing_data": {
              "average_headcount_fte": null,
              "total_staff_costs": null,
              "high_pay_bands": []
            }
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[
                    ExtractionType.DetailedBalanceSheet,
                    ExtractionType.AcademyTrustAnnualReport,
                ],
            )

        self.assertTrue(
            any(
                "Detailed balance sheet differs between top-level `detailed_balance_sheet`"
                in warning
                for warning in result.validation_warnings
            )
        )

    def test_build_response_schema_for_individual_mat_sections(self):
        schema = OpenRouterDocumentExtractor._build_response_format(
            [
                ExtractionType.Metadata,
                ExtractionType.Governance,
                ExtractionType.StatementOfFinancialActivities,
                ExtractionType.DetailedBalanceSheet,
                ExtractionType.StaffingData,
            ]
        )["json_schema"]["schema"]

        self.assertIn("metadata", schema["required"])
        self.assertIn("governance", schema["required"])
        self.assertIn("statement_of_financial_activities", schema["required"])
        self.assertIn("detailed_balance_sheet", schema["required"])
        self.assertIn("staffing_data", schema["required"])
        self.assertNotIn("academy_trust_annual_report", schema["required"])
        self.assertNotIn("balance_sheet", schema["required"])

    def test_extract_allows_missing_detailed_balance_sheet_with_warning(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "metadata": {
            "trust_name": "ACE LEARNING",
            "company_registration_number": "08681270",
            "financial_year_ending": "2025-08-31",
            "accounting_officer": "Example Officer"
          }
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            result = extractor.extract(
                path,
                extraction_types=[
                    ExtractionType.Metadata,
                    ExtractionType.DetailedBalanceSheet,
                ],
            )

        self.assertIsNotNone(result.metadata)
        self.assertIsNone(result.detailed_balance_sheet)
        self.assertTrue(
            any(
                "Section `detailed_balance_sheet` missing/null; set to null."
                in warning
                for warning in result.validation_warnings
            )
        )

    def test_extract_rejects_missing_personnel_fields(self):
        extractor = OpenRouterDocumentExtractor(api_key="or_key")
        llm_json = """
        {
          "personnel_details": [
            {"first_name": "Ada", "last_name": "Lovelace", "job_title": ""}
          ]
        }
        """
        response = {
            "choices": [{"message": {"content": llm_json}}],
        }
        extractor._post_openrouter_chat_completion = Mock(return_value=response)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "full_accounts.txt")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("Sample full accounts text")

            with self.assertRaises(DocumentExtractionError):
                extractor.extract(
                    path,
                    extraction_types=[ExtractionType.PersonnelDetails],
                )

    def test_parse_json_response_handles_markdown_code_fence(self):
        content = """```json
        {"personnel_details": [], "balance_sheet": []}
        ```"""
        out = OpenRouterDocumentExtractor._parse_json_response(content)
        self.assertEqual(out["personnel_details"], [])
        self.assertEqual(out["balance_sheet"], [])

    def test_parse_json_response_uses_repair_fallback_when_available(self):
        content = "{personnel_details: [], balance_sheet: [],}"
        repaired = {"personnel_details": [], "balance_sheet": []}
        with patch("openrouter_document_extractor.json_repair_loads", return_value=repaired) as mocked:
            out = OpenRouterDocumentExtractor._parse_json_response(content)
        self.assertEqual(out["personnel_details"], [])
        self.assertEqual(out["balance_sheet"], [])
        mocked.assert_called()


class FullReportsExtractionPipelineTests(unittest.TestCase):
    def test_parse_company_numbers_csv_normalizes_and_dedupes(self):
        parsed = _parse_company_numbers_csv(" 9618502,09618502, ABC, 08496504 ,")
        self.assertEqual(parsed, ["09618502", "08496504"])

    def test_validate_input_xor_rejects_both_and_neither(self):
        with self.assertRaises(ValueError):
            _validate_input_xor("", "")
        with self.assertRaises(ValueError):
            _validate_input_xor("Trusts.xlsx", "09618502")
        _validate_input_xor("Trusts.xlsx", "")
        _validate_input_xor("", "09618502")

    def test_validate_worker_settings_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            _validate_worker_settings(0, 1, 1)
        with self.assertRaises(ValueError):
            _validate_worker_settings(1, 0, 1)
        with self.assertRaises(ValueError):
            _validate_worker_settings(1, 1, 0)
        _validate_worker_settings(1, 1, 1)

    def test_shared_throttle_enforces_global_spacing_and_count(self):
        client = CompaniesHouseClient(api_key="k")
        client.session.request = Mock(return_value=DummyResponse())
        throttle = GlobalCHThrottle(min_interval_seconds=0.05, lock=threading.Lock())
        _install_global_throttle_on_client(client, throttle)

        started = time.monotonic()
        client.session.request("GET", "https://example.test/1")
        client.session.request("GET", "https://example.test/2")
        elapsed = time.monotonic() - started

        self.assertGreaterEqual(elapsed, 0.04)
        self.assertEqual(throttle.request_count, 2)

    def test_bounded_queue_backpressure(self):
        work_queue: queue.Queue[object] = queue.Queue(maxsize=1)
        work_queue.put({"job": 1})
        shutdown = threading.Event()
        pushed = threading.Event()

        def producer() -> None:
            ok = _enqueue_with_backpressure(work_queue, {"job": 2}, shutdown, timeout_seconds=0.01)
            if ok:
                pushed.set()

        thread = threading.Thread(target=producer)
        thread.start()
        time.sleep(0.05)
        self.assertFalse(pushed.is_set())
        self.assertEqual(work_queue.get()["job"], 1)
        thread.join(timeout=1.0)
        self.assertTrue(pushed.is_set())
        self.assertEqual(work_queue.get()["job"], 2)

    def test_job_state_transitions_persist_terminal_states(self):
        conn = sqlite3.connect(":memory:")
        _create_tables(conn)
        run_id = _insert_run(
            conn,
            {
                "mode": "all",
                "input_source_type": "company_numbers",
                "input_source_value": "09618502",
                "output_run_dir": "output/run_test",
                "model": "m",
                "fallback_models_json": "[]",
                "schema_profile": "compact_single_call",
                "ch_workers": 2,
                "or_workers": 4,
                "max_pending_extractions": 10,
                "ch_min_request_interval_seconds": 0.0,
                "filing_history_items_per_page": 100,
                "retries_on_invalid_json": 2,
                "openrouter_timeout_seconds": 180.0,
                "total_jobs": 1,
            },
        )
        _insert_job(
            conn,
            {
                "run_id": run_id,
                "job_index": 1,
                "company_number": "09618502",
                "download_status": "pending",
                "extract_status": "pending",
                "final_status": "pending",
            },
        )
        _update_download_state(conn, run_id, 1, "running", {"download_attempts": 1})
        _update_download_state(
            conn,
            run_id,
            1,
            "success",
            {"document_id": "doc-1", "pdf_path": "a.pdf", "download_error": None},
        )
        _update_extract_state(conn, run_id, 1, "running", {"extract_attempts": 1})
        _update_extract_state(conn, run_id, 1, "failed", {"extract_error": "bad json"})
        _update_final_state(conn, run_id, 1, "failed", "bad json")

        row = conn.execute(
            """
            SELECT download_status, extract_status, final_status, document_id, extract_error, final_error
            FROM pipeline_jobs
            WHERE run_id = ? AND job_index = 1
            """,
            (run_id,),
        ).fetchone()
        self.assertEqual(row[0], "success")
        self.assertEqual(row[1], "failed")
        self.assertEqual(row[2], "failed")
        self.assertEqual(row[3], "doc-1")
        self.assertEqual(row[4], "bad json")
        self.assertEqual(row[5], "bad json")
        conn.close()


class CompaniesHouseLiveSmokeTest(unittest.TestCase):
    def test_live_list_documents_for_known_company(self):
        api_key = os.getenv("CH_API_KEY")
        if not api_key:
            self.skipTest("CH_API_KEY not set; skipping live smoke test")

        client = CompaniesHouseClient(api_key=api_key)
        docs = client.list_filing_documents("09618502")
        self.assertGreater(len(docs), 0)
        self.assertIn("document_id", docs[0])

    def test_live_full_report_for_9253218_and_extract_content_names(self):
        api_key = os.getenv("CH_API_KEY")
        if not api_key:
            self.skipTest("CH_API_KEY not set; skipping live smoke test")
        openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
        if not openrouter_api_key:
            self.skipTest(
                "OPENROUTER_API_KEY not set; skipping live extraction in smoke test"
            )

        raw_company_number = "9253218"
        company_number = raw_company_number.zfill(8)
        client = CompaniesHouseClient(api_key=api_key)

        profile = client.get_company_profile(company_number)
        filing_history = client.get_all_filing_history(company_number=company_number)

        content_names = sorted(
            {
                str(item.get("description")).strip()
                for item in filing_history
                if item.get("description")
            }
        )

        full_report = {
            "company_number": company_number,
            "company_name": profile.get("company_name"),
            "company_profile": profile,
            "filing_history_count": len(filing_history),
            "filing_history": filing_history,
            "content_names": content_names,
        }

        latest_full_accounts = client.get_latest_document(
            company_number=company_number,
            document_type=FilingDocumentType.FULL_ACCOUNTS,
        )
        self.assertIsNotNone(latest_full_accounts)
        document_id = str((latest_full_accounts or {}).get("document_id") or "").strip()
        self.assertTrue(document_id)

        output_dir = os.path.join("SourceData", "_tmp_live_tests")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"{company_number}_latest_full_accounts.pdf")
        downloaded_path = client.download_document(
            document_id=document_id,
            output_path=output_path,
            accept="application/pdf",
        )
        extractor = OpenRouterDocumentExtractor(
            api_key=openrouter_api_key,
            model=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
        )
        extraction = extractor.extract(
            document_path=downloaded_path,
            extraction_types=[
                ExtractionType.PersonnelDetails,
                ExtractionType.BalanceSheet,
            ],
        )

        print(f"\nFull report company_number: {company_number}")
        print(f"Full report company_name: {profile.get('company_name')}")
        print(f"Content names ({len(content_names)}):")
        for name in content_names:
            print(f"- {name}")

        personnel_rows = extraction.personnel_details or []
        print(f"Personnel findings ({len(personnel_rows)}):")
        if not personnel_rows:
            print("- none")
        for row in personnel_rows:
            print(f"- {row.first_name} {row.last_name}: {row.job_title}")

        balance_rows = extraction.balance_sheet or []
        print(f"Balance sheet findings ({len(balance_rows)}):")
        if not balance_rows:
            print("- none")
        for row in balance_rows:
            period = f" | period={row.period}" if row.period else ""
            currency = f" | currency={row.currency}" if row.currency else ""
            print(f"- {row.line_item}: {row.value}{period}{currency}")

        self.assertEqual(str(profile.get("company_number")), company_number)
        self.assertGreater(full_report["filing_history_count"], 0)
        self.assertIsInstance(full_report["content_names"], list)
        self.assertTrue(all(isinstance(name, str) for name in full_report["content_names"]))
        self.assertIsNotNone(extraction.personnel_details)
        self.assertIsNotNone(extraction.balance_sheet)


if __name__ == "__main__":
    unittest.main()
