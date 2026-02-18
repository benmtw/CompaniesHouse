import base64
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from companies_house_client import (
    CompaniesHouseApiError,
    CompaniesHouseClient,
    DocumentExtractionError,
    ExtractionResult,
    ExtractionType,
    FilingDocumentType,
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
        client = CompaniesHouseClient(api_key="k")
        client.session.request = Mock(
            return_value=DummyResponse(ok=True, chunks=[b"hello", b"world"])
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "doc.pdf")
            out = client.download_document("abc123", path)
            self.assertEqual(out, path)
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"helloworld")

    def test_download_document_error_raises(self):
        client = CompaniesHouseClient(api_key="k")
        client.session.request = Mock(
            return_value=DummyResponse(ok=False, status_code=406, text="not acceptable")
        )

        with self.assertRaises(CompaniesHouseApiError) as ctx:
            client.download_document("abc123", "x.pdf")
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
        client = CompaniesHouseClient(api_key="k", max_retries_on_429=1)
        r1 = DummyResponse(ok=False, status_code=429, text="too many")
        r2 = DummyResponse(ok=True, status_code=200, chunks=[b"file"])
        client.session.request = Mock(side_effect=[r1, r2])

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "doc.pdf")
            out = client.download_document("abc123", path)
            self.assertEqual(out, path)
            with open(path, "rb") as fh:
                self.assertEqual(fh.read(), b"file")
        self.assertEqual(client.session.request.call_count, 2)

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


class CompaniesHouseLiveSmokeTest(unittest.TestCase):
    def test_live_list_documents_for_known_company(self):
        api_key = os.getenv("CH_API_KEY")
        if not api_key:
            self.skipTest("CH_API_KEY not set; skipping live smoke test")

        client = CompaniesHouseClient(api_key=api_key)
        docs = client.list_filing_documents("09618502")
        self.assertGreater(len(docs), 0)
        self.assertIn("document_id", docs[0])


if __name__ == "__main__":
    unittest.main()
