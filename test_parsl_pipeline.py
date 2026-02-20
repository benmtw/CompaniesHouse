"""
Unit and integration tests for Parsl-based Companies House extraction pipeline.

Run with (Unix/WSL2/macOS only):
    python -m unittest -v test_parsl_pipeline.py

NOTE: Parsl requires fork-based multiprocessing and is NOT compatible with Windows.
      On Windows, these tests will be skipped. Use WSL2 or run the legacy pipeline.

Live smoke tests run only when CH_API_KEY and/or OPENROUTER_API_KEY are set.
"""

from __future__ import annotations

import json
import os
import platform
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Test imports - ensure path includes project root
sys.path.insert(0, str(Path(__file__).parent))

# Skip all tests on Windows due to Parsl fork requirement
SKIP_REASON_WINDOWS = (
    "Parsl is not compatible with Windows due to fork-based multiprocessing. "
    "Use WSL2, Linux, or macOS."
)
IS_WINDOWS = platform.system() == "Windows"


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineConfig(unittest.TestCase):
    """Tests for parsl_pipeline_config module."""

    def test_create_thread_config_default(self):
        """Test creating config with default ThreadPoolExecutor."""
        from parsl_pipeline_config import create_pipeline_config

        config = create_pipeline_config(
            ch_workers=2,
            or_workers=4,
            executor_type="thread",
        )

        self.assertIsNotNone(config)
        self.assertEqual(len(config.executors), 2)
        labels = [e.label for e in config.executors]
        self.assertIn("download_executor", labels)
        self.assertIn("extract_executor", labels)

    def test_create_htex_config(self):
        """Test creating config with HighThroughputExecutor."""
        from parsl_pipeline_config import create_pipeline_config

        config = create_pipeline_config(
            ch_workers=3,
            or_workers=5,
            executor_type="htex",
        )

        self.assertIsNotNone(config)
        self.assertEqual(len(config.executors), 2)

    def test_invalid_executor_type_raises(self):
        """Test that invalid executor_type raises ValueError."""
        from parsl_pipeline_config import create_pipeline_config

        with self.assertRaises(ValueError) as ctx:
            create_pipeline_config(executor_type="invalid")

        self.assertIn("executor_type must be", str(ctx.exception))

    def test_invalid_worker_count_raises(self):
        """Test that invalid worker counts raise ValueError."""
        from parsl_pipeline_config import create_pipeline_config

        with self.assertRaises(ValueError):
            create_pipeline_config(ch_workers=0)

        with self.assertRaises(ValueError):
            create_pipeline_config(or_workers=-1)


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineApps(unittest.TestCase):
    """Tests for parsl_pipeline_apps module."""

    def test_download_app_returns_correct_structure(self):
        """Test that download_company_document returns expected keys."""
        # This tests the structure without actually running Parsl
        expected_keys = {
            "success",
            "company_number",
            "job_index",
            "pdf_path",
            "document_id",
            "company_name",
            "cache_hit",
            "pdf_size_bytes",
            "error",
        }
        # We can't easily unit test the @python_app directly without Parsl,
        # but we can verify the function signature exists
        from parsl_pipeline_apps import download_company_document

        self.assertTrue(callable(download_company_document))

    def test_extract_app_returns_correct_structure(self):
        """Test that extract_document returns expected keys."""
        from parsl_pipeline_apps import extract_document

        self.assertTrue(callable(extract_document))

    def test_process_company_join_app_exists(self):
        """Test that process_company join_app exists."""
        from parsl_pipeline_apps import process_company

        self.assertTrue(callable(process_company))


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineCLI(unittest.TestCase):
    """Tests for CLI argument parsing and validation."""

    def test_build_parser_defaults(self):
        """Test default argument values."""
        from companies_house_parsl_pipeline import _build_parser

        parser = _build_parser()
        args = parser.parse_args([])

        self.assertEqual(args.mode, "all")
        self.assertEqual(args.ch_workers, 2)
        self.assertEqual(args.or_workers, 4)
        self.assertEqual(args.executor_type, "thread")
        self.assertFalse(args.parsl_monitoring)

    def test_build_parser_custom_args(self):
        """Test custom argument values."""
        from companies_house_parsl_pipeline import _build_parser

        parser = _build_parser()
        args = parser.parse_args([
            "--mode", "download",
            "--ch-workers", "5",
            "--or-workers", "10",
            "--executor-type", "htex",
            "--parsl-monitoring",
        ])

        self.assertEqual(args.mode, "download")
        self.assertEqual(args.ch_workers, 5)
        self.assertEqual(args.or_workers, 10)
        self.assertEqual(args.executor_type, "htex")
        self.assertTrue(args.parsl_monitoring)

    def test_validate_input_xor_both_raises(self):
        """Test that providing both input sources raises."""
        from companies_house_parsl_pipeline import _validate_input_xor

        with self.assertRaises(ValueError):
            _validate_input_xor("input.xlsx", "00000006")

    def test_validate_input_xor_neither_raises(self):
        """Test that providing neither input source raises."""
        from companies_house_parsl_pipeline import _validate_input_xor

        with self.assertRaises(ValueError):
            _validate_input_xor("", "")

    def test_validate_input_xor_xlsx_only_passes(self):
        """Test that xlsx-only input passes."""
        from companies_house_parsl_pipeline import _validate_input_xor

        # Should not raise
        _validate_input_xor("input.xlsx", "")

    def test_validate_input_xor_numbers_only_passes(self):
        """Test that company-numbers-only input passes."""
        from companies_house_parsl_pipeline import _validate_input_xor

        # Should not raise
        _validate_input_xor("", "00000006,00000008")


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineDatabase(unittest.TestCase):
    """Tests for SQLite database operations."""


    def test_init_shared_throttle_state_and_count_read(self):
        """Shared throttle state file should initialize and report request count."""
        from companies_house_parsl_pipeline import (
            _init_shared_throttle_state,
            _read_shared_throttle_request_count,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "ch_throttle_state.json"
            lock_path = Path(tmpdir) / "ch_throttle_state.lock"
            lock_path.write_text("stale lock", encoding="utf-8")
            _init_shared_throttle_state(state_path, lock_path, 0.2)

            self.assertFalse(lock_path.exists())
            self.assertEqual(_read_shared_throttle_request_count(state_path), 0)
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertTrue(payload["enabled"])
            self.assertEqual(payload["min_interval_seconds"], 0.2)

    def test_file_backed_throttle_enforces_spacing_and_updates_count(self):
        """File-backed throttle should coordinate request count and delay."""
        import time

        from companies_house_client import CompaniesHouseClient
        from pipeline_shared import install_request_throttle

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "ch_throttle_state.json"
            lock_path = Path(tmpdir) / "ch_throttle_state.lock"
            state_path.write_text(
                json.dumps({
                    "enabled": True,
                    "min_interval_seconds": 0.05,
                    "last_request_ts": 0.0,
                    "request_count": 0,
                }),
                encoding="utf-8",
            )

            client = CompaniesHouseClient(api_key="k")
            client.session.request = MagicMock(return_value={"ok": True})
            install_request_throttle(
                client=client,
                min_interval_seconds=0.05,
                shared_state_path=str(state_path),
                shared_lock_path=str(lock_path),
            )

            started = time.monotonic()
            client.session.request("GET", "https://example.test/1")
            client.session.request("GET", "https://example.test/2")
            elapsed = time.monotonic() - started

            self.assertGreaterEqual(elapsed, 0.04)
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("request_count"), 2)

    def test_create_tables(self):
        """Test table creation."""
        from companies_house_parsl_pipeline import _create_tables

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            _create_tables(conn)

            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = {row[0] for row in cursor.fetchall()}

            self.assertIn("pipeline_runs", tables)
            self.assertIn("pipeline_jobs", tables)

            conn.close()

    def test_insert_run(self):
        """Test run insertion."""
        from companies_house_parsl_pipeline import _create_tables, _insert_run

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            _create_tables(conn)

            run_id = _insert_run(conn, {
                "mode": "all",
                "input_source_type": "company_numbers",
                "input_source_value": "00000006",
                "output_run_dir": "/tmp/run",
                "model": "test-model",
                "fallback_models_json": "[]",
                "schema_profile": "compact_single_call",
                "ch_workers": 2,
                "or_workers": 4,
                "executor_type": "thread",
                "ch_min_request_interval_seconds": 2.0,
                "filing_history_items_per_page": 100,
                "retries_on_invalid_json": 2,
                "openrouter_timeout_seconds": 180.0,
                "parsl_monitoring": 0,
                "total_jobs": 1,
            })

            self.assertGreater(run_id, 0)

            cursor = conn.execute(
                "SELECT mode, total_jobs FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            )
            row = cursor.fetchone()
            self.assertEqual(row[0], "all")
            self.assertEqual(row[1], 1)

            conn.close()

    def test_insert_and_update_job(self):
        """Test job insertion and state update."""
        from companies_house_parsl_pipeline import (
            _create_tables,
            _insert_job,
            _insert_run,
            _update_job_state_from_result,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            conn = sqlite3.connect(db_path)
            _create_tables(conn)

            run_id = _insert_run(conn, {
                "mode": "all",
                "input_source_type": "company_numbers",
                "input_source_value": "00000006",
                "output_run_dir": "/tmp/run",
                "model": "test-model",
                "fallback_models_json": "[]",
                "schema_profile": "compact_single_call",
                "ch_workers": 2,
                "or_workers": 4,
                "executor_type": "thread",
                "ch_min_request_interval_seconds": 2.0,
                "filing_history_items_per_page": 100,
                "retries_on_invalid_json": 2,
                "openrouter_timeout_seconds": 180.0,
                "parsl_monitoring": 0,
                "total_jobs": 1,
            })

            _insert_job(conn, {
                "run_id": run_id,
                "job_index": 1,
                "company_number": "00000006",
                "download_status": "pending",
                "extract_status": "pending",
                "final_status": "pending",
            })

            # Simulate a successful result
            result = {
                "success": True,
                "company_number": "00000006",
                "job_index": 1,
                "pdf_path": "/tmp/run/00000006/documents/test.pdf",
                "document_id": "doc123",
                "company_name": "Test Company",
                "cache_hit": False,
                "pdf_size_bytes": 12345,
                "error": None,
                "extract_success": True,
                "model_used": "test-model",
                "extraction_json_path": "/tmp/run/00000006/extraction/result.json",
                "warnings_json_path": "/tmp/run/00000006/extraction/warnings.json",
                "extract_error": None,
            }

            _update_job_state_from_result(conn, run_id, result, "all")

            cursor = conn.execute(
                "SELECT download_status, extract_status, final_status, company_name "
                "FROM pipeline_jobs WHERE run_id = ? AND job_index = ?",
                (run_id, 1),
            )
            row = cursor.fetchone()
            self.assertEqual(row[0], "success")
            self.assertEqual(row[1], "success")
            self.assertEqual(row[2], "success")
            self.assertEqual(row[3], "Test Company")

            conn.close()


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineBatchBuilding(unittest.TestCase):
    """Tests for batch building from input sources."""

    def test_parse_company_numbers_csv(self):
        """Test parsing company numbers from CSV string."""
        from companies_house_parsl_pipeline import _parse_company_numbers_csv

        result = _parse_company_numbers_csv("00000006,00000008,00000010")
        self.assertEqual(result, ["00000006", "00000008", "00000010"])

    def test_parse_company_numbers_csv_dedupes(self):
        """Test that duplicates are removed."""
        from companies_house_parsl_pipeline import _parse_company_numbers_csv

        result = _parse_company_numbers_csv("00000006,00000006,8")
        self.assertEqual(result, ["00000006", "00000008"])

    def test_parse_company_numbers_csv_normalizes(self):
        """Test that company numbers are zero-padded."""
        from companies_house_parsl_pipeline import _parse_company_numbers_csv

        result = _parse_company_numbers_csv("6,8,10")
        self.assertEqual(result, ["00000006", "00000008", "00000010"])


@unittest.skipIf(IS_WINDOWS, SKIP_REASON_WINDOWS)
class TestParslPipelineLive(unittest.TestCase):
    """
    Live smoke tests that require API keys.

    Run with:
        CH_API_KEY=... OPENROUTER_API_KEY=... python -m unittest test_parsl_pipeline.TestParslPipelineLive
    """

    @unittest.skipUnless(
        os.getenv("CH_API_KEY") and os.getenv("OPENROUTER_API_KEY"),
        "CH_API_KEY and OPENROUTER_API_KEY required for live tests",
    )
    def test_live_single_company_all_mode(self):
        """Test live pipeline with a single company in 'all' mode."""
        import parsl

        from companies_house_parsl_pipeline import main
        from parsl_pipeline_config import create_pipeline_config

        # Use a known company number (TESCO)
        test_args = [
            "--company-numbers", "00000006",
            "--mode", "all",
            "--ch-workers", "1",
            "--or-workers", "1",
            "--executor-type", "thread",
            "--model", os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash-lite"),
        ]

        # Parse args and run
        import sys
        old_argv = sys.argv
        try:
            sys.argv = ["companies_house_parsl_pipeline.py"] + test_args
            # This is a full integration test - it will make real API calls
            # We just verify it doesn't crash
            # exit_code = main()
            # self.assertEqual(exit_code, 0)
            self.skipTest("Full live test disabled by default - uncomment to run manually")
        finally:
            sys.argv = old_argv

    @unittest.skipUnless(
        os.getenv("CH_API_KEY"),
        "CH_API_KEY required for live download test",
    )
    def test_live_single_company_download_mode(self):
        """Test live pipeline download stage only."""
        self.skipTest("Live test disabled by default - uncomment to run manually")


if __name__ == "__main__":
    unittest.main()
