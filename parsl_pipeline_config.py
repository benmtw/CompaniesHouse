"""
Parsl configuration factory for Companies House extraction pipeline.

Supports both ThreadPoolExecutor (local) and HighThroughputExecutor (distributed/cluster).

NOTE: Parsl requires fork-based multiprocessing and is NOT compatible with Windows.
"""

from __future__ import annotations

import platform
from pathlib import Path
from typing import Any

# Windows compatibility check
if platform.system() == "Windows":
    raise ImportError(
        "Parsl is not compatible with Windows due to fork-based multiprocessing. "
        "Use WSL2, Linux, or macOS."
    )

from parsl import Config
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from parsl.providers import LocalProvider


def create_pipeline_config(
    ch_workers: int = 2,
    or_workers: int = 4,
    executor_type: str = "thread",
    monitoring_enabled: bool = False,
    run_dir: str | Path | None = None,
    app_cache_enabled: bool = True,
    max_idletime: float = 120.0,
) -> Config:
    """
    Create a Parsl Config with configurable executor types.

    Args:
        ch_workers: Max workers for download executor (Companies House API)
        or_workers: Max workers for extract executor (OpenRouter LLM)
        executor_type: "thread" for local ThreadPoolExecutor, "htex" for HighThroughputExecutor
        monitoring_enabled: Enable Parsl monitoring hub for visualization
        run_dir: Directory for Parsl run logs (default: output/parsl_runs)
        app_cache_enabled: Enable Parsl memoization for task results
        max_idletime: Seconds before idle workers shutdown (htex only)

    Returns:
        Configured Parsl Config object ready for parsl.load()
    """
    if executor_type not in ("thread", "htex"):
        raise ValueError(f"executor_type must be 'thread' or 'htex', got {executor_type!r}")

    if ch_workers < 1:
        raise ValueError(f"ch_workers must be >= 1, got {ch_workers}")
    if or_workers < 1:
        raise ValueError(f"or_workers must be >= 1, got {or_workers}")

    run_dir_path = Path(run_dir) if run_dir else Path("output/parsl_runs")

    if executor_type == "htex":
        download_executor = HighThroughputExecutor(
            label="download_executor",
            max_workers=ch_workers,
            provider=LocalProvider(),
            max_idletime=max_idletime,
        )
        extract_executor = HighThroughputExecutor(
            label="extract_executor",
            max_workers=or_workers,
            provider=LocalProvider(),
            max_idletime=max_idletime,
        )
    else:  # thread (default)
        download_executor = ThreadPoolExecutor(
            label="download_executor",
            max_threads=ch_workers,
        )
        extract_executor = ThreadPoolExecutor(
            label="extract_executor",
            max_threads=or_workers,
        )

    config_kwargs: dict[str, Any] = {
        "executors": [download_executor, extract_executor],
        "usage_tracking": False,
        "app_cache": app_cache_enabled,
        "run_dir": str(run_dir_path),
    }

    if monitoring_enabled:
        try:
            from parsl.monitoring import MonitoringHub
        except ImportError as exc:
            raise ImportError(
                "Parsl monitoring requires additional dependencies. "
                "Install with: pip install parsl[monitoring]"
            ) from exc

        config_kwargs["monitoring"] = MonitoringHub(
            hub_address="localhost",
            workflow_name="companies_house_extraction",
        )

    return Config(**config_kwargs)


__all__ = ["create_pipeline_config"]
