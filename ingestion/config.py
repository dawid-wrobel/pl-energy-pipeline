"""Central place for filesystem locations used by the pipeline.

Paths come from environment variables with sensible defaults, so the same code
runs locally (WSL), in Docker and in CI without changes.
"""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def data_dir() -> Path:
    """Root directory for all generated data (env: DATA_DIR, default: <repo>/data)."""
    return Path(os.environ.get("DATA_DIR", REPO_ROOT / "data"))


def raw_dir() -> Path:
    """Directory with raw Parquet files, partitioned by source/dataset/business_date."""
    return data_dir() / "raw"


def duckdb_path() -> Path:
    """DuckDB warehouse file (env: DUCKDB_PATH, default: <data>/warehouse.duckdb)."""
    return Path(os.environ.get("DUCKDB_PATH", data_dir() / "warehouse.duckdb"))
