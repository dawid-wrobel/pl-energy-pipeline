"""Tests for path configuration: defaults and environment overrides."""

from pathlib import Path

import pytest

from ingestion import config


def test_defaults_live_under_repo_data_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    assert config.data_dir() == config.REPO_ROOT / "data"
    assert config.raw_dir() == config.REPO_ROOT / "data" / "raw"
    assert config.duckdb_path() == config.REPO_ROOT / "data" / "warehouse.duckdb"


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "custom.duckdb"))

    assert config.raw_dir() == tmp_path / "raw"
    assert config.duckdb_path() == tmp_path / "custom.duckdb"
