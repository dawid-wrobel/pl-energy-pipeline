"""Smoke test: every pinned dependency can be imported in this environment."""

import importlib

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "requests",
        "pandas",
        "pyarrow",
        "duckdb",
        "dbt.version",
        "dagster",
        "dagster_dbt",
        "streamlit",
        "ingestion",
        "orchestration",
        "dashboard",
    ],
)
def test_module_is_importable(module: str) -> None:
    importlib.import_module(module)
