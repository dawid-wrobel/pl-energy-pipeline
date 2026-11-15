"""Tests for Parquet storage: layout, idempotency, atomicity and DuckDB readability."""

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ingestion.storage import partition_dir, write_partition

DAY = date(2026, 3, 29)
STAMP = datetime(2026, 11, 15, 10, 0, tzinfo=UTC)


def frame(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({"period": list(range(n)), "value": [float(i) for i in range(n)]})


def write(df: pd.DataFrame, root: Path, day: date = DAY) -> Path:
    return write_partition(
        df, root=root, source="pse", dataset="rce_pln", business_date=day, ingested_at=STAMP
    )


def test_writes_hive_style_path_and_adds_ingested_at(tmp_path: Path) -> None:
    path = write(frame(), tmp_path)

    assert path == tmp_path / "pse" / "rce_pln" / "business_date=2026-03-29" / "data.parquet"
    stored = pd.read_parquet(path)
    assert len(stored) == 3
    assert (stored["ingested_at"] == pd.Timestamp(STAMP)).all()


def test_rewriting_a_partition_replaces_data_instead_of_duplicating(tmp_path: Path) -> None:
    write(frame(3), tmp_path)
    path = write(frame(5), tmp_path)

    assert len(pd.read_parquet(path)) == 5
    assert len(list(partition_dir(tmp_path, "pse", "rce_pln", DAY).iterdir())) == 1


def test_partitions_are_independent(tmp_path: Path) -> None:
    write(frame(2), tmp_path, date(2026, 3, 28))
    write(frame(4), tmp_path, date(2026, 3, 29))

    assert len(pd.read_parquet(partition_dir(tmp_path, "pse", "rce_pln", date(2026, 3, 28)))) == 2
    assert len(pd.read_parquet(partition_dir(tmp_path, "pse", "rce_pln", date(2026, 3, 29)))) == 4


def test_empty_frame_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="empty"):
        write(pd.DataFrame({"value": []}), tmp_path)


def test_reserved_column_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="reserved"):
        write(frame().assign(ingested_at=1), tmp_path)


def test_duckdb_reads_partitions_with_business_date(tmp_path: Path) -> None:
    write(frame(2), tmp_path, date(2026, 3, 28))
    write(frame(4), tmp_path, date(2026, 3, 29))

    rows = duckdb.sql(
        f"""
        select business_date, count(*) as n
        from read_parquet('{tmp_path}/pse/rce_pln/*/data.parquet', hive_partitioning = true)
        group by business_date
        order by business_date
        """
    ).fetchall()

    assert rows == [(date(2026, 3, 28), 2), (date(2026, 3, 29), 4)]
