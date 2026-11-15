"""Idempotent Parquet storage for the raw layer, partitioned by business_date.

Layout (Hive style, readable by DuckDB with `hive_partitioning=true`):

    <root>/<source>/<dataset>/business_date=YYYY-MM-DD/data.parquet

Each partition is exactly one file that is replaced atomically, so re-running
the same day (retry, backfill) never duplicates rows.
"""

import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PARTITION_FILE_NAME = "data.parquet"
INGESTED_AT_COLUMN = "ingested_at"


def partition_dir(root: Path, source: str, dataset: str, business_date: date) -> Path:
    """Directory holding one day of one dataset."""
    return root / source / dataset / f"business_date={business_date.isoformat()}"


def write_partition(
    df: pd.DataFrame,
    *,
    root: Path,
    source: str,
    dataset: str,
    business_date: date,
    ingested_at: datetime | None = None,
) -> Path:
    """Write `df` as the single file of a partition, replacing any previous version.

    Adds an `ingested_at` (UTC) column used for data lineage and for the
    "data retrieved on" attribution required by the data providers.
    """
    if df.empty:
        raise ValueError(
            f"Refusing to write an empty partition: {source}/{dataset} {business_date}"
        )
    if INGESTED_AT_COLUMN in df.columns:
        raise ValueError(f"Column '{INGESTED_AT_COLUMN}' is reserved for the storage layer")

    target_dir = partition_dir(root, source, dataset, business_date)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / PARTITION_FILE_NAME
    tmp = target_dir / f".{PARTITION_FILE_NAME}.tmp"

    stamped = df.assign(**{INGESTED_AT_COLUMN: ingested_at or datetime.now(UTC)})
    try:
        stamped.to_parquet(tmp, engine="pyarrow", compression="zstd", index=False)
        os.replace(tmp, target)  # atomic: readers never see a half-written file
    finally:
        tmp.unlink(missing_ok=True)

    logger.info("Wrote %d rows to %s", len(df), target)
    return target
