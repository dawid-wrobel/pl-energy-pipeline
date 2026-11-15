"""Client for the PSE Reports API v2 (https://api.raporty.pse.pl).

Downloads one business day of a report, validates it lightly and stores it as
a raw Parquet partition. Data is stored as received (local time strings, PSE
field names); all time normalisation happens later in dbt staging.

PSE conventions worth knowing:
- `business_date` is the trading day in Polish local time.
- `dtime` is the END of the interval, in local time (DST applies), so a day has
  96 quarter-hour rows, or 92/100 on the days the clocks change.
- OData filtering: `$filter=business_date eq 'YYYY-MM-DD'`, rows in `value`.

Usage from the shell (also works as a backfill over a date range):

    python -m ingestion.pse rce-pln 2026-03-29
    python -m ingestion.pse his-wlk-cal 2026-03-01 --end-date 2026-03-31
"""

import argparse
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urljoin
from zoneinfo import ZoneInfo

import pandas as pd

from ingestion import config
from ingestion.errors import NoDataError, UnexpectedResponseError
from ingestion.http import HttpClient
from ingestion.storage import write_partition

logger = logging.getLogger(__name__)

PSE_BASE_URL = "https://api.raporty.pse.pl/api"
SOURCE = "pse"
WARSAW = ZoneInfo("Europe/Warsaw")
MAX_PAGES = 50  # safety net against endless @odata.nextLink loops


class SupportsGetJson(Protocol):
    """Anything that can GET a URL and return decoded JSON (HttpClient or a test stub)."""

    def get_json(self, url: str, params: Mapping[str, Any] | None = None) -> Any: ...


@dataclass(frozen=True)
class PseReport:
    """Description of one PSE report: what to request and how to validate it."""

    name: str  # report name in the API path, e.g. "his-wlk-cal"
    dataset: str  # dataset name in the raw layer, e.g. "his_wlk_cal"
    fields: tuple[str, ...]  # fields requested with $select (also the stored columns)
    numeric_fields: tuple[str, ...]  # columns cast to float64 for a stable Parquet schema
    hourly_until: date | None = None  # last business date published with hourly resolution

    def intervals_per_hour(self, business_date: date) -> int:
        if self.hourly_until is not None and business_date <= self.hourly_until:
            return 1
        return 4


HIS_WLK_CAL = PseReport(
    name="his-wlk-cal",
    dataset="his_wlk_cal",
    fields=("dtime", "period", "pv", "wi"),
    numeric_fields=("pv", "wi"),
)

RCE_PLN = PseReport(
    name="rce-pln",
    dataset="rce_pln",
    fields=(
        "dtime",
        "period",
        "rce_pln",
        "dtime_utc",
        "period_utc",
        "business_date",
        "publication_ts",
        "publication_ts_utc",
    ),
    numeric_fields=("rce_pln",),
    hourly_until=date(2025, 9, 30),
)

REPORTS: dict[str, PseReport] = {report.name: report for report in (HIS_WLK_CAL, RCE_PLN)}


def build_url(report: PseReport, business_date: date, base_url: str = PSE_BASE_URL) -> str:
    """Build the OData request URL for one business day of a report."""
    select = quote(",".join(report.fields), safe="")
    odata_filter = quote(f"business_date eq '{business_date.isoformat()}'", safe="")
    return f"{base_url}/{report.name}?$select={select}&$filter={odata_filter}"


def expected_row_count(report: PseReport, business_date: date) -> int:
    """Number of rows a complete day should have (DST days have 23 or 25 hours)."""
    start = datetime.combine(business_date, time.min, tzinfo=WARSAW)
    end = datetime.combine(business_date + timedelta(days=1), time.min, tzinfo=WARSAW)
    hours = round((end.astimezone(UTC) - start.astimezone(UTC)) / timedelta(hours=1))
    return hours * report.intervals_per_hour(business_date)


def _extract_rows(payload: Any, url: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise UnexpectedResponseError(f"Expected an object with a 'value' list from {url}")
    return payload["value"]


def fetch_report(
    client: SupportsGetJson,
    report: PseReport,
    business_date: date,
    base_url: str = PSE_BASE_URL,
) -> list[dict[str, Any]]:
    """Download all rows of one business day, following OData paging links."""
    rows: list[dict[str, Any]] = []
    url = build_url(report, business_date, base_url)
    for _ in range(MAX_PAGES):
        payload = client.get_json(url)
        rows.extend(_extract_rows(payload, url))
        next_link = payload.get("@odata.nextLink")
        if not next_link:
            return rows
        url = urljoin(url, next_link)
    raise UnexpectedResponseError(f"More than {MAX_PAGES} pages for {report.name} {business_date}")


def rows_to_frame(
    rows: list[dict[str, Any]], report: PseReport, business_date: date
) -> pd.DataFrame:
    """Turn API rows into a DataFrame with the expected columns and stable dtypes."""
    if not rows:
        raise NoDataError(f"PSE returned no rows for {report.name} on {business_date}")

    df = pd.DataFrame(rows)
    missing = [column for column in report.fields if column not in df.columns]
    if missing:
        raise UnexpectedResponseError(f"{report.name}: missing fields {missing}")
    df = df.loc[:, list(report.fields)]

    for column in report.numeric_fields:
        df[column] = pd.to_numeric(df[column]).astype("float64")

    if "business_date" in df.columns:
        returned = df["business_date"].astype(str).str[:10]
        if (returned != business_date.isoformat()).any():
            raise UnexpectedResponseError(
                f"{report.name}: rows outside requested business_date {business_date}"
            )
    return df


def ingest_pse(
    client: SupportsGetJson,
    report: PseReport,
    business_date: date,
    *,
    root: Path | None = None,
) -> Path:
    """Download one day of a report and store it as a raw Parquet partition (idempotent)."""
    rows = fetch_report(client, report, business_date)
    df = rows_to_frame(rows, report, business_date)

    expected = expected_row_count(report, business_date)
    if len(df) != expected:
        logger.warning(
            "%s %s: got %d rows, expected %d (incomplete day or changed resolution)",
            report.name,
            business_date,
            len(df),
            expected,
        )

    return write_partition(
        df,
        root=root or config.raw_dir(),
        source=SOURCE,
        dataset=report.dataset,
        business_date=business_date,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download PSE report data into the raw layer.")
    parser.add_argument("report", choices=sorted(REPORTS))
    parser.add_argument("business_date", type=date.fromisoformat, help="YYYY-MM-DD")
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        help="last day (inclusive); downloads every day in the range",
    )
    args = parser.parse_args(argv)

    last = args.end_date or args.business_date
    if last < args.business_date:
        parser.error("--end-date must not be earlier than business_date")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    report = REPORTS[args.report]
    with HttpClient() as client:
        day = args.business_date
        while day <= last:
            print(ingest_pse(client, report, day))
            day += timedelta(days=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
