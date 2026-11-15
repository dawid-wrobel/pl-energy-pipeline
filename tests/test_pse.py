"""Tests for the PSE client using synthetic API responses (no network).

The payloads below imitate the documented shape of PSE API v2 (`value` list,
OData paging link); they are not real PSE data.
"""

import logging
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from ingestion import pse
from ingestion.errors import NoDataError, UnexpectedResponseError
from ingestion.pse import HIS_WLK_CAL, RCE_PLN

DAY = date(2026, 3, 1)


def rce_row(index: int, business_date: str = "2026-03-01") -> dict[str, Any]:
    return {
        "dtime": f"{business_date} 00:{index:02d}:00",
        "period": f"00:{index:02d} - 00:{index + 15:02d}",
        "rce_pln": 400.0 + index,
        "dtime_utc": f"{business_date} 00:{index:02d}:00",
        "period_utc": f"00:{index:02d} - 00:{index + 15:02d}",
        "business_date": business_date,
        "publication_ts": "2026-02-28 13:00:00",
        "publication_ts_utc": "2026-02-28 12:00:00",
    }


def wind_solar_row(index: int) -> dict[str, Any]:
    return {
        "dtime": f"2026-03-01 00:{index:02d}:00",
        "period": f"00:{index:02d} - 00:{index + 15:02d}",
        "pv": 0,
        "wi": 1000 + index,
    }


class StubClient:
    """Returns queued payloads and records requested URLs."""

    def __init__(self, *payloads: Any) -> None:
        self._payloads = list(payloads)
        self.urls: list[str] = []

    def get_json(self, url: str, params: Mapping[str, Any] | None = None) -> Any:
        self.urls.append(url)
        return self._payloads.pop(0)

    def __enter__(self) -> "StubClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None


def test_build_url_uses_odata_select_and_filter() -> None:
    assert pse.build_url(HIS_WLK_CAL, date(2025, 9, 12)) == (
        "https://api.raporty.pse.pl/api/his-wlk-cal"
        "?$select=dtime%2Cperiod%2Cpv%2Cwi"
        "&$filter=business_date%20eq%20%272025-09-12%27"
    )


def test_fetch_report_returns_rows_from_value() -> None:
    client = StubClient({"value": [rce_row(0), rce_row(15)]})

    rows = pse.fetch_report(client, RCE_PLN, DAY)

    assert len(rows) == 2
    assert len(client.urls) == 1


def test_fetch_report_follows_next_link() -> None:
    next_url = "https://api.raporty.pse.pl/api/rce-pln?$skiptoken=abc"
    client = StubClient(
        {"value": [rce_row(0)], "@odata.nextLink": next_url},
        {"value": [rce_row(15)]},
    )

    rows = pse.fetch_report(client, RCE_PLN, DAY)

    assert len(rows) == 2
    assert client.urls[1] == next_url


def test_fetch_report_stops_on_endless_paging() -> None:
    link = "https://api.raporty.pse.pl/api/rce-pln?$skiptoken=loop"
    client = StubClient(*[{"value": [rce_row(0)], "@odata.nextLink": link}] * pse.MAX_PAGES)

    with pytest.raises(UnexpectedResponseError, match="pages"):
        pse.fetch_report(client, RCE_PLN, DAY)


@pytest.mark.parametrize("payload", [[], {"value": "x"}, {"error": "boom"}])
def test_fetch_report_rejects_unexpected_payload(payload: Any) -> None:
    with pytest.raises(UnexpectedResponseError):
        pse.fetch_report(StubClient(payload), RCE_PLN, DAY)


def test_rows_to_frame_keeps_expected_columns_and_casts_numbers() -> None:
    rows = [{**wind_solar_row(0), "extra": "ignored"}, wind_solar_row(15)]

    df = pse.rows_to_frame(rows, HIS_WLK_CAL, DAY)

    assert list(df.columns) == ["dtime", "period", "pv", "wi"]
    assert df["pv"].dtype == "float64"
    assert df["wi"].dtype == "float64"


def test_rows_to_frame_rejects_empty_input() -> None:
    with pytest.raises(NoDataError):
        pse.rows_to_frame([], RCE_PLN, DAY)


def test_rows_to_frame_rejects_missing_fields() -> None:
    with pytest.raises(UnexpectedResponseError, match="missing"):
        pse.rows_to_frame([{"dtime": "x"}], RCE_PLN, DAY)


def test_rows_to_frame_rejects_rows_from_another_business_date() -> None:
    with pytest.raises(UnexpectedResponseError, match="outside"):
        pse.rows_to_frame([rce_row(0, business_date="2026-03-02")], RCE_PLN, DAY)


@pytest.mark.parametrize(
    ("report", "day", "expected"),
    [
        (HIS_WLK_CAL, date(2026, 3, 1), 96),
        (HIS_WLK_CAL, date(2026, 3, 29), 92),  # spring forward: 23 hours
        (HIS_WLK_CAL, date(2026, 10, 25), 100),  # fall back: 25 hours
        (RCE_PLN, date(2025, 9, 30), 24),  # hourly product
        (RCE_PLN, date(2025, 10, 1), 96),  # 15-minute product
    ],
)
def test_expected_row_count(report: pse.PseReport, day: date, expected: int) -> None:
    assert pse.expected_row_count(report, day) == expected


def test_ingest_pse_writes_partition_and_is_idempotent(tmp_path: Path) -> None:
    payload = {"value": [rce_row(0), rce_row(15)]}

    first = pse.ingest_pse(StubClient(payload), RCE_PLN, DAY, root=tmp_path)
    second = pse.ingest_pse(StubClient(payload), RCE_PLN, DAY, root=tmp_path)

    assert first == second
    assert first == tmp_path / "pse" / "rce_pln" / "business_date=2026-03-01" / "data.parquet"
    stored = pd.read_parquet(first)
    assert len(stored) == 2
    assert "ingested_at" in stored.columns


def test_ingest_pse_warns_when_row_count_is_unexpected(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING, logger="ingestion.pse"):
        pse.ingest_pse(StubClient({"value": [rce_row(0)]}), RCE_PLN, DAY, root=tmp_path)

    assert "expected 96" in caplog.text


def test_main_downloads_a_range_of_days(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    stub = StubClient(
        {"value": [rce_row(0, "2026-03-01")]},
        {"value": [rce_row(0, "2026-03-02")]},
    )
    monkeypatch.setattr(pse, "HttpClient", lambda: stub)

    exit_code = pse.main(["rce-pln", "2026-03-01", "--end-date", "2026-03-02"])

    assert exit_code == 0
    assert (tmp_path / "raw/pse/rce_pln/business_date=2026-03-01/data.parquet").exists()
    assert (tmp_path / "raw/pse/rce_pln/business_date=2026-03-02/data.parquet").exists()
    assert capsys.readouterr().out.count("data.parquet") == 2
