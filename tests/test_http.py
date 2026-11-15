"""Tests for HttpClient: retries, backoff, Retry-After, rate limiting, error types.

No real network and no real sleeping: a fake session returns queued responses
and a fake clock records requested sleeps.
"""

import json
from collections import deque
from datetime import UTC, datetime
from typing import Any

import pytest
import requests

from ingestion.http import (
    HttpClient,
    HttpClientError,
    NonRetryableHttpError,
    RetriesExhaustedError,
    parse_retry_after,
)


def make_response(
    status: int = 200,
    body: Any = None,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = (body if isinstance(body, str) else json.dumps(body)).encode()
    response.headers.update(headers or {})
    return response


class FakeSession:
    """Returns queued responses (or raises queued exceptions) and counts calls."""

    def __init__(self, *items: requests.Response | Exception) -> None:
        self._items = deque(items)
        self.calls = 0

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        self.calls += 1
        item = self._items.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    def close(self) -> None:
        pass


class FakeClock:
    """Monotonic clock that advances only when `sleep` is called."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(session: FakeSession, clock: FakeClock, **kwargs: Any) -> HttpClient:
    return HttpClient(
        session=session,  # type: ignore[arg-type]
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        jitter=lambda: 1.0,  # deterministic: no randomness in tests
        **kwargs,
    )


def test_returns_decoded_json_on_success() -> None:
    session = FakeSession(make_response(200, {"value": [1, 2]}))
    client = make_client(session, FakeClock())

    assert client.get_json("https://example.test/api") == {"value": [1, 2]}
    assert session.calls == 1


def test_retries_server_error_with_exponential_backoff() -> None:
    session = FakeSession(make_response(503, "x"), make_response(502, "x"), make_response(200, {}))
    clock = FakeClock()
    client = make_client(session, clock, backoff_base_s=1.0)

    client.get_json("https://example.test/api")

    assert session.calls == 3
    assert clock.sleeps == [1.0, 2.0]


def test_retries_connection_errors() -> None:
    session = FakeSession(requests.ConnectionError("boom"), make_response(200, {"ok": True}))
    clock = FakeClock()
    client = make_client(session, clock)

    assert client.get_json("https://example.test/api") == {"ok": True}
    assert len(clock.sleeps) == 1


def test_respects_retry_after_header_on_429() -> None:
    session = FakeSession(
        make_response(429, "slow down", {"Retry-After": "42"}), make_response(200, {})
    )
    clock = FakeClock()
    client = make_client(session, clock)

    client.get_json("https://example.test/api")

    assert clock.sleeps == [42.0]


def test_gives_up_when_retry_after_is_too_long() -> None:
    session = FakeSession(make_response(429, "slow down", {"Retry-After": "3600"}))
    clock = FakeClock()
    client = make_client(session, clock, max_retry_after_s=300.0)

    with pytest.raises(RetriesExhaustedError):
        client.get_json("https://example.test/api")
    assert clock.sleeps == []
    assert session.calls == 1


def test_does_not_retry_client_errors() -> None:
    session = FakeSession(make_response(404, "not found"))
    client = make_client(session, FakeClock())

    with pytest.raises(NonRetryableHttpError) as exc_info:
        client.get_json("https://example.test/api")

    assert exc_info.value.status_code == 404
    assert session.calls == 1


def test_raises_after_max_attempts() -> None:
    session = FakeSession(*[make_response(500, "x") for _ in range(3)])
    clock = FakeClock()
    client = make_client(session, clock, max_attempts=3)

    with pytest.raises(RetriesExhaustedError):
        client.get_json("https://example.test/api")

    assert session.calls == 3
    assert len(clock.sleeps) == 2  # no sleep after the last attempt


def test_min_interval_throttles_consecutive_requests() -> None:
    session = FakeSession(make_response(200, {}), make_response(200, {}))
    clock = FakeClock()
    client = make_client(session, clock, min_interval_s=30.0)

    client.get_json("https://example.test/api")
    client.get_json("https://example.test/api")

    assert clock.sleeps == [30.0]


def test_invalid_json_raises_client_error() -> None:
    session = FakeSession(make_response(200, "<html>not json</html>"))
    client = make_client(session, FakeClock())

    with pytest.raises(HttpClientError):
        client.get_json("https://example.test/api")


def test_invalid_max_attempts_is_rejected() -> None:
    with pytest.raises(ValueError):
        HttpClient(max_attempts=0)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("7", 7.0),
        ("2.5", 2.5),
        ("-3", 0.0),
        ("garbage", None),
        ("Wed, 21 Oct 2026 07:28:30 GMT", 30.0),
        ("Wed, 21 Oct 2026 07:27:00 GMT", 0.0),
    ],
)
def test_parse_retry_after(header: str | None, expected: float | None) -> None:
    now = datetime(2026, 10, 21, 7, 28, 0, tzinfo=UTC)
    assert parse_retry_after(header, now=now) == expected
