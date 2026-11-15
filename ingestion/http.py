"""Resilient HTTP client shared by all API clients.

Adds what bare `requests` lacks: default timeouts, retries with exponential
backoff and jitter, `Retry-After` handling for HTTP 429/503, an optional
minimum interval between requests (client-side rate limiting) and clear error
types. Sleeping and time are injectable, so tests run instantly.
"""

import logging
import random
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
DEFAULT_USER_AGENT = "pl-energy-pipeline/0.1 (+https://github.com/dawid-wrobel/pl-energy-pipeline)"


class HttpClientError(Exception):
    """Base class for all errors raised by HttpClient."""


class NonRetryableHttpError(HttpClientError):
    """The server returned an error that retrying cannot fix (for example 400 or 404)."""

    def __init__(self, url: str, status_code: int, body: str) -> None:
        super().__init__(f"HTTP {status_code} for {url}: {body}")
        self.url = url
        self.status_code = status_code
        self.body = body


class RetriesExhaustedError(HttpClientError):
    """All attempts failed, or the server asked us to wait unreasonably long."""


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Parse a Retry-After header (seconds or HTTP-date) into seconds to wait."""
    if not value:
        return None
    text = value.strip()
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - (now or datetime.now(UTC))).total_seconds())


class HttpClient:
    """Thin wrapper over requests.Session with retries, backoff and rate limiting."""

    def __init__(
        self,
        *,
        max_attempts: int = 5,
        backoff_base_s: float = 1.0,
        backoff_max_s: float = 60.0,
        max_retry_after_s: float = 300.0,
        min_interval_s: float = 0.0,
        timeout: tuple[float, float] = (5.0, 30.0),
        user_agent: str = DEFAULT_USER_AGENT,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._max_attempts = max_attempts
        self._backoff_base_s = backoff_base_s
        self._backoff_max_s = backoff_max_s
        self._max_retry_after_s = max_retry_after_s
        self._min_interval_s = min_interval_s
        self._timeout = timeout
        self._headers = {"User-Agent": user_agent, "Accept": "application/json"}
        self._session = session or requests.Session()
        self._sleep = sleep
        self._monotonic = monotonic
        self._jitter = jitter
        self._last_request_at: float | None = None

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self._session.close()

    def get_json(self, url: str, params: Mapping[str, Any] | None = None) -> Any:
        """GET `url` and return the decoded JSON body."""
        response = self._get(url, params)
        try:
            return response.json()
        except ValueError as exc:
            raise HttpClientError(f"Invalid JSON returned by {url}: {exc}") from exc

    def _get(self, url: str, params: Mapping[str, Any] | None) -> requests.Response:
        last_error = ""
        for attempt in range(1, self._max_attempts + 1):
            self._throttle()
            try:
                response = self._session.get(
                    url, params=params, headers=self._headers, timeout=self._timeout
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                delay = self._backoff_delay(attempt)
            else:
                status = response.status_code
                if status < 400:
                    return response
                if status not in RETRYABLE_STATUS:
                    raise NonRetryableHttpError(url, status, response.text[:300])
                last_error = f"HTTP {status}"
                delay = self._retry_delay(url, response, attempt)

            if attempt == self._max_attempts:
                break
            logger.warning(
                "Attempt %d/%d for %s failed (%s); retrying in %.1fs",
                attempt,
                self._max_attempts,
                url,
                last_error,
                delay,
            )
            self._sleep(delay)

        raise RetriesExhaustedError(
            f"{url} failed after {self._max_attempts} attempts: {last_error}"
        )

    def _retry_delay(self, url: str, response: requests.Response, attempt: int) -> float:
        retry_after = parse_retry_after(response.headers.get("Retry-After"))
        if retry_after is None:
            return self._backoff_delay(attempt)
        if retry_after > self._max_retry_after_s:
            raise RetriesExhaustedError(
                f"{url} asked to wait {retry_after:.0f}s (limit {self._max_retry_after_s:.0f}s)"
            )
        return retry_after

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with 'equal jitter': half fixed, half random."""
        ceiling = min(self._backoff_max_s, self._backoff_base_s * 2 ** (attempt - 1))
        return ceiling * (0.5 + 0.5 * self._jitter())

    def _throttle(self) -> None:
        """Enforce the minimum interval between the starts of two requests."""
        if self._min_interval_s > 0 and self._last_request_at is not None:
            wait = self._min_interval_s - (self._monotonic() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._monotonic()
