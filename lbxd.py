"""lbxd — a standalone Python client for the Letterboxd API.

Signs requests itself (HMAC-SHA256 per the Letterboxd API spec) — no
dependency on the abandoned ``letterboxd`` wrapper package.

Public surface (stable since v0.x; downstream code relies on these contracts):

* ``api_request(path)`` — one signed GET. Returns the ``requests.Response`` on
  2xx and raises ``requests.HTTPError`` (with ``.response`` attached) on any
  other status. Network failures raise ``requests.RequestException``;
  timeouts raise ``requests.Timeout``.
* ``get_id_from_username(name)`` — resolve a username to an API member id.
  Raises ``LbxdNotFound`` (a ``ValueError`` subclass) for unknown users and
  ``LbxdTransientError`` for upstream failures, so callers can tell a bad
  username from an outage.
* ``get_member_watches(member_id)`` — DataFrame of (member, film, rating).
* ``get_member_watchlist(member_id)`` / ``get_combined_watchlists(ids)`` —
  DataFrame(s) of raw watchlist items.
* ``get_member_info(member_id)`` — raw member dict.
* ``threaded_api_request(urls, ...)`` — concurrent GETs with retry/backoff.
  404s fail fast into ``missing_urls``; 429s honour ``Retry-After``.
* ``encode_id`` / ``decode_id`` — internal <-> external Letterboxd ids.

Credentials come from the ``LBXD_KEY`` / ``LBXD_SECRET`` environment
variables (or pass them to ``Client`` explicitly).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import base62
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

__version__ = "1.0.0"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

API_BASE = "https://api.letterboxd.com/api/v0"
WWW_BASE = "https://letterboxd.com"
HEADERS = {"user-agent": f"toolboxd-lbxd/{__version__}"}

# (connect, read) seconds — applied to every request this module makes.
DEFAULT_TIMEOUT = (5.0, 30.0)

# Sized ~2x the default worker count in threaded_api_request.
POOL_SIZE = 100

# How many Retry-After waits a single URL may burn in threaded_api_request
# before it is declared failed (separate from the error-retry budget, which
# would otherwise let a persistent rate limit loop forever).
RATE_LIMIT_MAX_WAITS = 10

# Letterboxd usernames are letters/digits/underscores; hyphens tolerated for
# legacy accounts. Anything else would be path injection into the lookup URL.
_USERNAME_RE = re.compile(r"[A-Za-z0-9_-]{1,32}")


class LbxdError(Exception):
    """Base class for errors raised by this module."""


class LbxdNotFound(LbxdError, ValueError):
    """The requested member/resource does not exist (HTTP 404).

    Subclasses ``ValueError`` because callers historically caught that from
    ``get_id_from_username`` to mean "no such user".
    """


class LbxdInvalidUsername(LbxdNotFound):
    """The username contains characters Letterboxd usernames can't have."""


class LbxdTransientError(LbxdError):
    """A transient upstream failure (5xx, rate limit, network) — not a 404.

    Deliberately *not* a ``ValueError``: an outage must never be reported to
    users as "no such username".
    """


class Client:
    """Signed HTTP client for the Letterboxd API.

    Thread-safe for concurrent GETs: the underlying ``requests.Session``
    connection pool is shared and no per-request state is stored on ``self``.
    """

    def __init__(self, api_key=None, api_secret=None, timeout=DEFAULT_TIMEOUT,
                 session=None):
        self.api_key = api_key or os.environ.get("LBXD_KEY")
        self.api_secret = api_secret or os.environ.get("LBXD_SECRET")
        if not self.api_key or not self.api_secret:
            raise LbxdError(
                "Letterboxd API credentials missing: set the LBXD_KEY and "
                "LBXD_SECRET environment variables (or pass api_key/api_secret)."
            )
        self.timeout = timeout
        self.session = session or requests.Session()
        adapter = HTTPAdapter(pool_connections=POOL_SIZE, pool_maxsize=POOL_SIZE)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, path: str) -> requests.Response:
        """One signed GET of an API path (which may carry its own query string).

        Returns the ``Response`` on 2xx; raises ``requests.HTTPError`` with
        ``.response`` attached otherwise.
        """
        url = f"{API_BASE}/{path}"
        params = {
            "apikey": self.api_key,
            "nonce": str(uuid.uuid4()),
            "timestamp": int(time.time()),
        }
        request = requests.Request("GET", url, params=params, headers=HEADERS)
        prepared = self.session.prepare_request(request)
        signature = self._sign(prepared.method, prepared.url, prepared.body)
        prepared.prepare_url(prepared.url, {"signature": signature})
        response = self.session.send(prepared, timeout=self.timeout)
        response.raise_for_status()
        return response

    def _sign(self, method: str, url: str, body=None) -> str:
        """HMAC-SHA256 of ``[METHOD]\\x00[URL]\\x00[BODY]`` with the API secret.

        The URL must already include apikey/nonce/timestamp; the resulting
        hex digest is appended as the final ``signature`` query parameter.
        """
        if body is None:
            body = b""
        elif not isinstance(body, bytes):
            body = str(body).encode()
        message = b"\x00".join([method.upper().encode(), url.encode(), body])
        return hmac.new(self.api_secret.encode(), message, hashlib.sha256).hexdigest()


# Module-level singleton so the whole process shares one connection pool.
_client: Client | None = None
_client_lock = threading.Lock()


def _get_client() -> Client:
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = Client()
    return _client


def api_request(path: str) -> requests.Response:
    """Signed GET against the Letterboxd API.

    Parameters
    ----------
    path : str
        The API path (may include a query string), e.g.
        ``"member/abc123/watchlist?perPage=100"``.

    Returns
    -------
    requests.Response
        The response on 2xx. Any other status raises ``requests.HTTPError``
        with the response attached; network errors raise
        ``requests.RequestException``.
    """
    return _get_client().get(path)


def get_id_from_username(member_name: str) -> str:
    """Resolve a Letterboxd username to its API member id.

    The API takes member ids, not usernames, so this reads the
    ``X-Letterboxd-Identifier`` header from the member's profile page on
    the main site. A streamed GET is used because the CDN rejects HEAD
    (403 as of 2026-07); streaming means the page body is never downloaded.

    Raises
    ------
    LbxdInvalidUsername
        If ``member_name`` isn't a possible Letterboxd username.
    LbxdNotFound
        If no member exists with that username (HTTP 404).
    LbxdTransientError
        For rate limits, 5xx, or other upstream failures — the username may
        be perfectly valid.
    """
    if not isinstance(member_name, str) or not _USERNAME_RE.fullmatch(member_name):
        raise LbxdInvalidUsername(f"{member_name!r} is not a valid Letterboxd username")

    url = f"{WWW_BASE}/{member_name}/"
    response = requests.get(url, headers=HEADERS, timeout=DEFAULT_TIMEOUT,
                            allow_redirects=True, stream=True)
    response.close()  # headers only; never pull the page body

    if response.status_code == 404:
        raise LbxdNotFound(f"No Letterboxd member found for username {member_name!r}")
    if not response.ok:
        raise LbxdTransientError(
            f"Username lookup for {member_name!r} failed with HTTP "
            f"{response.status_code}; the username may still be valid."
        )

    member_id = response.headers.get("X-Letterboxd-Identifier")
    if not member_id:
        raise LbxdTransientError(
            "Profile response carried no X-Letterboxd-Identifier header; "
            "possible upstream change."
        )
    return member_id


def get_member_watchlist(member_id: str) -> pd.DataFrame:
    """Return a member's watchlist as a DataFrame of raw API film items.

    An empty watchlist yields an empty DataFrame with an ``id`` column, so
    ``df["id"]`` is always safe.
    """
    items = []
    cursor = "start=0"
    while True:
        response = api_request(f"member/{member_id}/watchlist?perPage=100&cursor={cursor}")
        payload = response.json()
        items.extend(payload["items"])
        cursor = payload.get("next")
        if not cursor:
            break
    if not items:
        return pd.DataFrame(columns=["id"])
    return pd.DataFrame(items)


def get_combined_watchlists(member_ids) -> pd.DataFrame:
    """Return the concatenated watchlists of several members."""
    frames = [get_member_watchlist(member_id) for member_id in member_ids]
    if not frames:
        return pd.DataFrame(columns=["id"])
    return pd.concat(frames, ignore_index=True)


def get_member_watches(member_id: str) -> pd.DataFrame:
    """Return a member's watched films and ratings.

    Returns
    -------
    pd.DataFrame
        Columns ``member``, ``film``, ``rating`` (NaN where the film was
        watched but not rated). The columns are present even when the
        member has no watches.
    """
    rows = []
    cursor = "start=0"
    while True:
        response = api_request(
            f"films/?perPage=100&member={member_id}"
            f"&memberRelationship=Watched&sort=MemberRatingHighToLow&cursor={cursor}"
        )
        payload = response.json()
        for item in payload["items"]:
            rating = None
            relationships = item.get("relationships") or []
            if relationships:
                relationship = relationships[0].get("relationship") or {}
                rating = relationship.get("rating")
            rows.append({"member": member_id, "film": item.get("id"), "rating": rating})
        cursor = payload.get("next")
        if not cursor:
            break
    return pd.DataFrame(rows, columns=["member", "film", "rating"])


def get_member_info(member_id: str) -> dict:
    """Return a member's raw info dict from the API."""
    return api_request(f"member/{member_id}").json()


def _retry_after_seconds(response, fallback: float) -> float:
    """Parse a Retry-After header (delta-seconds form); fall back if absent/odd."""
    value = response.headers.get("Retry-After") if response is not None else None
    if value:
        try:
            return max(float(value), 0.0)
        except (TypeError, ValueError):
            pass
    return fallback


def threaded_api_request(url_list, max_retries=5, max_threads=50, print_every=1000,
                         preserve_order=True, base_delay=1.0, respect_retry_after=True):
    """Fetch a list of API paths concurrently, with retry and backoff.

    404s are *not* retried: they go straight to ``missing_urls``. 429s honour
    ``Retry-After`` (when ``respect_retry_after``) on a separate budget of
    ``RATE_LIMIT_MAX_WAITS`` waits. 5xx and network errors are retried up to
    ``max_retries`` times with exponential backoff.

    Parameters
    ----------
    url_list : list
        API paths to fetch (as accepted by ``api_request``).
    max_retries : int, optional
        Retries per URL for 5xx/network errors. Defaults to 5.
    max_threads : int, optional
        Worker thread count. Defaults to 50.
    print_every : int, optional
        Log progress every N completed URLs. Defaults to 1000.
    preserve_order : bool, optional
        If True, successful results come back in ``url_list`` order
        (missing/failed URLs are omitted). Defaults to True.
    base_delay : float, optional
        Base for exponential backoff, in seconds. Defaults to 1.0.
    respect_retry_after : bool, optional
        Honour 429 ``Retry-After`` headers. Defaults to True.

    Returns
    -------
    tuple
        ``(all_results, missing_urls, failed_urls)`` —
        parsed-JSON results, URLs that 404ed, and
        ``{"url", "reason"}`` dicts for URLs that exhausted retries.
    """
    results_dict = {}
    missing_urls = []
    failed_urls = []

    def fetch(url, index):
        retries = 0
        rate_limit_waits = 0
        while True:
            last_error = None
            try:
                response = api_request(url)
                return index, response.json()
            except requests.HTTPError as exc:
                resp = exc.response
                status = resp.status_code if resp is not None else None
                if status == 404:
                    missing_urls.append(url)
                    return index, None
                if status == 429:
                    rate_limit_waits += 1
                    if rate_limit_waits <= RATE_LIMIT_MAX_WAITS:
                        if respect_retry_after:
                            delay = _retry_after_seconds(
                                resp, base_delay * (2 ** min(rate_limit_waits, 6)))
                        else:
                            delay = base_delay * (2 ** min(rate_limit_waits, 6))
                        time.sleep(delay)
                        continue
                    last_error = f"HTTP 429 (rate limited, {rate_limit_waits - 1} waits)"
                else:
                    last_error = f"HTTP {status}"
            except requests.RequestException as exc:
                last_error = repr(exc)

            retries += 1
            if retries > max_retries:
                failed_urls.append({"url": url, "reason": last_error})
                log.warning("URL failed after %d retries: %s (%s)",
                            max_retries, url, last_error)
                return index, None
            time.sleep(base_delay * (2 ** min(retries, 6)))

    log.info("Fetching %d URLs with %d threads...", len(url_list), max_threads)
    completed = 0
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(fetch, url, idx) for idx, url in enumerate(url_list)]
        for future in as_completed(futures):
            index, entry = future.result()
            if entry is not None:
                results_dict[index] = entry
            completed += 1
            if print_every and completed % print_every == 0:
                log.info("%d/%d URLs processed.", completed, len(url_list))
    log.info("Complete: %d successful, %d missing, %d failed.",
             len(results_dict), len(missing_urls), len(failed_urls))

    if preserve_order:
        all_results = [results_dict[i] for i in sorted(results_dict)]
    else:
        all_results = list(results_dict.values())

    return all_results, missing_urls, failed_urls


def encode_id(internal_id: int, is_user: bool = False) -> str:
    """Encode a Letterboxd internal numeric id to the external base62 form.

    The external id is what the API uses for both members and films. Member
    ids carry a check digit of 7; film ids a check digit of 0.
    """
    if is_user:
        return base62.encode((internal_id * 10) + 7, charset=base62.CHARSET_INVERTED)
    return base62.encode(internal_id * 10, charset=base62.CHARSET_INVERTED)


def decode_id(external_id: str) -> int:
    """Decode an external base62 Letterboxd id back to the internal numeric id."""
    return base62.decode(external_id, charset=base62.CHARSET_INVERTED) // 10
