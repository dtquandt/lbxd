"""Contract tests for lbxd.

These lock down the public surface that letterboxd-collector,
letterboxd-recommender, and toolboxd-web depend on:

* api_request returns the Response on 2xx and raises requests.HTTPError
  (with .response attached) otherwise.
* get_id_from_username raises a ValueError subclass for unknown/invalid
  usernames and a non-ValueError for transient failures.
* DataFrame shapes from get_member_watches / get_member_watchlist.
* threaded_api_request's (results, missing, failed) tuple, fast-404s,
  and Retry-After handling.
* Request signing (apikey/nonce/timestamp params + HMAC-SHA256 signature).

All HTTP is faked at the Session.send seam; no network involved.
"""

import hashlib
import hmac
import json
from urllib.parse import parse_qs, urlsplit, urlunsplit

import pandas as pd
import pytest
import requests

import lbxd


def make_response(status=200, payload=None, headers=None, url="https://api.test/"):
    resp = requests.Response()
    resp.status_code = status
    resp.url = url
    resp._content = json.dumps(payload if payload is not None else {}).encode()
    resp._content_consumed = True  # lets .close() work without a raw stream
    resp.headers.update(headers or {})
    return resp


class FakeApi:
    """Fake transport behind a real Client: queue responses per path substring."""

    def __init__(self, monkeypatch):
        self.client = lbxd.Client(api_key="test-key", api_secret="test-secret")
        self.routes = []  # (substring, [responses...])
        self.sent = []  # prepared requests, in order
        monkeypatch.setattr(self.client.session, "send", self._send)
        monkeypatch.setattr(lbxd, "_client", self.client)

    def route(self, substring, *responses):
        self.routes.append((substring, list(responses)))

    def route_pages(self, substring, total_items, id_prefix="f", item_extra=None):
        """Serve a paginated listing keyed by the request's cursor=start=N."""
        def handler(prepared):
            q = parse_qs(urlsplit(prepared.url).query)
            start = int(q.get("cursor", ["start=0"])[0].split("=", 1)[1])
            items = [
                {"id": f"{id_prefix}{i}", **(item_extra(i) if item_extra else {})}
                for i in range(start, min(start + 100, total_items))
            ]
            payload = {"items": items}
            if start + 100 < total_items:
                payload["next"] = f"start={start + 100}"
            return make_response(200, payload)

        self.routes.append((substring, handler))

    def _send(self, prepared, timeout=None, **kwargs):
        assert timeout is not None, "every request must carry a timeout"
        self.sent.append(prepared)
        for substring, responses in self.routes:
            if substring in prepared.url:
                if callable(responses):
                    resp = responses(prepared)
                else:
                    resp = responses.pop(0) if len(responses) > 1 else responses[0]
                resp.request = prepared
                return resp
        raise AssertionError(f"no fake route for {prepared.url}")


@pytest.fixture
def fake_api(monkeypatch):
    return FakeApi(monkeypatch)


@pytest.fixture
def no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(lbxd.time, "sleep", slept.append)
    return slept


# ---------------------------------------------------------------- signing


def test_requests_are_signed_correctly(fake_api):
    fake_api.route("member/abc", make_response(200, {"id": "abc"}))
    lbxd.api_request("member/abc")

    (prepared,) = fake_api.sent
    scheme, netloc, path, query, frag = urlsplit(prepared.url)
    params = parse_qs(query)

    assert params["apikey"] == ["test-key"]
    assert "nonce" in params and "timestamp" in params
    # Signature must be the HMAC-SHA256 of METHOD \x00 URL-without-signature \x00 body
    unsigned_query = query[: query.rindex("&signature=")]
    unsigned_url = urlunsplit((scheme, netloc, path, unsigned_query, frag))
    expected = hmac.new(
        b"test-secret", b"\x00".join([b"GET", unsigned_url.encode(), b""]), hashlib.sha256
    ).hexdigest()
    assert params["signature"] == [expected]
    assert query.endswith(f"signature={expected}"), "signature must be the final param"


def test_nonce_unique_per_request(fake_api):
    fake_api.route("member/abc", make_response(200, {}))
    lbxd.api_request("member/abc")
    lbxd.api_request("member/abc")
    nonces = [parse_qs(urlsplit(p.url).query)["nonce"][0] for p in fake_api.sent]
    assert nonces[0] != nonces[1]


def test_path_query_strings_survive(fake_api):
    fake_api.route("watchlist", make_response(200, {"items": []}))
    lbxd.get_member_watchlist("abc")
    query = parse_qs(urlsplit(fake_api.sent[0].url).query)
    assert query["perPage"] == ["100"]
    assert query["cursor"] == ["start=0"]


# ---------------------------------------------------------------- api_request


def test_api_request_returns_response_on_200(fake_api):
    fake_api.route("member/abc", make_response(200, {"id": "abc"}))
    resp = lbxd.api_request("member/abc")
    assert resp.json() == {"id": "abc"}


def test_api_request_raises_httperror_with_response(fake_api):
    """The collector's Client depends on exactly this contract."""
    fake_api.route("member/gone", make_response(404))
    with pytest.raises(requests.HTTPError) as excinfo:
        lbxd.api_request("member/gone")
    assert excinfo.value.response.status_code == 404


def test_client_requires_credentials(monkeypatch):
    monkeypatch.delenv("LBXD_KEY", raising=False)
    monkeypatch.delenv("LBXD_SECRET", raising=False)
    with pytest.raises(lbxd.LbxdError, match="LBXD_KEY"):
        lbxd.Client()


# ------------------------------------------------------ get_id_from_username


def profile_response(status=200, member_id=None):
    headers = {"X-Letterboxd-Identifier": member_id} if member_id else {}
    return make_response(status, headers=headers)


def test_username_resolves_via_streamed_get(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        assert kwargs.get("timeout") is not None
        assert kwargs.get("stream") is True, "must not download the profile body"
        return profile_response(200, "abc123")

    monkeypatch.setattr(lbxd.requests, "get", fake_get)
    assert lbxd.get_id_from_username("some_user") == "abc123"
    assert calls == ["https://letterboxd.com/some_user/"]


def test_unknown_username_raises_notfound_valueerror(monkeypatch):
    monkeypatch.setattr(lbxd.requests, "get", lambda url, **kw: profile_response(404))
    with pytest.raises(lbxd.LbxdNotFound) as excinfo:
        lbxd.get_id_from_username("nobody")
    assert isinstance(excinfo.value, ValueError)  # toolboxd-web maps ValueError -> UserNotFound


def test_outage_is_not_a_valueerror(monkeypatch):
    """A 5xx/429 must never look like a bad username."""
    for status in (429, 500, 503):
        monkeypatch.setattr(
            lbxd.requests, "get", lambda url, s=status, **kw: profile_response(s)
        )
        with pytest.raises(lbxd.LbxdTransientError) as excinfo:
            lbxd.get_id_from_username("some_user")
        assert not isinstance(excinfo.value, ValueError)


def test_invalid_usernames_rejected_without_http(monkeypatch):
    def boom(*a, **kw):
        raise AssertionError("no HTTP call should be made")

    monkeypatch.setattr(lbxd.requests, "get", boom)
    for bad in ("film/parasite-2019", "a/../b", "user name", "user?x=1", "", "a" * 33):
        with pytest.raises(ValueError):
            lbxd.get_id_from_username(bad)


# ------------------------------------------------------------- data fetchers


def test_get_member_watches_paginates_and_shapes(fake_api):
    # 230 items across 3 pages; rating present only on multiples of 3.
    fake_api.route_pages(
        "films/", 230,
        item_extra=lambda i: {
            "relationships": [{"relationship": {"rating": 4.0}}] if i % 3 == 0 else []
        },
    )
    df = lbxd.get_member_watches("m1")
    assert list(df.columns) == ["member", "film", "rating"]
    assert len(df) == 230
    assert df["film"].tolist() == [f"f{i}" for i in range(230)], "page order preserved"
    assert df["rating"].iloc[0] == 4.0
    assert pd.isna(df["rating"].iloc[1])


def test_get_member_watches_empty_has_columns(fake_api):
    fake_api.route("films/", make_response(200, {"items": []}))
    df = lbxd.get_member_watches("m1")
    assert df.empty
    assert list(df.columns) == ["member", "film", "rating"]


def test_get_member_watchlist_parallel_pages_ordered(fake_api):
    # 2,050 items = 21 pages = several speculative waves, ends mid-wave.
    fake_api.route_pages("watchlist", 2050)
    df = lbxd.get_member_watchlist("m1")
    assert len(df) == 2050
    assert df["id"].tolist() == [f"f{i}" for i in range(2050)], "page order preserved"


def test_single_page_listing_makes_one_request(fake_api):
    fake_api.route_pages("watchlist", 40)  # no "next" on page one
    df = lbxd.get_member_watchlist("m1")
    assert len(df) == 40
    assert len(fake_api.sent) == 1, "no speculative fetches when page one is terminal"


def test_overshoot_pages_are_harmless(fake_api):
    # 150 items: page one has next; wave overshoots far past the end.
    fake_api.route_pages("watchlist", 150)
    df = lbxd.get_member_watchlist("m1")
    assert len(df) == 150
    assert df["id"].tolist() == [f"f{i}" for i in range(150)]


def test_get_member_watchlist_empty_has_id_column(fake_api):
    fake_api.route("watchlist", make_response(200, {"items": []}))
    df = lbxd.get_member_watchlist("m1")
    assert df.empty and "id" in df.columns


def test_get_combined_watchlists_empty_input():
    assert lbxd.get_combined_watchlists([]).empty


def test_get_member_info(fake_api):
    fake_api.route("member/abc", make_response(200, {"id": "abc", "displayName": "A"}))
    assert lbxd.get_member_info("abc")["displayName"] == "A"


# ------------------------------------------------------- threaded_api_request


def test_threaded_404_fails_fast_without_retries(fake_api, no_sleep):
    """The old implementation burned ~62s of backoff per deleted member."""
    fake_api.route("member/gone", make_response(404))
    fake_api.route("member/ok", make_response(200, {"id": "ok"}))

    results, missing, failed = lbxd.threaded_api_request(
        ["member/ok", "member/gone"], max_threads=1
    )
    assert results == [{"id": "ok"}]
    assert missing == ["member/gone"]
    assert failed == []
    assert no_sleep == [], "404 must not trigger any backoff sleep"


def test_threaded_429_honours_retry_after(fake_api, no_sleep):
    fake_api.route(
        "member/busy",
        make_response(429, headers={"Retry-After": "7"}),
        make_response(200, {"id": "busy"}),
    )
    results, missing, failed = lbxd.threaded_api_request(["member/busy"], max_threads=1)
    assert results == [{"id": "busy"}]
    assert no_sleep == [7.0]
    assert failed == [] and missing == []


def test_threaded_429_gives_up_after_wait_budget(fake_api, no_sleep):
    fake_api.route("member/wall", make_response(429, headers={"Retry-After": "1"}))
    results, missing, failed = lbxd.threaded_api_request(
        ["member/wall"], max_threads=1, max_retries=0
    )
    assert results == [] and missing == []
    assert len(failed) == 1 and "429" in failed[0]["reason"]
    assert len(no_sleep) == lbxd.RATE_LIMIT_MAX_WAITS, "429 loop must be bounded"


def test_threaded_5xx_retries_then_fails(fake_api, no_sleep):
    fake_api.route("member/flaky", make_response(500))
    results, missing, failed = lbxd.threaded_api_request(
        ["member/flaky"], max_threads=1, max_retries=2
    )
    assert results == [] and missing == []
    assert failed == [{"url": "member/flaky", "reason": "HTTP 500"}]
    assert len(no_sleep) == 2  # one backoff per retry, none after giving up


def test_threaded_preserves_input_order(fake_api):
    for i in range(5):
        fake_api.route(f"member/u{i}", make_response(200, {"n": i}))
    urls = [f"member/u{i}" for i in range(5)]
    results, _, _ = lbxd.threaded_api_request(urls, max_threads=5)
    assert [r["n"] for r in results] == [0, 1, 2, 3, 4]


# ------------------------------------------------------------------ id codec


def test_encode_decode_roundtrip():
    for internal in (1, 42, 123456, 10**15):
        for is_user in (False, True):
            external = lbxd.encode_id(internal, is_user=is_user)
            assert lbxd.decode_id(external) == internal


def test_decode_large_id_exact():
    # 10**17 * 10 + 7 overflows float53 precision: the old float-division
    # implementation returned an off-by-one here.
    external = lbxd.encode_id(10**17, is_user=True)
    assert lbxd.decode_id(external) == 10**17
