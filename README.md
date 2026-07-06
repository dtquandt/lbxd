# lbxd

A standalone Python client for the [Letterboxd API](https://api-docs.letterboxd.com/),
used across the Toolboxd ecosystem (`letterboxd-collector`,
`letterboxd-recommender`, `toolboxd-web`).

Signs requests itself (HMAC-SHA256 per the API spec) — as of v1.0 there is no
dependency on the abandoned `letterboxd` wrapper package. Every request
carries a timeout; 404s fail fast; 429s honour `Retry-After`.

## Install

```bash
pip install git+https://github.com/dtquandt/lbxd
```

Credentials via environment variables: `LBXD_KEY`, `LBXD_SECRET`.

## Use

```python
import lbxd

member_id = lbxd.get_id_from_username("dtquandt")   # LbxdNotFound if unknown
watches = lbxd.get_member_watches(member_id)        # DataFrame: member, film, rating
watchlist = lbxd.get_member_watchlist(member_id)    # DataFrame of raw film items
info = lbxd.get_member_info(member_id)              # raw member dict

resp = lbxd.api_request("films/?perPage=100")       # one signed GET; Response on
                                                    # 2xx, requests.HTTPError otherwise

results, missing, failed = lbxd.threaded_api_request(
    [f"member/{m}" for m in member_ids], max_threads=20
)
```

### Errors

- `LbxdNotFound` (subclasses `ValueError`) — the user/resource doesn't exist.
- `LbxdInvalidUsername` — input can't be a Letterboxd username (also a `LbxdNotFound`).
- `LbxdTransientError` — upstream outage / rate limit; **not** a `ValueError`,
  so it can never be mistaken for a bad username.
- `api_request` keeps its historical contract: `requests.HTTPError` with
  `.response` attached on any non-2xx.

## Tests

```bash
pytest tests/
```

The suite fakes HTTP at the `Session.send` seam — no network, no credentials.
