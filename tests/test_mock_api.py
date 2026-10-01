from fastapi.testclient import TestClient

from mock_api.main import PAGE_SIZE, app

client = TestClient(app)
AUTH = {"Authorization": "Bearer local-dev-token"}
Q = {"client_id": "C102", "retailer_id": "R202", "week_start": "2026-08-29", "week_end": "2026-09-26"}


def test_requires_token():
    assert client.get("/v1/sales", params=Q).status_code == 401


def test_unknown_feed_is_404():
    assert client.get("/v1/sales", params=Q | {"client_id": "C101"}, headers=AUTH).status_code == 404


def test_non_saturday_window_is_422():
    assert client.get("/v1/sales", params=Q | {"week_end": "2026-09-25"}, headers=AUTH).status_code == 422


def test_cursor_pagination_returns_every_record_once():
    records, cursor = [], 0
    while cursor is not None:
        page = client.get("/v1/sales", params=Q | {"cursor": cursor}, headers=AUTH).json()
        assert len(page["data"]) <= PAGE_SIZE
        records += page["data"]
        cursor = page["next_cursor"]
    assert len(records) == page["total"] == 5 * 2 * 21   # weeks x stores x (20 SKUs + 1 unmapped)
    assert len({(r["weekEnding"], r["storeNumber"], r["style"], r["color"], r["size"]) for r in records}) == len(records)
