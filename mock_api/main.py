"""Mock retailer sell-through API -- stands in for a retailer vendor portal
that serves weekly sales by REST instead of dropping files.

    GET /v1/sales?client_id=C102&retailer_id=R202&week_start=2026-08-29&week_end=2026-09-26[&cursor=N]
    Authorization: Bearer <RETAILER_API_TOKEN>

Cursor-paged (PAGE_SIZE records per page, `next_cursor` is null on the last
page). Data comes from the same generator as the S3 files, as of the
requested week_end, so restatements behave the same way.

Run:  uvicorn mock_api.main:app --port 9100
"""
import os
from datetime import date

from fastapi import FastAPI, Header, HTTPException, Query

from data_gen.catalog import load_pipelines
from data_gen.generate import api_records, normalized_rows
from etl.weeks import week_range

PAGE_SIZE = 50
TOKEN = os.environ.get("RETAILER_API_TOKEN", "local-dev-token")

app = FastAPI(title="Mock retailer sell-through API")

# Only pipelines configured as API sources are served -- anything else is a 404,
# like asking a real portal for a vendor/retailer pair you have no feed for.
API_FEEDS = {(p["client_id"], p["retailer_id"]): p["product_level"]
             for p in load_pipelines() if p["source_type"] == "api"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/sales")
def sales(
    client_id: str,
    retailer_id: str,
    week_start: date,
    week_end: date,
    cursor: int = Query(0, ge=0),
    authorization: str = Header(""),
):
    if authorization != f"Bearer {TOKEN}":
        raise HTTPException(401, "invalid or missing bearer token")
    level = API_FEEDS.get((client_id, retailer_id))
    if level is None:
        raise HTTPException(404, f"no API feed for {client_id} at {retailer_id}")
    try:
        weeks = week_range(week_start, week_end)
    except ValueError as e:
        raise HTTPException(422, str(e))

    records = api_records(normalized_rows(client_id, retailer_id, level, weeks, as_of=week_end))
    page = records[cursor:cursor + PAGE_SIZE]
    next_cursor = cursor + PAGE_SIZE if cursor + PAGE_SIZE < len(records) else None
    return {"data": page, "next_cursor": next_cursor, "total": len(records)}
