"""Analytics chatbot: plain-English questions -> SQL on the curated chat
views -> answer. A separate always-on service; it never touches the ETL
tables or Airflow -- the DAGs only keep the data behind the views fresh.

    POST /api/chat  {"message": "..."}
      1. route      Gemini picks the views the question needs (catalog summaries only)
      2. write_sql  Gemini writes SQL with ONLY those views' columns/values/examples
      3. validate   sqlglot: one SELECT, only the routed views, row cap
      4. run        as the read-only chat_reader user (30s timeout)
         -> on a validation or SQL error, one repair round (back to 2 with the error)
      5. summarize  Gemini answers from the returned rows only

Run:  .venv/bin/uvicorn chatbot.app:app --port 8000   ->  http://localhost:8000
"""
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from chatbot import catalog, db, guard, llm
from etl import config

app = FastAPI(title="Retail sales analytics chatbot")
STATIC = Path(__file__).resolve().parent / "static"
MAX_REPAIRS = 1
CACHE_TTL = 15 * 60   # same question within 15 min -> no Gemini or Redshift call
_cache: dict[str, tuple[float, dict]] = {}


class ChatRequest(BaseModel):
    message: str = Field(min_length=3, max_length=500)


class ChatResponse(BaseModel):
    question: str
    answer: str
    views: list[str] = []
    sql: str | None = None
    explanation: str | None = None
    columns: list[str] = []
    rows: list[list] = []
    row_count: int = 0
    data_as_of: str | None = None
    cached: bool = False
    error: str | None = None


MESSAGES = {
    "busy": "The language model is busy right now -- please try again in a minute.",
    "quota": "The language model's daily quota is used up -- try again tomorrow.",
    "config": "The language model isn't configured correctly on the server.",
    "declined": "The language model declined to answer that question.",
}


@app.exception_handler(llm.LLMUnavailable)
def llm_unavailable(request: Request, exc: llm.LLMUnavailable):
    # A clear, retryable answer instead of a 500; `reason` carries the detail for the operator
    return JSONResponse(status_code=503, content={"detail": MESSAGES.get(exc.kind, MESSAGES["busy"]),
                                                  "reason": str(exc)})


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health():
    return {"status": "ok", "env": config.ENV, "database": config.REDSHIFT_DB, "provider": config.LLM_PROVIDER,
            "model": llm.model_name(), "llm_configured": llm.configured(), "views": catalog.view_names()}


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    question = req.message.strip()
    key = " ".join(question.lower().split())
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < CACHE_TTL:
        return ChatResponse(**hit[1], cached=True)
    if not llm.configured():
        raise HTTPException(503, "LLM features are turned off on the server (LLM_PROVIDER=none)"
                            if config.LLM_PROVIDER == "none" else
                            f"The {config.LLM_PROVIDER} LLM is not configured on the server")

    # 1. route: only the views this question needs
    route = llm.route(question)
    if not route.views:
        return ChatResponse(question=question, answer=f"I can only answer questions about weekly retail "
                            f"sales and inventory data. ({route.reason})")

    # 2-4. write SQL with only those views' metadata, validate, run; one repair round on failure
    details = catalog.detail_text(route.views, db.known_values(catalog.known_value_columns(route.views)))
    plan, error, result, sql = llm.write_sql(question, route.views, details), None, None, None
    for attempt in range(MAX_REPAIRS + 1):
        if not plan.sql.strip():
            return ChatResponse(question=question, views=route.views,
                                answer=plan.unanswerable_reason or "That can't be answered from the available data.")
        try:
            sql = guard.validate(plan.sql, route.views)
            result = db.query(sql)
            error = None
            break
        except Exception as e:   # UnsafeSQL or a Redshift error: give Gemini the exact message once
            error = str(e).strip().splitlines()[0][:300]
            if attempt < MAX_REPAIRS:
                plan = llm.write_sql(question, route.views, details, previous_error=error, previous_sql=plan.sql)
    if error:
        return ChatResponse(question=question, views=route.views, sql=sql or plan.sql, error=error,
                            answer="Sorry, I couldn't build a working query for that. Try rephrasing.")

    # 5. answer from the rows only
    as_of = db.data_as_of()
    answer = llm.summarize(question, sql, result["columns"], result["rows"], as_of)
    payload = dict(question=question, answer=answer, views=route.views, sql=sql, explanation=plan.explanation,
                   columns=result["columns"], rows=result["rows"], row_count=result["row_count"], data_as_of=as_of)
    _cache[key] = (time.monotonic(), payload)
    return ChatResponse(**payload)
