"""Gemini calls (google-genai, same SDK and env vars as Web_App).

  route()        question + one-line view summaries -> the views it needs
  write_sql()    question + ONLY those views' details -> SQL (JSON output)
  summarize()    question + result rows -> a short plain-English answer

Structured output (response_schema) instead of parsing free text, and
temperature 0 for the SQL so the same question gives the same query.
"""
import time

from google import genai
from google.genai import errors, types
from pydantic import BaseModel, Field

from chatbot import catalog
from etl import config

_client = None


def client():
    global _client
    if _client is None:
        if not config.GOOGLE_API_KEY:
            raise RuntimeError("GOOGLE_API_KEY is not set (add it to .env)")
        _client = genai.Client(api_key=config.GOOGLE_API_KEY)
    return _client


class LLMUnavailable(RuntimeError):
    """Gemini is overloaded or unreachable after retries -- tell the user to try again."""


RETRY_DELAYS = (2, 5, 10)   # seconds between attempts on 429 / 5xx
MAX_SERVER_HINT = 30        # cap on a 429's own retryDelay hint, seconds


def _daily_quota_hit(e: errors.APIError) -> bool:
    """A per-DAY quota (e.g. the free tier's 20 requests/day/model) won't reset in seconds --
    retrying only burns time, so fail fast with a clear message."""
    for d in ((e.details or {}).get("error") or {}).get("details", []) or []:
        if any("PerDay" in str(v.get("quotaId", "")) for v in d.get("violations", []) or []):
            return True
    return False


def _server_retry_delay(e: errors.APIError) -> float:
    """Gemini's 429 says how long to wait (details[].retryDelay, e.g. "21s"); per-minute quotas
    reset faster if we honour it than with a blind backoff."""
    for d in ((e.details or {}).get("error") or {}).get("details", []) or []:
        hint = str(d.get("retryDelay", "")).rstrip("s")
        try:
            return min(float(hint), MAX_SERVER_HINT)
        except ValueError:
            continue
    return 0.0


def _generate(prompt: str, gen_config: types.GenerateContentConfig):
    """generate_content with backoff on rate limits and server overload
    (e.g. 503 "model is experiencing high demand"); other errors raise at once."""
    for attempt, delay in enumerate((*RETRY_DELAYS, None)):
        try:
            return client().models.generate_content(model=config.GEMINI_MODEL, contents=prompt, config=gen_config)
        except errors.APIError as e:
            retryable = e.code == 429 or (e.code or 0) >= 500
            if not retryable:
                raise
            if e.code == 429 and _daily_quota_hit(e):
                raise LLMUnavailable(f"Gemini daily quota exhausted for {config.GEMINI_MODEL} "
                                     f"(set GEMINI_MODEL to another model or enable billing)") from e
            if delay is None:
                raise LLMUnavailable(f"Gemini unavailable after {attempt + 1} attempts: {e.code} {e.status}") from e
            time.sleep(max(delay, _server_retry_delay(e)))


class Route(BaseModel):
    views: list[str] = Field(description="Views needed to answer, from the list given. Empty if none fit.")
    reason: str = Field(description="One short sentence.")


class SqlPlan(BaseModel):
    sql: str = Field(description="One Redshift SELECT query, or empty if the question can't be answered.")
    explanation: str = Field(description="One sentence: what the query computes.")
    unanswerable_reason: str = Field(default="", description="Why it can't be answered, if sql is empty.")


def _json_call(prompt: str, schema: type[BaseModel], temperature: float = 0.0) -> BaseModel:
    resp = _generate(prompt, types.GenerateContentConfig(response_mime_type="application/json",
                                                         response_schema=schema, temperature=temperature))
    return resp.parsed if isinstance(resp.parsed, schema) else schema.model_validate_json(resp.text)


def route(question: str) -> Route:
    prompt = f"""You route questions about retail sell-through data to the database views needed to answer them.
Pick the FEWEST views that answer the question (usually one; add chat.v_calendar only for named
months/quarters/dates). Only choose from this list:
{catalog.routing_text()}

Question: "{question}"
"""
    result = _json_call(prompt, Route)
    result.views = [v for v in result.views if v in catalog.view_names()]   # drop anything invented
    return result


def write_sql(question: str, views: list[str], details: str, previous_error: str | None = None,
              previous_sql: str | None = None) -> SqlPlan:
    retry = ""
    if previous_error:
        retry = f"""
Your previous query failed. Fix it.
Previous SQL: {previous_sql}
Error: {previous_error}
"""
    prompt = f"""You write Amazon Redshift SQL for a retail analytics chatbot.
Use ONLY these views (fully qualified, e.g. chat.v_sales_by_week): {', '.join(views)}.
Write one read-only SELECT. Use the exact known values for brand/retailer/category filters.
Round money to 2 decimals. Order results meaningfully. If the question can't be answered from
these views, return an empty sql and say why.

{details}
{retry}
Question: "{question}"
"""
    return _json_call(prompt, SqlPlan)


def summarize(question: str, columns: list[str], rows: list[list], data_as_of: str | None) -> str:
    sample = [dict(zip(columns, r)) for r in rows[:50]]
    prompt = f"""You are a retail analytics assistant. Answer the question in 2-4 sentences using ONLY
the query result below. Quote specific numbers (format money as $1,234.56). If the result is
empty, say no matching data was found. Do not invent numbers. Data runs through the week ending
{data_as_of or 'unknown'}; mention it when the question is about recent periods.

Question: "{question}"
Result ({len(rows)} rows{', first 50 shown' if len(rows) > 50 else ''}):
{sample}
"""
    resp = _generate(prompt, types.GenerateContentConfig(temperature=0.2))
    return (resp.text or "").strip()
