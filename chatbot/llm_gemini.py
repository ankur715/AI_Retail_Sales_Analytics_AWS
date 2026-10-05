"""Gemini provider (google-genai, same SDK and env vars as Web_App).

Structured output via response_schema; backoff on 503 overload and 429
per-minute limits (honouring Gemini's retryDelay hint); a per-DAY quota
fails fast because it won't reset in seconds.
"""
import time

from google import genai
from google.genai import errors, types
from pydantic import BaseModel

from chatbot.llm_types import LLMUnavailable
from etl import config

RETRY_DELAYS = (2, 5, 10)   # seconds between attempts on 429 / 5xx
MAX_SERVER_HINT = 30        # cap on a 429's own retryDelay hint, seconds

_client = None


def client():
    global _client
    if _client is None:
        if not config.GOOGLE_API_KEY:
            raise LLMUnavailable("GOOGLE_API_KEY is not set (add it to .env)", kind="config")
        _client = genai.Client(api_key=config.GOOGLE_API_KEY)
    return _client


def _details(e: errors.APIError) -> list:
    return ((e.details or {}).get("error") or {}).get("details", []) or []


def _daily_quota_hit(e: errors.APIError) -> bool:
    """A per-DAY quota (e.g. the free tier's 20 requests/day/model) won't reset in seconds --
    retrying only burns time, so fail fast with a clear message."""
    return any("PerDay" in str(v.get("quotaId", "")) for d in _details(e) for v in d.get("violations", []) or [])


def _server_retry_delay(e: errors.APIError) -> float:
    """Gemini's 429 says how long to wait (details[].retryDelay, e.g. "21s"); per-minute quotas
    reset faster if we honour it than with a blind backoff."""
    for d in _details(e):
        try:
            return min(float(str(d.get("retryDelay", "")).rstrip("s")), MAX_SERVER_HINT)
        except ValueError:
            continue
    return 0.0


def _generate(prompt: str, gen_config: types.GenerateContentConfig):
    for attempt, delay in enumerate((*RETRY_DELAYS, None)):
        try:
            return client().models.generate_content(model=config.GEMINI_MODEL, contents=prompt, config=gen_config)
        except errors.APIError as e:
            if not (e.code == 429 or (e.code or 0) >= 500):
                raise LLMUnavailable(f"Gemini error {e.code} {e.status}", kind="config") from e
            if e.code == 429 and _daily_quota_hit(e):
                raise LLMUnavailable(f"Gemini daily quota exhausted for {config.GEMINI_MODEL} "
                                     f"(set GEMINI_MODEL to another model, enable billing, "
                                     f"or LLM_PROVIDER=bedrock)", kind="quota") from e
            if delay is None:
                raise LLMUnavailable(f"Gemini unavailable after {attempt + 1} attempts: {e.code} {e.status}") from e
            time.sleep(max(delay, _server_retry_delay(e)))


def json_call(prompt: str, schema: type[BaseModel]) -> BaseModel:
    resp = _generate(prompt, types.GenerateContentConfig(response_mime_type="application/json",
                                                         response_schema=schema, temperature=0.0))
    return resp.parsed if isinstance(resp.parsed, schema) else schema.model_validate_json(resp.text)


def text_call(prompt: str) -> str:
    return (_generate(prompt, types.GenerateContentConfig(temperature=0.2)).text or "").strip()


def model_name() -> str:
    return config.GEMINI_MODEL
