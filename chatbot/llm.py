"""The chatbot's three LLM calls, independent of which LLM answers them.

  route()        question + one-line view summaries -> the views it needs
  write_sql()    question + ONLY those views' details -> SQL (structured output)
  summarize()    question + result rows -> a short plain-English answer

LLM_PROVIDER picks who answers (the prompts are the same for both):
  gemini   chatbot/llm_gemini.py   Google Gemini (GOOGLE_API_KEY), like Web_App
  bedrock  chatbot/llm_bedrock.py  Claude on Amazon Bedrock (AWS credentials),
                                   like the Member Engagement project
"""
from importlib import import_module

from chatbot import catalog
from chatbot.llm_types import LLMUnavailable, Route, SqlPlan  # noqa: F401  (re-exported for app/tests)
from etl import config

PROVIDERS = {"gemini": "chatbot.llm_gemini", "bedrock": "chatbot.llm_bedrock"}


def provider():
    """The provider module for LLM_PROVIDER; each exposes json_call(), text_call(), model_name()."""
    if config.LLM_PROVIDER == "none":
        raise LLMUnavailable("LLM features are turned off (LLM_PROVIDER=none)", kind="config")
    if config.LLM_PROVIDER not in PROVIDERS:
        raise LLMUnavailable(f"LLM_PROVIDER must be one of {sorted(PROVIDERS)}, got {config.LLM_PROVIDER!r}",
                             kind="config")
    return import_module(PROVIDERS[config.LLM_PROVIDER])


def configured() -> bool:
    """Whether the selected provider has what it needs to make a call."""
    if config.LLM_PROVIDER == "gemini":
        return bool(config.GOOGLE_API_KEY)
    return config.LLM_PROVIDER in PROVIDERS   # Bedrock uses the AWS credential chain


def model_name() -> str:
    return provider().model_name()


def route(question: str) -> Route:
    prompt = f"""You route questions about retail sell-through data to the database views needed to answer them.
Pick the FEWEST views that answer the question (usually one; add chat.v_calendar only for named
months/quarters/dates). Only choose from this list:
{catalog.routing_text()}

Question: "{question}"
"""
    result = provider().json_call(prompt, Route)
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
    return provider().json_call(prompt, SqlPlan)


def summarize(question: str, sql: str, columns: list[str], rows: list[list], data_as_of: str | None) -> str:
    sample = [dict(zip(columns, r)) for r in rows[:50]]
    # The SQL goes in too: its WHERE clause carries filters (e.g. brand = '...') that the result
    # columns don't repeat -- without it the model second-guesses rows that are correctly filtered.
    prompt = f"""You are a retail analytics assistant. Answer the question in 2-4 sentences using ONLY
the query result below. The result was produced by the SQL shown, so every row already satisfies its
WHERE filters even when a filtered column isn't in the result. Quote specific numbers (format money as
$1,234.56). If the result is empty, say no matching data was found. Do not invent numbers. Data runs
through the week ending {data_as_of or 'unknown'}; mention it when the question is about recent periods.

Question: "{question}"
SQL: {sql}
Result ({len(rows)} rows{', first 50 shown' if len(rows) > 50 else ''}):
{sample}
"""
    return provider().text_call(prompt)
