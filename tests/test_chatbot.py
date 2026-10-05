"""Chatbot tests -- no Gemini and no Redshift: llm.* and db.* are faked."""
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chatbot import app as chat_app
from chatbot import catalog, guard, llm, llm_bedrock, llm_gemini

VIEWS_SQL = (Path(__file__).resolve().parent.parent / "sql" / "redshift" / "V010__chat_views.sql").read_text()


# ---------------------------------------------------------------------------
# guard
# ---------------------------------------------------------------------------
ROUTED = ["chat.v_sales_by_week", "chat.v_calendar"]


@pytest.mark.parametrize("sql", [
    "SELECT retailer, SUM(sales_usd) FROM chat.v_sales_by_week GROUP BY retailer",
    "SELECT updated_at FROM chat.v_sales_by_week",                     # 'update' inside a name is fine
    "WITH t AS (SELECT * FROM chat.v_sales_by_week) SELECT * FROM t",
    "SELECT s.week_ending FROM chat.v_sales_by_week s JOIN chat.v_calendar c ON c.week_ending = s.week_ending",
    "```sql\nSELECT brand FROM chat.v_sales_by_week;```",
])
def test_guard_accepts_reads_of_routed_views(sql):
    assert guard.validate(sql, ROUTED).endswith(f"LIMIT {guard.MAX_ROWS}")


@pytest.mark.parametrize("sql,reason", [
    ("SELECT * FROM fact.fact_sales", "not allowed"),
    ("SELECT * FROM chat.v_weekly_sales", "not allowed"),               # real view, but not routed
    ("SELECT * FROM (SELECT * FROM stage.stg_sales) x", "not allowed"),
    ("SELECT * FROM chat.v_sales_by_week UNION ALL SELECT * FROM etl.load_audit", "not allowed"),
    ("DELETE FROM chat.v_sales_by_week", "only SELECT"),
    ("SELECT 1; DROP TABLE fact.fact_sales", "exactly one statement"),
])
def test_guard_rejects(sql, reason):
    with pytest.raises(guard.UnsafeSQL, match=reason):
        guard.validate(sql, ROUTED)


def test_guard_caps_limit_but_keeps_smaller_ones():
    assert guard.validate("SELECT brand FROM chat.v_sales_by_week LIMIT 5", ROUTED).endswith("LIMIT 5")
    assert guard.validate("SELECT brand FROM chat.v_sales_by_week LIMIT 99999", ROUTED).endswith("LIMIT 500")


# ---------------------------------------------------------------------------
# catalog (metadata)
# ---------------------------------------------------------------------------
def test_every_catalog_view_is_created_by_the_migration():
    for view in catalog.view_names():
        assert f"CREATE OR REPLACE VIEW {view} AS" in VIEWS_SQL


def test_routing_text_has_only_summaries():
    text = catalog.routing_text()
    assert all(v in text for v in catalog.view_names())
    assert "inventory_units:" not in text            # no column detail at the routing step


def test_detail_text_includes_only_the_selected_views():
    text = catalog.detail_text(["chat.v_data_freshness"],
                               {"chat.v_data_freshness.retailer": ["Harbor Mart", "Summit Outfitters"]})
    assert "View chat.v_data_freshness" in text and "known values of retailer: Harbor Mart" in text
    assert "chat.v_weekly_sales" not in text and "is_out_of_stock" not in text


def test_catalog_examples_pass_the_guard_with_the_views_they_use():
    for name, view in catalog.load()["views"].items():
        for ex in view.get("examples", []):
            used = sorted(set(re.findall(r"chat\.v_\w+", ex["sql"])))
            assert name in used
            guard.validate(ex["sql"], used)


# ---------------------------------------------------------------------------
# /api/chat flow
# ---------------------------------------------------------------------------
@pytest.fixture
def fakes(monkeypatch):
    calls = {"write_sql": [], "query": []}
    monkeypatch.setattr(chat_app.config, "GOOGLE_API_KEY", "test-key")
    monkeypatch.setattr(chat_app, "_cache", {})
    monkeypatch.setattr(llm, "route", lambda q: llm.Route(views=["chat.v_sales_by_week"], reason="totals"))
    monkeypatch.setattr(llm, "summarize", lambda q, sql, cols, rows, as_of: f"{len(rows)} rows through {as_of}")
    monkeypatch.setattr(chat_app.db, "known_values", lambda cols: {"chat.v_sales_by_week.retailer": ["Harbor Mart"]})
    monkeypatch.setattr(chat_app.db, "data_as_of", lambda: "2026-10-17")

    def fake_query(sql):
        calls["query"].append(sql)
        return {"columns": ["retailer", "sales_usd"], "rows": [["Harbor Mart", 100.0]], "row_count": 1}

    monkeypatch.setattr(chat_app.db, "query", fake_query)
    return calls


def test_chat_routes_writes_validates_and_answers(fakes, monkeypatch):
    seen = {}

    def write_sql(q, views, details, previous_error=None, previous_sql=None):
        seen["views"], seen["details"] = views, details
        return llm.SqlPlan(sql="SELECT retailer, SUM(sales_usd) AS sales_usd FROM chat.v_sales_by_week "
                               "WHERE weeks_ago = 0 GROUP BY retailer", explanation="totals")

    monkeypatch.setattr(llm, "write_sql", write_sql)
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "Total sales by retailer last week"}).json()
    assert r["answer"] == "1 rows through 2026-10-17" and r["views"] == ["chat.v_sales_by_week"]
    assert r["sql"].endswith("LIMIT 500") and r["rows"] == [["Harbor Mart", 100.0]]
    assert "View chat.v_sales_by_week" in seen["details"] and "v_weekly_sales" not in seen["details"]


def test_chat_repairs_once_when_sql_uses_an_unrouted_table(fakes, monkeypatch):
    plans = iter([llm.SqlPlan(sql="SELECT * FROM fact.fact_sales", explanation="bad"),
                  llm.SqlPlan(sql="SELECT brand FROM chat.v_sales_by_week", explanation="fixed")])
    errors = []

    def write_sql(q, views, details, previous_error=None, previous_sql=None):
        errors.append(previous_error)
        return next(plans)

    monkeypatch.setattr(llm, "write_sql", write_sql)
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "anything at all"}).json()
    assert r["error"] is None and r["explanation"] == "fixed"
    assert errors[0] is None and "fact.fact_sales is not allowed" in errors[1]
    assert len(fakes["query"]) == 1                      # the unsafe query never reached the database


def test_chat_caches_repeated_questions(fakes, monkeypatch):
    monkeypatch.setattr(llm, "write_sql", lambda *a, **k: llm.SqlPlan(sql="SELECT brand FROM chat.v_sales_by_week",
                                                                      explanation="x"))
    client = TestClient(chat_app.app)
    first = client.post("/api/chat", json={"message": "Brands please"}).json()
    second = client.post("/api/chat", json={"message": "  brands   PLEASE "}).json()
    assert not first["cached"] and second["cached"] and len(fakes["query"]) == 1


def test_chat_off_topic_question_runs_no_query(fakes, monkeypatch):
    monkeypatch.setattr(llm, "route", lambda q: llm.Route(views=[], reason="not about sales data"))
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "What's the weather?"}).json()
    assert "not about sales data" in r["answer"] and fakes["query"] == []


def test_chat_without_api_key_is_503(monkeypatch):
    monkeypatch.setattr(chat_app.config, "GOOGLE_API_KEY", "")
    monkeypatch.setattr(chat_app, "_cache", {})
    assert TestClient(chat_app.app).post("/api/chat", json={"message": "hello there"}).status_code == 503


def test_gemini_overload_retries_then_returns_clear_503(monkeypatch):
    from google.genai import errors

    attempts = []

    class Busy:
        class models:
            @staticmethod
            def generate_content(**kwargs):
                attempts.append(1)
                raise errors.ServerError(503, {"error": {"code": 503, "status": "UNAVAILABLE",
                                                         "message": "high demand"}})

    monkeypatch.setattr(llm_gemini, "client", lambda: Busy)
    monkeypatch.setattr(llm_gemini.time, "sleep", lambda s: None)
    monkeypatch.setattr(chat_app.config, "GOOGLE_API_KEY", "test-key")
    monkeypatch.setattr(chat_app, "_cache", {})
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "Total sales last week"})
    assert r.status_code == 503 and "busy" in r.json()["detail"]
    assert len(attempts) == len(llm_gemini.RETRY_DELAYS) + 1


def test_daily_quota_fails_fast_without_retrying(monkeypatch):
    from google.genai import errors

    attempts = []

    class OutOfQuota:
        class models:
            @staticmethod
            def generate_content(**kwargs):
                attempts.append(1)
                raise errors.ClientError(429, {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED", "message": "quota",
                    "details": [{"violations": [{"quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]},
                                {"retryDelay": "23517s"}]}})

    monkeypatch.setattr(llm_gemini, "client", lambda: OutOfQuota)
    monkeypatch.setattr(llm_gemini.time, "sleep", lambda s: pytest.fail("must not sleep on a daily quota"))
    monkeypatch.setattr(chat_app.config, "GOOGLE_API_KEY", "test-key")
    monkeypatch.setattr(chat_app, "_cache", {})
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "Total sales last week"})
    assert r.status_code == 503 and "daily quota" in r.json()["detail"] and len(attempts) == 1



# ---------------------------------------------------------------------------
# Bedrock provider (Claude via the anthropic SDK) -- faked client, no AWS
# ---------------------------------------------------------------------------
class FakeMessages:
    def __init__(self, parsed=None, text="", stop_reason="end_turn", error=None):
        self.parsed, self.text, self.stop_reason, self.error, self.calls = parsed, text, stop_reason, error, []

    def _respond(self, **kwargs):
        from types import SimpleNamespace
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return SimpleNamespace(stop_reason=self.stop_reason, parsed_output=self.parsed,
                               content=[SimpleNamespace(type="text", text=self.text)])

    def parse(self, **kwargs):
        return self._respond(**kwargs)

    def create(self, **kwargs):
        return self._respond(**kwargs)


@pytest.fixture
def bedrock(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(llm.config, "LLM_PROVIDER", "bedrock")
    fake = FakeMessages()
    monkeypatch.setattr(llm_bedrock, "client", lambda: SimpleNamespace(messages=fake))
    return fake


def test_bedrock_structured_call_uses_parse_with_the_schema(bedrock):
    bedrock.parsed = llm.Route(views=["chat.v_sales_by_week"], reason="totals")
    route = llm.route("Total sales by retailer last week")
    assert route.views == ["chat.v_sales_by_week"]
    call = bedrock.calls[0]
    assert call["output_format"] is llm.Route and call["model"] == "anthropic.claude-opus-5-5"


def test_bedrock_model_id_rules(monkeypatch):
    monkeypatch.setattr(llm_bedrock.config, "BEDROCK_MODEL", "claude-opus-5-5")
    assert llm_bedrock.model_name() == "anthropic.claude-opus-5-5"                 # bare id gets the prefix
    monkeypatch.setattr(llm_bedrock.config, "BEDROCK_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
    assert llm_bedrock.model_name() == "us.anthropic.claude-haiku-4-5-20251001-v1:0"   # profiles verbatim


def test_bedrock_summary_joins_text_blocks(bedrock):
    bedrock.text = "Northgate sold $27,157.60."
    assert llm.summarize("q", "SELECT retailer FROM chat.v_sales_by_week", ["retailer"], [["Northgate"]],
                         "2026-10-17") == "Northgate sold $27,157.60."
    assert "SELECT retailer FROM chat.v_sales_by_week" in bedrock.calls[0]["messages"][0]["content"]


def test_bedrock_refusal_becomes_declined(bedrock):
    bedrock.stop_reason = "refusal"
    with pytest.raises(llm.LLMUnavailable) as exc:
        llm.route("anything")
    assert exc.value.kind == "declined"


def test_bedrock_permission_error_is_a_config_problem_and_a_clear_503(bedrock, monkeypatch):
    import anthropic
    import httpx2
    response = httpx2.Response(403, request=httpx2.Request("POST", "https://bedrock.example"))
    bedrock.error = anthropic.PermissionDeniedError("no model access", response=response, body=None)
    monkeypatch.setattr(chat_app, "_cache", {})
    r = TestClient(chat_app.app).post("/api/chat", json={"message": "Total sales last week"})
    assert r.status_code == 503 and "isn't configured" in r.json()["detail"]
    assert "model access" in r.json()["reason"]


def test_unknown_provider_is_rejected(monkeypatch):
    monkeypatch.setattr(llm.config, "LLM_PROVIDER", "openai")
    with pytest.raises(llm.LLMUnavailable, match="LLM_PROVIDER must be one of"):
        llm.provider()
