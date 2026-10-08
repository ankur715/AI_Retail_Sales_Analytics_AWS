"""Pipeline Triage Agent tests -- a scripted fake Bedrock client and a fake
read-only session: no Bedrock, no Redshift. S3 tools use moto."""
import copy
import json

import boto3
import pytest
import sqlglot
from botocore.exceptions import ClientError
from moto import mock_aws
from sqlglot import exp

from etl import config, s3_io
from triage import agent, readonly, tools

LOAD, PIPELINE = "C101_R201_20261008T120000", "C101_R201"


class FakeBedrock:
    """Returns scripted Converse responses in order; records every request."""

    def __init__(self, responses):
        self.responses, self.requests = list(responses), []

    def converse(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        if not self.responses:
            raise AssertionError("model called more times than scripted")
        return self.responses.pop(0)


def tool_call(*calls, tokens=(500, 50)):
    content = [{"text": "Checking."}] + [
        {"toolUse": {"toolUseId": f"t{i}", "name": name, "input": inp}} for i, (name, inp) in enumerate(calls)]
    return {"output": {"message": {"role": "assistant", "content": content}}, "stopReason": "tool_use",
            "usage": {"inputTokens": tokens[0], "outputTokens": tokens[1]}}


def answer(text, tokens=(800, 200)):
    return {"output": {"message": {"role": "assistant", "content": [{"text": text}]}}, "stopReason": "end_turn",
            "usage": {"inputTokens": tokens[0], "outputTokens": tokens[1]}}


class FakeSession:
    """Stands in for ReadOnlySession: canned rows per named query, counts queries."""

    def __init__(self, rows=None):
        self.rows, self.queries_run, self.closed = rows or {}, 0, False

    def query(self, name, **params):
        readonly.QUERIES[name]                     # same contract as the real one: named queries only
        self.queries_run += 1
        return self.rows.get(name, [])

    def close(self):
        self.closed = True


AUDIT = [{"load_id": LOAD, "pipeline_id": PIPELINE, "status": "FAILED", "window_start": "2026-09-12",
          "window_end": "2026-10-10", "parse_stats": json.dumps({"parsed_rows": 105}),
          "error_message": "ReconciliationError('src != stg + rej ...')"}]
STATS_MISMATCH = [  # stg + rej = 22 > src = 21 in one week: a duplicated STM mapping
    {"week_date": "2026-10-03", "src_rows": 21, "stg_rows": 20, "rej_rows": 1, "src_sales": 1000.0,
     "stg_sales": 950.0, "rej_sales": 50.0, "src_inventory": 500, "stg_inventory": 480, "rej_inventory": 20},
    {"week_date": "2026-10-10", "src_rows": 21, "stg_rows": 21, "rej_rows": 1, "src_sales": 1000.0,
     "stg_sales": 1040.0, "rej_sales": 50.0, "src_inventory": 500, "stg_inventory": 520, "rej_inventory": 20},
]


@pytest.fixture
def llm_on(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "bedrock")
    monkeypatch.setattr(config, "TRIAGE_MODEL", "nova-lite")
    monkeypatch.setattr(config, "TRIAGE_MAX_STEPS", 8)
    monkeypatch.setattr(config, "TRIAGE_MAX_TOKENS", 40000)


@pytest.fixture
def session(monkeypatch):
    s = FakeSession({"load_audit": AUDIT, "stats_by_week": STATS_MISMATCH})
    monkeypatch.setattr(agent, "ReadOnlySession", lambda: s)
    return s


@pytest.fixture
def saved(monkeypatch):
    """Capture what record() writes instead of touching Redshift."""
    writes = []
    monkeypatch.setattr(agent.redshift, "run", lambda statements: writes.extend(statements) or [1])
    return writes


# ---------------------------------------------------------------------------
# the tool loop
# ---------------------------------------------------------------------------
def test_tool_loop_runs_tools_and_returns_the_answer(llm_on, session):
    bedrock = FakeBedrock([tool_call(("get_load_audit", {}), ("get_pipeline_config", {})),
                           tool_call(("get_stats", {})),
                           answer("DIAGNOSIS: duplicate mapping.\nEVIDENCE:\n- x\nSUGGESTED FIX (needs human "
                                  "approval): y\nCONFIDENCE: high")])
    result = agent.triage(LOAD, PIPELINE, "sku_subflow.reconcile", "boom", client=bedrock)

    assert result.status == "answered" and result.steps == 3
    assert result.tool_calls == ["get_load_audit", "get_pipeline_config", "get_stats"]
    assert result.tokens == (500 + 50) * 2 + 800 + 200
    assert result.model == "us.amazon.nova-lite-v1:0"
    assert result.note.startswith("DIAGNOSIS: duplicate mapping.") and "need human approval" in result.note
    assert session.closed and result.redshift_queries == 2         # audit + stats; config came from the snapshot

    first = bedrock.requests[0]
    assert first["modelId"] == "us.amazon.nova-lite-v1:0"
    assert [t["toolSpec"]["name"] for t in first["toolConfig"]["tools"]] == tools.TOOL_NAMES
    # the second request carries one toolResult per toolUse, matched by id
    results = [c["toolResult"] for c in bedrock.requests[1]["messages"][-1]["content"]]
    assert [r["toolUseId"] for r in results] == ["t0", "t1"]
    assert "status" not in results[0]                               # Nova: no status field on toolResult


def test_claude_haiku_switch_uses_its_profile_and_toolresult_status(llm_on, session, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MODEL", "claude-haiku")
    bedrock = FakeBedrock([tool_call(("get_load_audit", {})), answer("DIAGNOSIS: x")])
    agent.triage(LOAD, PIPELINE, "merge_fact", "boom", client=bedrock)
    assert bedrock.requests[0]["modelId"] == "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    assert bedrock.requests[1]["messages"][-1]["content"][0]["toolResult"]["status"] == "success"


def test_unknown_tool_and_bad_arguments_go_back_to_the_model_as_errors(llm_on, session):
    bedrock = FakeBedrock([tool_call(("drop_table", {}), ("get_stats", {"week": 1})), answer("DIAGNOSIS: x")])
    result = agent.triage(LOAD, PIPELINE, "t", "e", client=bedrock)
    texts = [c["toolResult"]["content"][0]["text"] for c in bedrock.requests[1]["messages"][-1]["content"]]
    assert texts[0].startswith("ERROR:") and "unknown tool" in texts[0]
    assert "bad arguments for get_stats" in texts[1]
    assert result.status == "answered"


# ---------------------------------------------------------------------------
# caps
# ---------------------------------------------------------------------------
def test_step_cap_stops_the_loop_and_asks_for_a_final_answer(llm_on, session, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MAX_STEPS", 3)
    bedrock = FakeBedrock([tool_call(("get_load_audit", {})) for _ in range(3)])
    result = agent.triage(LOAD, PIPELINE, "t", "e", client=bedrock)

    assert result.status == "step_cap" and result.steps == 3 and len(bedrock.requests) == 3
    assert result.note.startswith("Triage stopped at its 3 steps limit")
    last_user = bedrock.requests[-1]["messages"][-1]["content"]
    assert last_user[-1]["text"] == agent.FINAL_STEP_NUDGE          # told to answer on the final step


def test_token_cap_stops_before_the_next_call(llm_on, session, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MAX_TOKENS", 1000)
    bedrock = FakeBedrock([tool_call(("get_load_audit", {}), tokens=(900, 200)), answer("unused")])
    result = agent.triage(LOAD, PIPELINE, "t", "e", client=bedrock)
    assert result.status == "token_cap" and len(bedrock.requests) == 1
    assert "40000" not in result.note and "1000 tokens limit" in result.note


# ---------------------------------------------------------------------------
# LLM_PROVIDER=none
# ---------------------------------------------------------------------------
def test_no_llm_skips_cleanly_without_any_call(monkeypatch, saved):
    monkeypatch.setattr(config, "LLM_PROVIDER", "none")
    bedrock = FakeBedrock([])
    monkeypatch.setattr(agent, "ReadOnlySession", lambda: pytest.fail("no session when skipped"))
    result = agent.triage_failed_load(LOAD, PIPELINE, "t", "e", client=bedrock)
    assert result.status == "skipped" and bedrock.requests == [] and saved == []


def test_suite_default_is_llm_provider_none():
    assert config.LLM_PROVIDER == "none" and not agent.enabled()


# ---------------------------------------------------------------------------
# reconcile-mismatch scenario, end to end through the failure-path entry point
# ---------------------------------------------------------------------------
def test_reconcile_mismatch_is_diagnosed_and_saved(llm_on, session, saved):
    diagnosis = ("DIAGNOSIS: Week 2026-10-10 has stg + rej greater than src, so a source key matched two STM "
                 "rows.\nEVIDENCE:\n- get_stats: 2026-10-10 rows src 21 vs stg 21 + rej 1\n"
                 "SUGGESTED FIX (needs human approval): remove the duplicate row from dim.product_stm, then "
                 "clear the reconcile task.\nCONFIDENCE: high")
    bedrock = FakeBedrock([tool_call(("get_load_audit", {}), ("get_stats", {})), answer(diagnosis)])
    result = agent.triage_failed_load(LOAD, PIPELINE, "sku_subflow.reconcile",
                                      "ReconciliationError('src != stg + rej ...')", client=bedrock)

    # the model saw the mismatch, pre-computed per week
    stats_text = bedrock.requests[1]["messages"][-1]["content"][1]["toolResult"]["content"][0]["text"]
    stats = json.loads(stats_text)
    assert stats["weeks_not_reconciling"] == ["2026-10-10"]
    assert stats["weeks"][1]["src_minus_stg_rej"]["rows"] == -1     # negative = duplicated
    assert stats["findings"][0].startswith("2026-10-10: rows src 21 vs stg + rej 22 (1 duplicated)")
    assert "src != stg + rej" in bedrock.requests[0]["messages"][0]["content"][0]["text"]

    # the note, model and tokens were written to the audit row by the ETL user
    sql, params = saved[0]
    assert "UPDATE etl.load_audit" in sql and "triage_note" in sql
    assert params["load_id"] == LOAD and params["model"] == "us.amazon.nova-lite-v1:0"
    assert params["tokens"] == result.tokens and params["note"].startswith("DIAGNOSIS: Week 2026-10-10")


# ---------------------------------------------------------------------------
# never mask the original failure
# ---------------------------------------------------------------------------
def test_bedrock_error_is_swallowed_not_raised(llm_on, session, saved):
    class Down:
        def converse(self, **kwargs):
            raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no model access"}}, "Converse")

    result = agent.triage_failed_load(LOAD, PIPELINE, "t", "e", client=Down())
    assert result.status == "error" and result.note is None and saved == []
    assert session.closed


def test_failure_to_save_the_note_is_swallowed(llm_on, session, monkeypatch):
    monkeypatch.setattr(agent.redshift, "run", lambda statements: (_ for _ in ()).throw(RuntimeError("1023")))
    result = agent.triage_failed_load(LOAD, PIPELINE, "t", "e", client=FakeBedrock([answer("DIAGNOSIS: x")]))
    assert result.status == "answered"


# ---------------------------------------------------------------------------
# read-only access
# ---------------------------------------------------------------------------
def test_every_triage_query_is_a_single_read_only_select():
    for name, sql in readonly.QUERIES.items():
        statements = [s for s in sqlglot.parse(sql, read="redshift") if s is not None]
        assert len(statements) == 1, name
        tree = statements[0]
        assert isinstance(tree, exp.Select), name
        assert not any(isinstance(n, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop))
                       for n in tree.walk()), name
        tables = {f"{t.db}.{t.name}" for t in tree.find_all(exp.Table)}
        assert tables <= set(readonly.TABLES), (name, tables)


def test_session_runs_named_queries_only_on_one_cached_connection(monkeypatch):
    connects, executed = [], []

    class Cursor:
        description = [("load_id",), ("status",)]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params):
            executed.append(sql)

        def fetchall(self):
            return [("L1", "FAILED")]

    class Conn:
        def cursor(self):
            return Cursor()

        def rollback(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(config, "TRIAGE_REDSHIFT_PASSWORD", "x")
    monkeypatch.setattr(readonly.redshift, "get_connection", lambda **kw: connects.append(kw["user"]) or Conn())
    s = readonly.ReadOnlySession()
    assert s.query("load_audit", load_id="L1") == [{"load_id": "L1", "status": "FAILED"}]
    s.query("load_audit", load_id="L1")                      # cached
    s.query("stats_by_week", load_id="L1")
    assert connects == ["triage_reader"] and len(executed) == 2 and s.queries_run == 2
    with pytest.raises(KeyError):
        s.query("SELECT * FROM fact.fact_sales")             # not a named query
    s.close()


# ---------------------------------------------------------------------------
# S3-backed tools
# ---------------------------------------------------------------------------
@pytest.fixture
def s3():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=config.S3_BUCKET)
        yield


def test_parse_rejects_and_unmapped_come_from_s3_without_redshift(s3):
    base = f"{config.S3_ENV_PREFIX}/rejects/C101/R201/{LOAD}"
    s3_io.put_bytes(f"{base}/parse_rejects.csv",
                    b"week_date,style,reject_reason\n,TOTAL,summary/total row\n2026-10-09,S01,week_date is not a Saturday\n")
    s3_io.put_bytes(f"{base}/unmapped_products.csv", b"src_product_key,rows,sales\n010199999999,5,250.0\n")
    session = FakeSession()
    box = tools.Toolbox(tools.TriageTarget(LOAD, PIPELINE), session)

    rejects = box.get_parse_rejects(max_rows=1)
    assert rejects["parse_rejects"] == 2 and len(rejects["sample"]) == 1
    assert rejects["by_reason"] == {"summary/total row": 1, "week_date is not a Saturday": 1}
    unmapped = box.get_unmapped_products()
    assert unmapped["source"].startswith("s3") and unmapped["unmapped_keys"][0]["src_product_key"] == "010199999999"
    assert session.queries_run == 0


def test_unmapped_falls_back_to_staging_when_no_file(s3):
    session = FakeSession({"unmapped_keys": [{"src_product_key": "S99", "rows": 3}]})
    box = tools.Toolbox(tools.TriageTarget(LOAD, PIPELINE), session)
    assert box.get_unmapped_products() == {"source": "stage.stg_sales",
                                           "unmapped_keys": [{"src_product_key": "S99", "rows": 3}]}
    assert box.get_parse_rejects() == {"parse_rejects": 0, "note": "the parser rejected no rows for this load"}


def test_model_thinking_tags_are_kept_out_of_the_note(llm_on, session):
    bedrock = FakeBedrock([answer("<thinking>internal reasoning</thinking>\nDIAGNOSIS: duplicate mapping.")])
    result = agent.triage(LOAD, PIPELINE, "t", "e", client=bedrock)
    assert "thinking" not in result.note and result.note.startswith("DIAGNOSIS: duplicate mapping.")
