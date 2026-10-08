"""Pipeline Triage Agent: when a load fails (any task, including the reconcile
gate's src != stg + rej), investigate with read-only tools and write a
plain-English diagnosis and suggested fix for a person to approve.

A plain tool-use loop on the Amazon Bedrock Runtime Converse API (boto3,
toolConfig) -- no Bedrock Agents, AgentCore, Knowledge Bases or other services:

    user: "load X failed at task T with error E"
    loop (at most TRIAGE_MAX_STEPS model calls, TRIAGE_MAX_TOKENS for the run):
        model -> text and/or toolUse blocks
        toolUse -> run the read-only tool (triage/tools.py) -> toolResult back
        no toolUse -> that text is the diagnosis; stop

Guardrails:
  - tools are read-only and bound to the failed load; no free-form SQL
  - the note is a SUGGESTION: nothing here changes data, metadata or the DAG;
    a person reviews it (etl.load_audit.triage_note) and applies any fix
  - LLM_PROVIDER=none skips the agent entirely (CI, tests, fresh clones)
  - triage_failed_load() never raises, so an agent problem can't hide or
    replace the pipeline's own failure
"""
import logging
from dataclasses import dataclass, field

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from etl import config, redshift
from triage.readonly import ReadOnlySession
from triage.tools import TOOL_SPECS, Toolbox, TriageTarget

log = logging.getLogger(__name__)

MODELS = {
    "nova-lite": "us.amazon.nova-lite-v1:0",                           # default: cheapest that does tool use well
    "claude-haiku": "us.anthropic.claude-haiku-4-5-20251001-v1:0",     # switch: stronger reasoning, ~15x the price
}
MAX_OUTPUT_TOKENS = 1500     # per model call
NOTE_MAX_CHARS = 8000        # etl.load_audit.triage_note is VARCHAR(8000)

SYSTEM_PROMPT = """You are the triage assistant for a retail sell-through data pipeline (S3 files and a
retailer API -> parser -> Redshift landing -> staging mapped through a product mapping table (STM) ->
reconcile gate -> MERGE into a weekly fact table). A load just failed. Investigate it with the tools,
then explain what went wrong and what a person should do.

Rules:
- Use the tools to gather evidence; quote the numbers you rely on. Never guess or invent data.
- The tools are read-only. You cannot change anything, and your fix is only a suggestion that a
  person must approve.
- Be brief: call only the tools you need, then answer.

Common causes:
- Reconcile src != stg + rej with stg + rej LARGER than src: a source key matches more than one STM
  row (duplicate mapping), so the staging join multiplied rows.
- Reconcile with stg + rej SMALLER than src: rows were dropped between landing and staging.
- Parse errors: missing/renamed columns, bad dates, non-Saturday weeks, a file for the wrong
  client/retailer, rows newer than the load window (WindowError).
- Many unmapped products: new items not yet in the STM.
- Infrastructure: Redshift usage limit, connection timeouts, retailer API down, expired credentials.

Answer in plain text (no Markdown) with exactly these sections:
DIAGNOSIS: one or two sentences.
EVIDENCE: short bullet lines starting with "- ", each citing a tool result.
SUGGESTED FIX (needs human approval): concrete steps for a person.
CONFIDENCE: high, medium or low."""

FINAL_STEP_NUDGE = ("You have reached the step limit. Do not call any more tools: write your answer now, "
                    "in the required sections, from the evidence you already have.")


@dataclass
class TriageResult:
    status: str                    # answered | step_cap | token_cap | skipped | error
    note: str | None = None
    model: str | None = None
    tokens: int = 0
    steps: int = 0
    tool_calls: list[str] = field(default_factory=list)
    redshift_queries: int = 0


def enabled() -> bool:
    return config.LLM_PROVIDER != "none"


def model_id() -> str:
    """nova-lite / claude-haiku aliases, or any full Bedrock model id / inference profile."""
    return MODELS.get(config.TRIAGE_MODEL, config.TRIAGE_MODEL)


def bedrock_client():
    return boto3.client("bedrock-runtime", region_name=config.AWS_REGION,
                        config=Config(retries={"max_attempts": 3, "mode": "standard"}, read_timeout=60))


def _tool_result_block(tool_use_id: str, text: str, ok: bool, model: str) -> dict:
    block = {"toolUseId": tool_use_id, "content": [{"text": text}]}
    if "anthropic" in model:             # the status field is accepted by Claude models on Converse
        block["status"] = "success" if ok else "error"
    elif not ok:
        block["content"] = [{"text": "ERROR: " + text}]
    return {"toolResult": block}


def triage(load_id: str, pipeline_id: str, failed_task: str, error: str, client=None) -> TriageResult:
    """Investigate one failed load. Raises only on Bedrock/AWS errors (see triage_failed_load)."""
    if not enabled():
        return TriageResult(status="skipped", note=None)

    model = model_id()
    session = ReadOnlySession()
    tools = Toolbox(TriageTarget(load_id=load_id, pipeline_id=pipeline_id), session)
    bedrock = client or bedrock_client()
    result = TriageResult(status="error", model=model)
    messages = [{"role": "user", "content": [{"text":
        f"Load {load_id} of pipeline {pipeline_id} failed at task '{failed_task}'.\n"
        f"Error: {error[:1500]}\nInvestigate and diagnose it."}]}]
    last_text = ""
    try:
        for step in range(1, config.TRIAGE_MAX_STEPS + 1):
            if result.tokens >= config.TRIAGE_MAX_TOKENS:
                result.status = "token_cap"
                break
            if step == config.TRIAGE_MAX_STEPS:
                messages[-1]["content"].append({"text": FINAL_STEP_NUDGE})

            response = bedrock.converse(
                modelId=model,
                system=[{"text": SYSTEM_PROMPT}],
                messages=messages,
                toolConfig={"tools": [{"toolSpec": spec} for spec in TOOL_SPECS]},
                inferenceConfig={"maxTokens": MAX_OUTPUT_TOKENS, "temperature": 0},
            )
            result.steps = step
            usage = response.get("usage", {})
            result.tokens += usage.get("inputTokens", 0) + usage.get("outputTokens", 0)

            message = response["output"]["message"]
            messages.append(message)
            texts = [c["text"] for c in message["content"] if "text" in c]
            if texts:
                last_text = "\n".join(texts).strip()
            tool_uses = [c["toolUse"] for c in message["content"] if "toolUse" in c]

            if response.get("stopReason") != "tool_use" or not tool_uses:
                result.status, result.note = "answered", last_text
                break
            if step == config.TRIAGE_MAX_STEPS:
                result.status = "step_cap"
                break

            blocks = []
            for tu in tool_uses:
                text, ok = tools.run(tu["name"], tu.get("input"))
                log.info("triage step %d: %s(%s) -> %s, %d chars", step, tu["name"], tu.get("input") or "",
                         "ok" if ok else "error", len(text))
                blocks.append(_tool_result_block(tu["toolUseId"], text, ok, model))
            messages.append({"role": "user", "content": blocks})
        else:
            result.status = "step_cap"
    finally:
        session.close()
        result.tool_calls = tools.calls
        result.redshift_queries = session.queries_run

    if result.status in ("step_cap", "token_cap"):
        cap = (f"{config.TRIAGE_MAX_STEPS} steps" if result.status == "step_cap"
               else f"{config.TRIAGE_MAX_TOKENS} tokens")
        result.note = (f"Triage stopped at its {cap} limit before a final diagnosis. Partial findings: "
                       f"{last_text or 'none'}")
    if result.note:
        result.note = (f"{result.note}\n\n[Triage agent, {result.status}: {result.steps} steps, "
                       f"{result.tokens} tokens, tools {', '.join(result.tool_calls) or 'none'}. "
                       f"Suggested fixes need human approval.]")[:NOTE_MAX_CHARS]
    return result


def record(load_id: str, result: TriageResult) -> None:
    """Write the note to the audit row, as the ETL user (the triage user can't write)."""
    redshift.run([("""UPDATE etl.load_audit
                      SET triage_note = %(note)s, triage_model = %(model)s, triage_tokens = %(tokens)s
                      WHERE load_id = %(load_id)s;""",
                   {"note": result.note, "model": result.model, "tokens": result.tokens, "load_id": load_id})])


def triage_failed_load(load_id: str, pipeline_id: str, failed_task: str, error: str,
                       client=None) -> TriageResult | None:
    """The failure-path entry point (DAG callback, CLI). NEVER raises: an agent,
    Bedrock or Redshift problem is logged and returned as status=error, so the
    pipeline's own failure is what Airflow and the audit row keep showing."""
    try:
        result = triage(load_id, pipeline_id, failed_task, error, client=client)
    except (ClientError, BotoCoreError) as e:
        log.warning("triage of %s skipped: Bedrock unavailable (%s)", load_id, e)
        return TriageResult(status="error", model=model_id(), note=None)
    except Exception:
        log.exception("triage of %s failed; the original pipeline failure is unaffected", load_id)
        return TriageResult(status="error", model=model_id(), note=None)
    if result.status == "skipped":
        log.info("triage of %s skipped: LLM_PROVIDER=none", load_id)
        return result
    try:
        if result.note:
            record(load_id, result)
    except Exception:
        log.exception("could not save the triage note for %s", load_id)
    log.info("triage of %s: %s (%d steps, %d tokens, %d Redshift queries)", load_id, result.status,
             result.steps, result.tokens, result.redshift_queries)
    return result
