"""The DAG's failure path: mark the load FAILED, then triage -- and never let
either step raise, so the task's own failure is what Airflow reports."""
import importlib
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
os.environ.setdefault("RETAIL_ENV", "dev")
os.environ["LLM_PROVIDER"] = "none"

factory = importlib.import_module("retail_sales_dag_factory")
LOAD = {"load_id": "C101_R201_X", "pipeline_id": "C101_R201"}


def context(task_id="sku_subflow.reconcile", load=LOAD):
    ti = SimpleNamespace(task_id=task_id, xcom_pull=lambda task_ids: load, dag_id="retail_sales__C101_R201__serial",
                         run_id="manual__2026-10-08", log_url="http://localhost:8080/log")
    return {"ti": ti, "exception": RuntimeError("src != stg + rej")}


def test_marks_failed_then_triages_then_emails_the_note(monkeypatch):
    calls = []
    monkeypatch.setattr("etl.steps.fail", lambda load_id, error: calls.append(("fail", load_id)))

    def triage(load_id, pipeline_id, task, error):
        calls.append(("triage", load_id, task))
        return SimpleNamespace(note="DIAGNOSIS: duplicate STM row.", status="answered")

    monkeypatch.setattr("triage.agent.triage_failed_load", triage)
    monkeypatch.setattr("etl.alerts.send_failure_email", lambda **kw: calls.append(("email", kw)))
    factory._mark_failed(context())
    assert [c[0] for c in calls] == ["fail", "triage", "email"]
    email = calls[2][1]
    assert email["triage_note"] == "DIAGNOSIS: duplicate STM row." and email["load_id"] == "C101_R201_X"
    assert email["task_id"] == "sku_subflow.reconcile" and email["log_url"] == "http://localhost:8080/log"


def test_errors_in_either_step_never_escape(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("redshift / bedrock down")

    monkeypatch.setattr("etl.steps.fail", boom)
    monkeypatch.setattr("triage.agent.triage_failed_load", boom)
    monkeypatch.setattr("etl.alerts.send_failure_email", boom)
    factory._mark_failed(context())          # must not raise


def test_no_load_yet_means_no_triage_but_still_an_email(monkeypatch):
    sent = []
    monkeypatch.setattr("triage.agent.triage_failed_load", lambda *a, **k: pytest.fail("no load to triage"))
    monkeypatch.setattr("etl.alerts.send_failure_email", lambda **kw: sent.append(kw))
    factory._mark_failed(context(task_id="begin", load=None))
    assert sent[0]["load_id"] is None and sent[0]["pipeline_id"] == "C101_R201" and sent[0]["triage_note"] is None
