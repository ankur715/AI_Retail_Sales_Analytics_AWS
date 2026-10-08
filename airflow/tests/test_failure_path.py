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
    ti = SimpleNamespace(task_id=task_id, xcom_pull=lambda task_ids: load)
    return {"ti": ti, "exception": RuntimeError("src != stg + rej")}


def test_marks_failed_then_triages(monkeypatch):
    calls = []
    monkeypatch.setattr("etl.steps.fail", lambda load_id, error: calls.append(("fail", load_id)))
    monkeypatch.setattr("triage.agent.triage_failed_load",
                        lambda load_id, pipeline_id, task, error: calls.append(("triage", load_id, task)))
    factory._mark_failed(context())
    assert calls == [("fail", "C101_R201_X"), ("triage", "C101_R201_X", "sku_subflow.reconcile")]


def test_errors_in_either_step_never_escape(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("redshift / bedrock down")

    monkeypatch.setattr("etl.steps.fail", boom)
    monkeypatch.setattr("triage.agent.triage_failed_load", boom)
    factory._mark_failed(context())          # must not raise


def test_no_load_yet_means_nothing_to_triage(monkeypatch):
    monkeypatch.setattr("triage.agent.triage_failed_load", lambda *a, **k: pytest.fail("no load to triage"))
    factory._mark_failed(context(task_id="wait_for_file", load=None))
