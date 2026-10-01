"""DAG integrity: every DAG imports, there is one per active pipeline, and
each one has the right subflow for its product level and source type."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("RETAIL_ENV", "dev")

from airflow.models.dagbag import DagBag  # noqa: E402

from etl import metadata  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(ROOT / "airflow" / "dags"))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_one_dag_per_active_pipeline(dagbag):
    expected = {f"retail_sales__{p['pipeline_id']}__{p['product_level']}" for p in metadata.active_pipelines()}
    assert expected <= set(dagbag.dags)
    assert set(dagbag.dags) - expected == {"retail_metadata_sync"}


@pytest.mark.parametrize("dag_id,first,has_prestg", [
    ("retail_sales__C101_R201__serial", "wait_for_file", False),
    ("retail_sales__C101_R202__sku", "wait_for_file", True),
    ("retail_sales__C101_R203__style", "wait_for_file", True),
    ("retail_sales__C102_R202__sku", "begin", True),
])
def test_subflow_shape(dagbag, dag_id, first, has_prestg):
    dag = dagbag.dags[dag_id]
    level = dag_id.rsplit("__", 1)[1]
    ids = {t.task_id for t in dag.tasks}
    assert [t.task_id for t in dag.roots] == [first]
    assert (f"{level}_subflow.load_prestg" in ids) == has_prestg
    assert ("extract_api" in ids) == (first == "begin")
    # Nothing reaches the fact table without passing reconcile.
    assert f"{level}_subflow.reconcile" in dag.get_task("merge_fact").upstream_task_ids


@pytest.mark.parametrize("dag_id", ["retail_sales__C101_R201__serial", "retail_sales__C101_R202__sku",
                                    "retail_sales__C101_R203__style", "retail_sales__C102_R202__sku"])
def test_every_redshift_writer_is_in_the_pool(dagbag, dag_id):
    """Only pure S3/API tasks may run outside the 1-slot redshift pool."""
    for t in dagbag.dags[dag_id].tasks:
        expected = "default_pool" if t.task_id in {"wait_for_file", "extract_api", "parse", "archive"} else "redshift"
        assert t.pool == expected, t.task_id


def test_later_steps_outrank_earlier_ones(dagbag):
    """With weight_rule=upstream, an in-flight load's merge beats another
    pipeline's first load step when both wait for the redshift slot."""
    dag = dagbag.dags["retail_sales__C101_R202__sku"]
    weight = {t.task_id: len(t.get_flat_relative_ids(upstream=True)) for t in dag.tasks}
    assert all(str(t.weight_rule).lower().endswith("upstream") for t in dag.tasks)
    assert weight["merge_fact"] > weight["sku_subflow.reconcile"] > weight["sku_subflow.load_tmp"] > weight["begin"]


def test_s3_sensor_skips_instead_of_failing(dagbag):
    sensor = dagbag.dags["retail_sales__C101_R201__serial"].get_task("wait_for_file")
    assert sensor.soft_fail and sensor.mode == "reschedule"
