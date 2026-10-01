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


def test_redshift_writers_share_one_pool(dagbag):
    dag = dagbag.dags["retail_sales__C101_R202__sku"]
    for tid in ["sku_subflow.load_tmp", "sku_subflow.load_staging", "merge_fact"]:
        assert dag.get_task(tid).pool == "redshift"


def test_s3_sensor_skips_instead_of_failing(dagbag):
    sensor = dagbag.dags["retail_sales__C101_R201__serial"].get_task("wait_for_file")
    assert sensor.soft_fail and sensor.mode == "reschedule"
