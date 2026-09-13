"""DAG-level checks: the file must import, parse, and be wired the way we think.

Skipped automatically when Airflow is not installed (e.g. a plain local venv),
so the unit suite still runs everywhere.
"""

from __future__ import annotations

import pytest

pytest.importorskip("airflow", reason="Airflow not installed in this environment")

from airflow.models import DagBag  # noqa: E402

DAG_ID = "sales_ingestion"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_sales_dag_is_registered(dagbag):
    assert DAG_ID in dagbag.dags


def test_task_graph_matches_the_documented_flow(dagbag):
    dag = dagbag.dags[DAG_ID]
    task_ids = set(dag.task_ids)

    assert {"list_new_files", "process_file", "summarise"} <= task_ids
    # list_new_files is the detector and the root of the graph.
    assert dag.get_task("list_new_files").upstream_task_ids == set()
    assert "list_new_files" in dag.get_task("process_file").upstream_task_ids
    assert "process_file" in dag.get_task("summarise").upstream_task_ids


def test_dag_has_retries_and_no_catchup(dagbag):
    dag = dagbag.dags[DAG_ID]

    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] >= 1
