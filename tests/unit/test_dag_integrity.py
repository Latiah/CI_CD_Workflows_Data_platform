"""DAG-level checks: the file must import, parse, and be wired the way we think.

Skipped automatically when Airflow is not installed (a plain local venv), which
is why CI also runs this file inside the Airflow image — see the `dag-integrity`
job. There it is a real check rather than a silent skip.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow not installed in this environment")

try:
    # Airflow 3: DagBag lives here. The airflow.models.dagbag path still imports
    # but resolves to the DB-backed bag, whose __init__ takes different
    # arguments entirely.
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # pragma: no cover - Airflow 2 fallback
    from airflow.models.dagbag import DagBag

DAG_ID = "sales_ingestion"

# Resolved from this file rather than the cwd, so the same test works from the
# repo root and from /opt/airflow inside the image.
DAG_FOLDER = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    # No include_examples: it was removed in Airflow 3. Examples are governed by
    # core.load_examples, and pointing at our own folder excludes them anyway.
    bag = DagBag(dag_folder=str(DAG_FOLDER))
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag


def test_dags_import_without_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_sales_dag_is_registered(dagbag):
    assert DAG_ID in dagbag.dags, f"found instead: {list(dagbag.dags)}"


def test_task_graph_matches_the_documented_flow(dagbag):
    dag = dagbag.dags[DAG_ID]
    task_ids = set(dag.task_ids)

    assert {"list_new_files", "process_file", "summarise"} <= task_ids
    # list_new_files is the detector and the root of the graph.
    assert dag.get_task("list_new_files").upstream_task_ids == set()
    assert "list_new_files" in dag.get_task("process_file").upstream_task_ids
    assert "process_file" in dag.get_task("summarise").upstream_task_ids


def test_schedule_follows_the_environment(dagbag):
    """The cadence is configurable so CI can run the DAG manual-only.

    A scheduled run can otherwise consume an uploaded file before the
    end-to-end suite triggers its own run, making that suite nondeterministic.
    This guards the plumbing: hardcoding the cron again would fail here.
    """
    import os

    dag = dagbag.dags[DAG_ID]
    configured = os.environ.get("SALES_DAG_SCHEDULE", "*/10 * * * *").strip()
    expected = None if configured.lower() in {"", "none", "manual"} else configured

    assert (
        dag.schedule == expected
    ), f"SALES_DAG_SCHEDULE={configured!r} should give schedule={expected!r}, got {dag.schedule!r}"


def test_dag_has_retries_and_no_catchup(dagbag):
    dag = dagbag.dags[DAG_ID]

    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # Asserted on a task rather than dag.default_args: default_args is an
    # authoring-time convenience, and what matters is that it reached the tasks.
    assert dag.get_task("process_file").retries >= 1
