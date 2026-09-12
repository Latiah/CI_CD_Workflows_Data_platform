"""End-to-end data flow validation against a running stack.

    MinIO (ingestion) -> Airflow (processing) -> PostgreSQL (storage) -> Metabase (API)

Requires `docker compose up -d` first. Run with:

    pytest tests/integration -v
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
import uuid

import boto3
import psycopg2
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError

from data_generator.generate import generate_rows, to_csv
from pipeline.transform import transform_bytes

DAG_ID = "sales_ingestion"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw-data")
MINIO_PREFIX = os.environ.get("MINIO_PREFIX", "sales/").strip("/")

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
AIRFLOW_USER = os.environ.get("AIRFLOW_ADMIN_USER", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "airflow")

METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")
MB_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
MB_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "Metabase123!")

PG_DSN = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("ANALYTICS_DB", "analytics"),
    "user": os.environ.get("POSTGRES_USER", "platform"),
    "password": os.environ.get("POSTGRES_PASSWORD", "platform"),
}

DAG_RUN_TIMEOUT = int(os.environ.get("DAG_RUN_TIMEOUT", "900"))


def trigger_body(run_id: str) -> dict:
    """Airflow 3 requires logical_date in the body; null means an event-driven
    manual run with no data interval, which is what this pipeline wants."""
    return {"dag_run_id": run_id, "logical_date": None, "conf": {}}


# --------------------------------------------------------------------------- helpers
_token_cache: dict[str, str] = {}


def airflow_token() -> str:
    """Airflow 3 drops Basic auth: the REST API takes a JWT from /auth/token."""
    if "value" not in _token_cache:
        data = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
        request = urllib.request.Request(f"{AIRFLOW_URL}/auth/token", data=data, method="POST")
        request.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(request, timeout=60) as response:
            _token_cache["value"] = json.loads(response.read().decode())["access_token"]
    return _token_cache["value"]


def airflow_request(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(f"{AIRFLOW_URL}{path}", data=data, method=method)
    request.add_header("Content-Type", "application/json")
    request.add_header("Authorization", f"Bearer {airflow_token()}")
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode()
    return json.loads(body) if body else {}


def metabase_request(method: str, path: str, payload: dict | None = None, token: str | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(f"{METABASE_URL}{path}", data=data, method=method)
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("X-Metabase-Session", token)
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode()
    return json.loads(body) if body else {}


def http_json(url: str, timeout: int = 30) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode())


def wait_until(predicate, timeout: int, interval: int, description: str):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001 - service may still be starting
            last = exc
        time.sleep(interval)
    pytest.fail(f"Timed out after {timeout}s waiting for {description} (last result: {last})")


# --------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="session")
def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")
def pg():
    connection = wait_until(
        lambda: psycopg2.connect(**PG_DSN),
        timeout=180,
        interval=5,
        description="PostgreSQL to accept connections",
    )
    connection.autocommit = True
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def uploaded_batch(s3):
    """Ingestion step: push a known CSV batch into the MinIO landing bucket."""
    wait_until(
        lambda: s3.list_buckets() is not None,
        timeout=180,
        interval=5,
        description="MinIO to become reachable",
    )

    rows = generate_rows(750, seed=2024)
    payload = to_csv(rows)
    key = f"{MINIO_PREFIX}/e2e_{uuid.uuid4().hex[:10]}.csv"
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=payload.encode(), ContentType="text/csv")

    expected = transform_bytes(payload, source_file=key)
    return {"key": key, "rows_raw": len(rows), "expected": expected}


@pytest.fixture(scope="session")
def dag_run(uploaded_batch):
    """Processing step: trigger the DAG and wait for it to finish."""
    wait_until(
        lambda: http_json(f"{AIRFLOW_URL}/api/v2/monitor/health")["scheduler"]["status"] == "healthy",
        timeout=300,
        interval=5,
        description="the Airflow scheduler to report healthy",
    )

    airflow_request("PATCH", f"/api/v2/dags/{DAG_ID}", {"is_paused": False})
    run_id = f"e2e__{uuid.uuid4().hex[:8]}"
    airflow_request("POST", f"/api/v2/dags/{DAG_ID}/dagRuns", trigger_body(run_id))

    def finished():
        state = airflow_request("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}")["state"]
        return state if state in {"success", "failed"} else None

    state = wait_until(finished, timeout=DAG_RUN_TIMEOUT, interval=10, description=f"DAG run {run_id}")
    if state != "success":
        tasks = airflow_request("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        failed = [(t["task_id"], t["state"]) for t in tasks["task_instances"] if t["state"] == "failed"]
        pytest.fail(f"DAG run {run_id} ended as {state}; failed tasks: {failed}")
    return run_id


# --------------------------------------------------------------------------- tests
def test_1_minio_accepts_the_raw_file(s3, uploaded_batch):
    head = s3.head_object(Bucket=MINIO_BUCKET, Key=uploaded_batch["key"])
    assert head["ContentLength"] > 0


def test_2_airflow_processes_the_file(dag_run):
    assert dag_run  # the fixture fails loudly if the run did not succeed


def test_3_postgres_stores_the_cleaned_rows(pg, uploaded_batch, dag_run):
    key = uploaded_batch["key"]
    expected = uploaded_batch["expected"]

    with pg.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM analytics.sales WHERE source_file = %s", (key,))
        loaded = cursor.fetchone()[0]

    assert (
        loaded == expected.rows_loaded
    ), f"expected {expected.rows_loaded} clean rows from {key}, found {loaded}"


def test_4_ingestion_is_recorded_for_idempotency(pg, uploaded_batch, dag_run):
    with pg.cursor() as cursor:
        cursor.execute(
            "SELECT rows_raw, rows_loaded, rows_rejected FROM ops.ingested_files WHERE object_key = %s",
            (uploaded_batch["key"],),
        )
        record = cursor.fetchone()

    assert record is not None, "the pipeline did not record the file in ops.ingested_files"
    rows_raw, rows_loaded, rows_rejected = record
    assert rows_raw == uploaded_batch["rows_raw"]
    assert rows_loaded + rows_rejected == rows_raw


def test_5_bad_rows_were_quarantined_not_silently_dropped(pg, uploaded_batch, dag_run):
    expected_rejects = uploaded_batch["expected"].rows_rejected
    if expected_rejects == 0:
        pytest.skip("this batch happened to contain no defective rows")

    with pg.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM analytics.sales_rejects WHERE source_file = %s",
            (uploaded_batch["key"],),
        )
        assert cursor.fetchone()[0] == expected_rejects


def test_6_derived_amounts_are_consistent(pg, dag_run):
    with pg.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) FROM analytics.sales
            WHERE ABS(gross_amount - (quantity * unit_price)) > 0.01
               OR ABS(net_amount - (gross_amount * (1 - discount_pct))) > 0.01
            """
        )
        assert cursor.fetchone()[0] == 0, "found rows whose revenue maths does not add up"


def test_7_kpi_views_return_data_for_metabase(pg, dag_run):
    views = [
        "v_kpi_summary",
        "v_daily_sales",
        "v_sales_by_region",
        "v_top_products",
        "v_channel_performance",
        "v_pipeline_health",
    ]
    with pg.cursor() as cursor:
        for view in views:
            cursor.execute(f"SELECT COUNT(*) FROM analytics.{view}")
            assert cursor.fetchone()[0] > 0, f"analytics.{view} returned no rows"

        cursor.execute("SELECT total_orders, total_revenue FROM analytics.v_kpi_summary")
        orders, revenue = cursor.fetchone()

    assert orders > 0 and revenue > 0


def test_8_archived_object_left_the_landing_zone(s3, uploaded_batch, dag_run):
    key = uploaded_batch["key"]
    listing = s3.list_objects_v2(Bucket=f"{MINIO_BUCKET}-archive", Prefix=key)
    assert listing.get("KeyCount", 0) == 1, "processed file was not archived"

    with pytest.raises(ClientError) as excinfo:
        s3.head_object(Bucket=MINIO_BUCKET, Key=key)
    assert excinfo.value.response["Error"]["Code"] in {"404", "NoSuchKey"}


def test_9_metabase_api_is_healthy_and_sees_the_warehouse():
    health = wait_until(
        lambda: http_json(f"{METABASE_URL}/api/health"),
        timeout=300,
        interval=10,
        description="Metabase /api/health",
    )
    assert health.get("status") == "ok"

    properties = http_json(f"{METABASE_URL}/api/session/properties")
    if properties.get("setup-token"):
        pytest.skip("Metabase is not provisioned yet - run `make metabase` first")

    # Log in and confirm the warehouse connection the pipeline feeds is registered.
    token = metabase_request("POST", "/api/session", {"username": MB_EMAIL, "password": MB_PASSWORD})["id"]
    listing = metabase_request("GET", "/api/database", token=token)
    databases = listing.get("data", listing) if isinstance(listing, dict) else listing

    warehouses = [d for d in databases if d.get("engine") == "postgres"]
    assert (
        warehouses
    ), f"Metabase has no PostgreSQL connection registered: {[d.get('name') for d in databases]}"


def test_10_rerunning_the_same_file_inserts_nothing_new(pg, uploaded_batch, dag_run):
    """Idempotency: the pipeline must not double-count an already-ingested file."""
    key = uploaded_batch["key"]
    with pg.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM analytics.sales WHERE source_file = %s", (key,))
        before = cursor.fetchone()[0]

    run_id = f"e2e_repeat__{uuid.uuid4().hex[:8]}"
    airflow_request("POST", f"/api/v2/dags/{DAG_ID}/dagRuns", trigger_body(run_id))
    wait_until(
        lambda: airflow_request("GET", f"/api/v2/dags/{DAG_ID}/dagRuns/{run_id}")["state"]
        in {"success", "failed"},
        timeout=DAG_RUN_TIMEOUT,
        interval=10,
        description=f"repeat DAG run {run_id}",
    )

    with pg.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM analytics.sales WHERE source_file = %s", (key,))
        after = cursor.fetchone()[0]

    assert after == before, "re-running the pipeline duplicated rows"
