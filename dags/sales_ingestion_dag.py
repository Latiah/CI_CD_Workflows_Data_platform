"""MinIO -> Airflow -> PostgreSQL sales pipeline.

1. Sense new CSV objects landing in the MinIO raw bucket.
2. List the ones this platform has not ingested yet (bookkeeping in Postgres).
3. Clean, validate and transform each file, then load it into the warehouse.
4. Archive the processed object and log a run summary.

Every step is idempotent: re-running a file inserts nothing new, because
`analytics.sales` is keyed on order_id and `ops.ingested_files` records what has
already been consumed.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Airflow 3 moves the DAG authoring surface into the Task SDK; the old
# airflow.decorators paths still resolve but are on their way out.
from airflow.sdk import dag, get_current_context, task

from pipeline import transform as tf
from pipeline import warehouse as wh

LOG = logging.getLogger(__name__)

AWS_CONN_ID = "minio_default"
PG_CONN_ID = "analytics_db"
BUCKET = os.environ.get("MINIO_BUCKET", "raw-data")
ARCHIVE_BUCKET = f"{BUCKET}-archive"
PREFIX = os.environ.get("MINIO_PREFIX", "sales/")
MAX_FILES_PER_RUN = int(os.environ.get("MAX_FILES_PER_RUN", "25"))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}


@dag(
    dag_id="sales_ingestion",
    description="Ingest sales CSVs from MinIO, clean them, and load into PostgreSQL",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=30),
    tags=["minio", "postgres", "etl", "sales"],
    # AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false (set for local/dev
    # convenience) means every DAG comes up active the moment it's parsed. For
    # a cron-scheduled DAG that races with the e2e test: on a fresh deploy the
    # scheduler can fire its own automatic run before the test uploads its
    # file, or between the upload and the test's own trigger - Airflow then
    # correctly reports the test's run as a no-op (list_new_files finds
    # nothing left to do) even though the file loaded fine via the other run.
    # Overriding it here means nothing runs until it is explicitly unpaused -
    # which the test already does right before it triggers - so the test owns
    # timing deterministically. A real deployment unpauses it once, after
    # which it behaves as a normal polling pipeline.
    is_paused_upon_creation=True,
)
def sales_ingestion():
    # ------------------------------------------------------------ detect
    # This task is the detector. An S3KeySensor was tried here first and
    # removed: it cannot distinguish a genuinely new object from one already
    # ingested, and with soft_fail=True a real failure (bad credentials, wrong
    # endpoint) is reported as a skip, which cascades downstream and leaves the
    # DAG run green with nothing loaded. Listing the bucket and subtracting
    # ops.ingested_files answers the real question and fails loudly when it
    # cannot reach MinIO.
    @task
    def list_new_files() -> list[str]:
        """Objects present in MinIO that Postgres has no ingestion record for."""
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = hook.list_keys(bucket_name=BUCKET, prefix=PREFIX) or []
        candidates = sorted(key for key in keys if key.lower().endswith(".csv"))

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        try:
            already_done = wh.processed_keys(conn, BUCKET)
        finally:
            conn.close()

        new_keys = [key for key in candidates if key not in already_done][:MAX_FILES_PER_RUN]
        LOG.info("Found %s object(s), %s new: %s", len(candidates), len(new_keys), new_keys)

        # Returning an empty list (rather than skipping) keeps the mapped task's
        # XCom resolvable, so the summary task still runs on a quiet cycle.
        return new_keys

    # -------------------------------------------------------- process + load
    @task(max_active_tis_per_dag=4)
    def process_file(object_key: str) -> dict:
        """Download one object, clean it, and load the result into Postgres."""
        run_id = get_current_context()["run_id"]
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        payload = s3.read_key(key=object_key, bucket_name=BUCKET)

        result = tf.transform_bytes(payload, source_file=object_key)
        summary = result.summary()
        LOG.info("Transformed %s: %s", object_key, summary)

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        try:
            inserted = wh.insert_sales(conn, result.clean)
            wh.insert_rejects(conn, result.rejects)
            wh.record_ingestion(
                conn,
                object_key=object_key,
                bucket=BUCKET,
                rows_raw=result.rows_raw,
                rows_loaded=inserted,
                rows_rejected=result.rows_rejected,
                dag_run_id=run_id,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        # Move the object aside so the landing zone stays small and the sensor
        # keeps reacting to genuinely new arrivals.
        try:
            s3.copy_object(
                source_bucket_key=object_key,
                dest_bucket_key=object_key,
                source_bucket_name=BUCKET,
                dest_bucket_name=ARCHIVE_BUCKET,
            )
            s3.delete_objects(bucket=BUCKET, keys=[object_key])
        except Exception as exc:  # archiving must never lose a successful load
            LOG.warning("Could not archive %s: %s", object_key, exc)

        return {"object_key": object_key, "rows_inserted": inserted, **summary}

    # -------------------------------------------------------------- report
    # none_failed, not all_done: a quiet cycle (process_file mapped over an empty
    # list, so skipped) should still produce a summary, but a failed load must
    # not be papered over. summarise is the only leaf task, so with all_done a
    # failed process_file still left the DAG run marked "success".
    @task(trigger_rule="none_failed")
    def summarise(results: list[dict]) -> dict:
        results = [r for r in results if r]
        totals = {
            "files": len(results),
            "rows_raw": sum(r["rows_raw"] for r in results),
            "rows_inserted": sum(r["rows_inserted"] for r in results),
            "rows_rejected": sum(r["rows_rejected"] for r in results),
        }

        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn = pg.get_conn()
        try:
            conn.autocommit = True
            # ANALYZE keeps Metabase's queries on fresh statistics after a bulk load.
            with conn.cursor() as cursor:
                cursor.execute("ANALYZE analytics.sales")
            totals["warehouse"] = wh.warehouse_stats(conn)
        finally:
            conn.close()

        LOG.info("Run summary at %s: %s", datetime.now(UTC).isoformat(), totals)
        return totals

    summarise(process_file.expand(object_key=list_new_files()))


sales_ingestion()