"""Warehouse writes. Takes an open DB-API connection so the caller (the DAG, a
test, a script) decides where the connection comes from.

Deliberately driver-agnostic: apache-airflow-providers-postgres 6.x moved from
psycopg2 to psycopg 3, so PostgresHook.get_conn() returns a psycopg3 connection
while a test or script may hand in a psycopg2 one. Everything here sticks to
plain DB-API - execute, executemany, rowcount - rather than a driver's own
helpers. Using psycopg2.extras.execute_values on a psycopg3 connection fails
with "'Connection' object has no attribute 'encoding'".
"""

from __future__ import annotations

import numpy as np
import pandas as pd

SALES_COLUMNS = [
    "order_id",
    "order_ts",
    "order_date",
    "customer_id",
    "customer_name",
    "region",
    "country",
    "channel",
    "category",
    "product",
    "quantity",
    "unit_price",
    "discount_pct",
    "gross_amount",
    "net_amount",
    "source_file",
]

_SALES_PLACEHOLDERS = ", ".join(["%s"] * len(SALES_COLUMNS))

INSERT_SALES = f"""
INSERT INTO analytics.sales ({", ".join(SALES_COLUMNS)})
VALUES ({_SALES_PLACEHOLDERS})
ON CONFLICT (order_id) DO NOTHING
"""

# raw_record is jsonb. psycopg3 binds parameters server-side with an explicit
# type, so a Python str arrives as text and Postgres will not implicitly cast
# it; the ::jsonb makes the conversion explicit and works on both drivers.
INSERT_REJECTS = """
INSERT INTO analytics.sales_rejects (source_file, reason, raw_record)
VALUES (%s, %s, %s::jsonb)
"""

UPSERT_INGESTION = """
INSERT INTO ops.ingested_files
    (object_key, bucket, rows_raw, rows_loaded, rows_rejected, dag_run_id)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (object_key) DO UPDATE SET
    rows_raw      = EXCLUDED.rows_raw,
    rows_loaded   = EXCLUDED.rows_loaded,
    rows_rejected = EXCLUDED.rows_rejected,
    dag_run_id    = EXCLUDED.dag_run_id,
    ingested_at   = now()
"""

COUNT_BY_SOURCE = "SELECT COUNT(*) FROM analytics.sales WHERE source_file = %s"


def _to_python(value):
    """Neither psycopg2 nor psycopg3 can adapt numpy scalars or pandas NA."""
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):  # arrays and other non-scalars
        pass
    return value


def processed_keys(conn, bucket: str) -> set[str]:
    """Object keys this platform has already ingested from `bucket`."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT object_key FROM ops.ingested_files WHERE bucket = %s", (bucket,))
        return {row[0] for row in cursor.fetchall()}


def insert_sales(conn, frame: pd.DataFrame) -> int:
    """Insert clean rows, skipping any order_id already in the warehouse.

    Returns the number of rows actually inserted. rowcount after executemany is
    the total affected across the batch on both drivers, but it is advisory in
    DB-API, so fall back to counting the file's rows.
    """
    if frame.empty:
        return 0

    records = [
        tuple(_to_python(value) for value in row)
        for row in frame[SALES_COLUMNS].itertuples(index=False, name=None)
    ]
    source_file = records[0][SALES_COLUMNS.index("source_file")]

    with conn.cursor() as cursor:
        cursor.execute(COUNT_BY_SOURCE, (source_file,))
        before = cursor.fetchone()[0]

        cursor.executemany(INSERT_SALES, records)
        inserted = cursor.rowcount

        if inserted is None or inserted < 0:
            cursor.execute(COUNT_BY_SOURCE, (source_file,))
            inserted = cursor.fetchone()[0] - before
    return int(inserted)


def insert_rejects(conn, rejects: list[dict]) -> int:
    if not rejects:
        return 0
    records = [(r["source_file"], r["reason"], r["raw_record"]) for r in rejects]
    with conn.cursor() as cursor:
        cursor.executemany(INSERT_REJECTS, records)
    return len(records)


def record_ingestion(
    conn,
    object_key: str,
    bucket: str,
    rows_raw: int,
    rows_loaded: int,
    rows_rejected: int,
    dag_run_id: str | None = None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            UPSERT_INGESTION,
            (object_key, bucket, rows_raw, rows_loaded, rows_rejected, dag_run_id),
        )


def warehouse_stats(conn) -> dict:
    """Small snapshot used for logging and for the integration test."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(net_amount), 0), COUNT(DISTINCT source_file)
            FROM analytics.sales
            """
        )
        orders, revenue, files = cursor.fetchone()
    return {"orders": int(orders), "revenue": float(revenue), "source_files": int(files)}
