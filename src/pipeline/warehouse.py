"""Warehouse writes. Takes an open psycopg2 connection so the caller (the DAG,
a test, a script) decides where the connection comes from."""

from __future__ import annotations

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

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

INSERT_SALES = f"""
INSERT INTO analytics.sales ({", ".join(SALES_COLUMNS)})
VALUES %s
ON CONFLICT (order_id) DO NOTHING
RETURNING 1
"""

INSERT_REJECTS = """
INSERT INTO analytics.sales_rejects (source_file, reason, raw_record)
VALUES %s
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


def _to_python(value):
    """psycopg2 cannot adapt numpy scalars or pandas NA — convert to natives."""
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
    """Insert clean rows, skipping any order_id already in the warehouse."""
    if frame.empty:
        return 0
    records = [
        tuple(_to_python(value) for value in row)
        for row in frame[SALES_COLUMNS].itertuples(index=False, name=None)
    ]
    with conn.cursor() as cursor:
        # fetch=True aggregates RETURNING across every page, so the count stays
        # accurate for files larger than one page.
        inserted = execute_values(cursor, INSERT_SALES, records, page_size=1000, fetch=True)
        return len(inserted)


def insert_rejects(conn, rejects: list[dict]) -> int:
    if not rejects:
        return 0
    records = [(r["source_file"], r["reason"], r["raw_record"]) for r in rejects]
    with conn.cursor() as cursor:
        execute_values(cursor, INSERT_REJECTS, records, page_size=1000)
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
