-- Warehouse schema. Runs inside the `analytics` database created in step 01.
\connect analytics

CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- ops: pipeline bookkeeping. The DAG uses this to know which MinIO objects it
-- has already consumed, which makes re-runs idempotent.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.ingested_files (
    object_key    TEXT PRIMARY KEY,
    bucket        TEXT        NOT NULL,
    rows_raw      INTEGER     NOT NULL DEFAULT 0,
    rows_loaded   INTEGER     NOT NULL DEFAULT 0,
    rows_rejected INTEGER     NOT NULL DEFAULT 0,
    dag_run_id    TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- analytics: the cleaned, structured fact table Metabase charts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.sales (
    order_id      TEXT           PRIMARY KEY,
    order_ts      TIMESTAMPTZ    NOT NULL,
    order_date    DATE           NOT NULL,
    customer_id   TEXT           NOT NULL,
    customer_name TEXT,
    region        TEXT           NOT NULL,
    country       TEXT,
    channel       TEXT           NOT NULL,
    category      TEXT           NOT NULL,
    product       TEXT           NOT NULL,
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount_pct  NUMERIC(5, 4)  NOT NULL DEFAULT 0 CHECK (discount_pct >= 0 AND discount_pct < 1),
    gross_amount  NUMERIC(14, 2) NOT NULL,
    net_amount    NUMERIC(14, 2) NOT NULL,
    source_file   TEXT           NOT NULL,
    loaded_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sales_order_date ON analytics.sales (order_date);
CREATE INDEX IF NOT EXISTS idx_sales_region     ON analytics.sales (region);
CREATE INDEX IF NOT EXISTS idx_sales_category   ON analytics.sales (category);
CREATE INDEX IF NOT EXISTS idx_sales_source     ON analytics.sales (source_file);

-- Rows that failed validation are kept, not dropped, so data quality itself is
-- something the dashboard can report on.
CREATE TABLE IF NOT EXISTS analytics.sales_rejects (
    reject_id   BIGSERIAL PRIMARY KEY,
    source_file TEXT        NOT NULL,
    reason      TEXT        NOT NULL,
    raw_record  JSONB       NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- KPI views consumed by the Metabase dashboard.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_kpi_summary AS
SELECT
    COUNT(*)                                        AS total_orders,
    COUNT(DISTINCT customer_id)                     AS total_customers,
    COALESCE(SUM(net_amount), 0)                    AS total_revenue,
    COALESCE(ROUND(AVG(net_amount), 2), 0)          AS avg_order_value,
    COALESCE(SUM(quantity), 0)                      AS units_sold,
    COALESCE(ROUND(AVG(discount_pct) * 100, 2), 0)  AS avg_discount_pct,
    MIN(order_date)                                 AS first_order_date,
    MAX(order_date)                                 AS last_order_date
FROM analytics.sales;

CREATE OR REPLACE VIEW analytics.v_daily_sales AS
SELECT
    order_date,
    COUNT(*)                       AS orders,
    SUM(quantity)                  AS units,
    ROUND(SUM(net_amount), 2)      AS revenue,
    ROUND(AVG(net_amount), 2)      AS avg_order_value
FROM analytics.sales
GROUP BY order_date
ORDER BY order_date;

CREATE OR REPLACE VIEW analytics.v_sales_by_region AS
SELECT
    region,
    COUNT(*)                  AS orders,
    ROUND(SUM(net_amount), 2) AS revenue,
    ROUND(100 * SUM(net_amount) / NULLIF(SUM(SUM(net_amount)) OVER (), 0), 2) AS revenue_share_pct
FROM analytics.sales
GROUP BY region
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW analytics.v_top_products AS
SELECT
    category,
    product,
    SUM(quantity)             AS units,
    ROUND(SUM(net_amount), 2) AS revenue
FROM analytics.sales
GROUP BY category, product
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW analytics.v_channel_performance AS
SELECT
    channel,
    order_date,
    COUNT(*)                  AS orders,
    ROUND(SUM(net_amount), 2) AS revenue
FROM analytics.sales
GROUP BY channel, order_date
ORDER BY order_date, channel;

CREATE OR REPLACE VIEW analytics.v_pipeline_health AS
SELECT
    f.object_key,
    f.bucket,
    f.rows_raw,
    f.rows_loaded,
    f.rows_rejected,
    ROUND(100.0 * f.rows_loaded / NULLIF(f.rows_raw, 0), 2) AS accept_rate_pct,
    f.dag_run_id,
    f.ingested_at
FROM ops.ingested_files f
ORDER BY f.ingested_at DESC;
