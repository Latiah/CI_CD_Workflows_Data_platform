-- Three logical databases on one Postgres instance:
--   airflow   -> Airflow metadata
--   analytics -> the warehouse Metabase reads from
--   metabase  -> Metabase application state
CREATE DATABASE airflow;
CREATE DATABASE analytics;
CREATE DATABASE metabase;
