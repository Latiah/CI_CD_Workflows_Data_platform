# Mini Data Platform (Dockerized)

An end-to-end data platform run entirely by Docker Compose. Synthetic sales data
lands in object storage, Airflow cleans and loads it into a relational
warehouse, and Metabase charts the result — with CI/CD that proves the whole
path still works on every commit.

```
  data_generator ──▶  MinIO  ──▶  Airflow  ──▶  PostgreSQL  ──▶  Metabase
   (synthetic CSV)   (raw-data)   (clean +       (analytics.*     (KPIs &
                                   validate)      warehouse)       trends)
```

| Component | Technology | Purpose | URL |
| :-- | :-- | :-- | :-- |
| Database | PostgreSQL 16 | Structured storage for processed data | `localhost:5432` |
| Processing | Apache Airflow 3.3 | Orchestration and pipeline execution | http://localhost:8080 |
| Storage | MinIO | S3-compatible object storage for raw CSVs | http://localhost:9001 |
| Dashboards | Metabase | Charts and reporting | http://localhost:3000 |

---

## Quick start

```bash
cp .env.example .env         # optional: every value has a working default
docker compose up -d --build # start the platform

docker compose --profile setup run --rm metabase-init   # provision Metabase
docker compose run --rm data-generator --rows 2000      # drop a batch into MinIO
```

Or, with `make`:

```bash
make up && make metabase && make seed
```

Airflow picks the file up within ten minutes on its own schedule; to see it
immediately, open http://localhost:8080 and trigger the **`sales_ingestion`**
DAG, or run:

```bash
docker compose exec airflow-dag-processor airflow dags trigger sales_ingestion
```

Then check the warehouse:

```bash
docker compose exec postgres psql -U platform -d analytics -c "SELECT * FROM analytics.v_kpi_summary;"
```

**Default credentials** (change them for anything but local use):

| Service | User | Password |
| :-- | :-- | :-- |
| Airflow | `airflow` | `airflow` |
| MinIO Console | `minioadmin` | `minioadmin` |
| Metabase | `admin@example.com` | `Metabase123!` |
| PostgreSQL | `platform` | `platform` |

---

## Part 1 — Infrastructure

All four services, plus two one-shot helpers, live in
[docker-compose.yml](docker-compose.yml) on a single `data-platform` bridge
network, so Airflow reaches MinIO at `http://minio:9000` and PostgreSQL at
`postgres:5432` by service name — no host ports involved.

**Persistence** — five named volumes survive `docker compose down`:
`postgres-data`, `minio-data`, `metabase-data`, `airflow-logs`,
`airflow-plugins`. Use `make clean` (`docker compose down -v`) to wipe them.

**One Postgres, three databases** — created by
[config/postgres/init/01-create-databases.sql](config/postgres/init/01-create-databases.sql):
`airflow` (Airflow metadata), `analytics` (the warehouse), and `metabase`
(Metabase's own application state, so it never falls back to ephemeral H2).

**Connections without clicking** — Airflow's MinIO and Postgres connections are
injected as `AIRFLOW_CONN_*` environment variables, so a fresh stack is ready to
run with no manual setup in the UI.

**Airflow 3 topology** — five components, not two. `airflow-init` migrates the
metadata database and creates the admin user through the image entrypoint;
`airflow-apiserver` serves the UI, the REST API and the Task Execution API;
`airflow-dag-processor` parses `dags/` into serialised DAGs (its own service in
Airflow 3 — without it the DAG never appears); `airflow-scheduler` decides what
runs and, under LocalExecutor, runs it; `airflow-triggerer` handles deferred
work.

**Startup ordering** — `depends_on` conditions plus health checks mean
`airflow-scheduler` only starts after Postgres is accepting connections, the
MinIO buckets exist, `airflow-init` has migrated the metadata database, and the
api-server is healthy. That last one matters: in Airflow 3 task code reaches
Airflow through the Task Execution API rather than the metadata database, so a
task cannot start without it — even under LocalExecutor.

---

## Part 2 — The data engineering pipeline

### Ingestion

[data_generator/generate.py](data_generator/generate.py) produces synthetic
sales orders — regions, product catalogue, channel mix, weekend demand lift —
and uploads a CSV straight to `s3://raw-data/sales/`.

About 4% of rows are **deliberately defective** (blank regions, negative
quantities, unparseable prices, out-of-range discounts, stray whitespace), so
the cleaning stage has something real to do and the tests can assert on it.

```bash
docker compose run --rm data-generator --rows 5000        # one batch
docker compose --profile generator up -d data-generator   # a batch every 5 min
python -m data_generator.generate --rows 20 --out - --seed 1   # preview locally
```

### Orchestration

[dags/sales_ingestion_dag.py](dags/sales_ingestion_dag.py) — DAG
`sales_ingestion`, scheduled every 10 minutes:

1. **`wait_for_new_files`** — an `S3KeySensor` in `reschedule` mode watches
   `sales/*.csv` in MinIO, freeing its worker slot between pokes.
2. **`list_new_files`** — lists the bucket and subtracts everything already
   recorded in `ops.ingested_files`. Returns an empty list on a quiet cycle
   rather than skipping, which keeps the mapped task's XCom resolvable so the
   summary still runs.
3. **`process_file`** — dynamically mapped over each new object (4 at a time):
   download → clean → load → archive.
4. **`summarise`** — totals the run, runs `ANALYZE` so Metabase queries hit
   fresh statistics, and logs a warehouse snapshot.

The cleaning rules live in
[src/pipeline/transform.py](src/pipeline/transform.py), deliberately free of
Airflow imports so they unit test in milliseconds:

| Rule | Behaviour |
| :-- | :-- |
| Whitespace / casing | Trimmed; regions and categories title-cased, channels mapped to a canonical set (`website`, `online` → `web`) |
| Required fields | Rows missing `order_id`, `customer_id`, `region`, `category`, `product` or `channel` are rejected |
| Timestamps | Parsed to UTC; unparseable values rejected |
| Numerics | `quantity > 0`, `unit_price >= 0`, `0 <= discount_pct < 1` |
| Duplicates | First occurrence of an `order_id` wins within a batch |
| Derived columns | `order_date`, `gross_amount = quantity × unit_price`, `net_amount = gross × (1 − discount)` |

Rejected rows are **not dropped** — they go to `analytics.sales_rejects` with a
reason and the original record as JSON, so data quality is itself reportable.

**Idempotency** is enforced at three levels: `ops.ingested_files` stops a file
being picked up twice, `ON CONFLICT (order_id) DO NOTHING` stops duplicate
orders, and processed objects are moved to the `raw-data-archive` bucket. Re-run
the DAG as often as you like — row counts do not move. Test 10 in the
integration suite asserts exactly this.

### Storage

[config/postgres/init/02-analytics-schema.sql](config/postgres/init/02-analytics-schema.sql)
defines the warehouse:

- `analytics.sales` — the fact table, keyed on `order_id`, with check
  constraints mirroring the transform rules and indexes on date, region,
  category and source file.
- `analytics.sales_rejects` — quarantined rows with their rejection reason.
- `ops.ingested_files` — pipeline bookkeeping: which object, how many rows in,
  loaded, rejected, and under which DAG run.

---

## Part 3 — Visualization

```bash
docker compose --profile setup run --rm metabase-init
```

[scripts/setup_metabase.py](scripts/setup_metabase.py) creates the admin account
and registers the `analytics` database over the Metabase API — idempotent, so
re-running it just re-syncs the schema.

Six views are ready to chart at http://localhost:3000:

| View | Suggested visualization |
| :-- | :-- |
| `v_kpi_summary` | Number cards — revenue, orders, customers, average order value |
| `v_daily_sales` | Line chart of revenue over time |
| `v_sales_by_region` | Bar or map chart with revenue share |
| `v_top_products` | Row chart, top 10 by revenue |
| `v_channel_performance` | Stacked area chart by channel |
| `v_pipeline_health` | Table — files ingested and their accept rate |

To build the dashboard: **+ New → Question → Mini Data Platform → analytics →**
pick a view, then **Save → Add to a dashboard**. Number cards from
`v_kpi_summary` across the top, `v_daily_sales` as a full-width trend beneath,
then region and product breakdowns side by side.

---

## CI/CD

[.github/workflows/main.yml](.github/workflows/main.yml) runs five jobs:

| Job | What it does |
| :-- | :-- |
| **lint** | Ruff check + format, hadolint on both Dockerfiles, `docker compose config` validation, SQL presence check |
| **unit-tests** | Transform rules and DAG structure — no Docker needed |
| **build-images** | Builds the Airflow and data-generator images in a matrix, with GHA layer caching, and pushes to GHCR on `main` |
| **data-flow-validation** | Stands the real stack up and asserts data moves `MinIO → Airflow → PostgreSQL → Metabase` |
| **deploy-test** | On `main`, deploys the validated images to the test environment over SSH and smoke-checks both health endpoints |

### Data flow validation

[tests/integration/test_data_flow.py](tests/integration/test_data_flow.py) walks
the full path against a live stack:

1. Upload a seeded 750-row batch to MinIO and confirm the object exists.
2. Trigger `sales_ingestion` over the Airflow REST API and wait for success
   (reporting which tasks failed if it does not). Airflow 3 drops Basic auth,
   so the suite exchanges the admin credentials for a JWT at `/auth/token`
   and calls `/api/v2` with a bearer token.
3. Assert `analytics.sales` holds **exactly** the number of rows the transform
   predicted for that file — computed independently, not read back from the DB.
4. Assert `ops.ingested_files` recorded the run and that
   `loaded + rejected == raw`.
5. Assert defective rows landed in `sales_rejects` rather than vanishing.
6. Assert `gross_amount` and `net_amount` arithmetic holds across the table.
7. Assert all six KPI views return rows.
8. Assert the processed object was archived out of the landing zone.
9. Assert the Metabase API is healthy and running on its Postgres app database.
10. Re-run the DAG and assert row counts are unchanged (idempotency).

### Deployment configuration

CD is inert until you set these in the repository:

| Kind | Name | Example |
| :-- | :-- | :-- |
| Variable | `DEPLOY_HOST` | `test.example.com` |
| Variable | `DEPLOY_USER` | `deploy` |
| Variable | `DEPLOY_PATH` | `/opt/mini-data-platform` |
| Variable | `TEST_ENV_URL` | `http://test.example.com` |
| Secret | `DEPLOY_SSH_KEY` | private key for `DEPLOY_USER` |

Without them the job still runs, reports the published image tags, and tells you
what to configure — so a fork never fails CI on missing infrastructure.

---

## Running tests locally

```bash
pip install -r tests/requirements.txt

pytest tests/unit -v          # fast, no Docker
pytest tests/integration -v   # needs `docker compose up -d` first
make smoke                    # up + provision + seed + validate, in one go
```

---

## Repository structure

```text
├── dags/                      # Airflow DAG definitions
│   └── sales_ingestion_dag.py
├── data_generator/            # Synthetic sales data generator
├── src/pipeline/              # Transform + warehouse logic (Airflow-free, testable)
├── config/postgres/init/      # Database and warehouse schema bootstrap
├── docker/                    # Dockerfiles for the custom images
├── scripts/setup_metabase.py  # Metabase provisioning over the API
├── tests/unit/                # Transform rules and DAG integrity
├── tests/integration/         # End-to-end data flow validation
├── .github/workflows/main.yml # CI/CD pipeline
├── docker-compose.yml         # Platform orchestration
├── Makefile                   # Shortcuts for every common task
└── .env.example               # Configuration template
```

---

## Troubleshooting

**Airflow logs are unwritable on Linux** — set `AIRFLOW_UID=$(id -u)` in `.env`
and restart. (Not needed on Docker Desktop for Windows or macOS.)

**The DAG run is skipped** — that is the designed behaviour when nothing new is
in MinIO. Run `make seed` first.

**Metabase takes a while on first boot** — it migrates its application database;
the health check allows 90 seconds of start-up before it begins counting
failures. `docker compose logs -f metabase` shows progress.

**Port already in use** — override `POSTGRES_PORT`, `AIRFLOW_PORT`,
`METABASE_PORT`, `MINIO_API_PORT` or `MINIO_CONSOLE_PORT` in `.env`, then point
the integration tests at the same ports:

```bash
AIRFLOW_URL=http://localhost:18080 METABASE_URL=http://localhost:13000 \
MINIO_ENDPOINT=http://localhost:19000 POSTGRES_PORT=55432 pytest tests/integration -v
```

**MinIO images come from `quay.io`, not Docker Hub** — that is deliberate.
Docker Desktop's default image-access policy blocks the `minio/*` Docker Hub
repositories; `quay.io/minio/*` is MinIO's own registry and pulls without
authentication.

**Start completely fresh** — `make clean` removes every volume, including the
warehouse and Metabase's dashboards.
