# Mini Data Platform (Dockerized)

[![ci](https://github.com/Latiah/CI_CD_Workflows_Data_platform/actions/workflows/main.yml/badge.svg)](https://github.com/Latiah/CI_CD_Workflows_Data_platform/actions/workflows/main.yml)

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
cp .env.example .env         # every value has a working default
./scripts/gen_secrets.sh     # replace the public placeholder secrets

make up                      # build and start, waiting until all services are healthy
make metabase                # create the admin user and connect the warehouse
make seed                    # drop a batch of synthetic sales into MinIO
```

Without `make`:

```bash
docker compose up -d --build --wait --wait-timeout 600   postgres minio airflow-apiserver airflow-scheduler airflow-dag-processor metabase
python -m scripts.provision_metabase
docker compose run --rm data-generator --rows 2000
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
runs and, under LocalExecutor, runs it.

`airflow-triggerer` is defined but not started by default. Nothing here defers,
and an idle Airflow component costs memory the rest of the stack needs. Add a
deferrable operator and you need it:
`docker compose --profile deferrable up -d airflow-triggerer`.

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

1. **`list_new_files`** — lists `sales/*.csv` in MinIO and subtracts everything
   already recorded in `ops.ingested_files`. Returns an empty list on a quiet
   cycle rather than skipping, which keeps the mapped task's XCom resolvable so
   the summary still runs.
2. **`process_file`** — dynamically mapped over each new object (4 at a time):
   download → clean → load → archive.
3. **`summarise`** — totals the run, runs `ANALYZE` so Metabase queries hit
   fresh statistics, and logs a warehouse snapshot.

An `S3KeySensor` sat in front of this and was removed. It cannot tell a new
object from one already ingested, and with `soft_fail=True` a real failure —
wrong endpoint, bad credentials — is recorded as a *skip*, which cascades
downstream and leaves the DAG run green having loaded nothing. Listing the
bucket answers the same question and fails loudly when MinIO is unreachable.

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
make metabase          # or: python -m scripts.provision_metabase
```

[scripts/provision_metabase.py](scripts/provision_metabase.py) creates the admin account
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

[.github/workflows/main.yml](.github/workflows/main.yml) runs six jobs in two
tiers, then deploys.

**Fast tier** — no running services, so it fails in under two minutes:

| Job | What it does |
| :-- | :-- |
| **lint** | `make lint` (ruff check + format, hadolint on both Dockerfiles) and `make validate` (compose config, SQL bootstrap) |
| **unit** | `make test` — transform rules, no Docker needed |
| **dags** | Builds the image's `test` stage and parses the DAG bag inside it |

**Integration tier** — the real stack, end to end:

| Job | What it does |
| :-- | :-- |
| **integration** | `compose up --wait` on the six core services, provisions Metabase, then `make e2e` asserts data moves `MinIO → Airflow → PostgreSQL → Metabase` |

**Deployment** — only on a push to `main`:

| Job | What it does |
| :-- | :-- |
| **publish** | Builds the `runtime` stage and pushes it to GHCR tagged `sha-<commit>`, with GHA layer caching |
| **deploy-test** | Pulls that exact tag, deploys it with `--no-build`, and re-runs the integration suite against it |

Three details worth knowing:

**CI runs the same `make` targets you do.** Every job invokes `make <target>
PY="python"` rather than raw commands, so a green `make lint test` locally means
the pipeline ran identical commands. There is no second copy of the build recipe
to drift out of sync.

**`--wait` replaces a polling loop.** `compose up --wait --wait-timeout 600`
blocks until every named service reports healthy and fails fast if one never
gets there, instead of letting tests queue against a dead scheduler.

**The deploy tests the artifact, not the source.** `publish` tags by commit sha,
never `latest`, and `deploy-test` runs `--no-build` against that pulled image —
so what gets smoke-tested is byte-for-byte what would ship. A mutable tag is how
a pipeline goes green while the environment quietly runs older code.

The `dags` job exists because
[tests/unit/test_dag_integrity.py](tests/unit/test_dag_integrity.py) calls
`pytest.importorskip("airflow")`: on a bare runner it would always skip, so it
runs inside the image the scheduler actually uses and becomes a real check.

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

Publishing uses the built-in `GITHUB_TOKEN`, so nothing needs configuring for
`publish` to work. `deploy-test` needs one thing:

- A **`test` environment** under Settings → Environments. It can be empty; the
  job references `environment: name: test` and fails immediately if it does not
  exist. Adding required reviewers there turns the deploy into a gated release.

Images land at `ghcr.io/<owner>/<repo>/airflow:sha-<commit>`. Make the package
public, or grant the repository read access, if anything outside Actions needs
to pull it.

### When a run fails

The `integration` job writes a summary table of service states to the run page
and uploads a `compose-logs-<run_id>` artifact containing:

- `compose.log` — every service's output
- `ps.txt` — container states, including `oom=true/false` per container, since a
  container killed for memory leaves nothing in its own log
- `tasks.log` — the Airflow task logs from disk, which is where a task traceback
  actually lands

The suite also pulls a failed task's log into the pytest failure message itself,
so the common case needs no artifact download at all.

---

## Running tests locally

```bash
pip install -r requirements-dev.txt

make test     # unit tests - fast, no Docker
make lint     # ruff + hadolint (containerised, same as CI)
make validate # compose file and SQL bootstrap
make e2e      # end-to-end, needs `make up` first
make smoke    # up + provision + seed + validate, in one go
```

CI runs these exact targets with `PY="python"`, so a green `make lint test`
locally means the same commands ran in the pipeline. Pass `PY=` to pin an
interpreter: `make test PY=python3.12`.

---

## Repository structure

```text
├── dags/                        # Airflow DAG definitions
│   └── sales_ingestion_dag.py
├── data_generator/              # Synthetic sales data generator
├── src/pipeline/                # Transform + warehouse logic (Airflow-free, testable)
├── config/postgres/init/        # Database and warehouse schema bootstrap
├── docker/
│   ├── airflow/Dockerfile       # runtime + test stages, both used by CI
│   └── data_generator/Dockerfile
├── scripts/
│   ├── provision_metabase.py    # `python -m scripts.provision_metabase`
│   └── gen_secrets.sh           # Replaces the placeholder secrets in .env
├── tests/unit/                  # Transform rules and DAG integrity
├── tests/integration/           # End-to-end data flow validation
├── .github/workflows/main.yml   # CI/CD pipeline
├── docker-compose.yml           # Platform orchestration
├── Makefile                     # The entry point CI and humans share
├── requirements-dev.txt         # One toolchain for local and CI
└── .env.example                 # Configuration template
```

