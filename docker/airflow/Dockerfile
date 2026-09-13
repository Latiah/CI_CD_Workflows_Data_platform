# syntax=docker/dockerfile:1
# Airflow image for the mini data platform. The scheduler, api-server,
# dag-processor and triggerer all run this exact image, so every component
# shares one dependency set and the pinned Airflow version is the only source
# of truth for it.
FROM apache/airflow:3.3.1-python3.12 AS runtime

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installed as the airflow user: pip must not write into root-owned site-packages.
USER airflow

COPY docker/airflow/requirements.txt /tmp/requirements.txt

# The constraints file is the official one for this exact Airflow/Python pair,
# so providers resolve to versions known to work with 3.3.1 rather than newest.
RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
        -r /tmp/requirements.txt

# DAGs and shared pipeline code are bind-mounted in compose, but baking them in
# keeps the image self-contained for CI and for deployments without volumes.
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root src/ /opt/airflow/src/

ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/src

# Test stage: tests/unit/test_dag_integrity.py skips itself with
# pytest.importorskip("airflow") whenever it runs on a plain CI runner, since
# Airflow is never installed there - so that check silently never executes.
# This stage runs it against the same Airflow install the scheduler and
# api-server actually use, so it becomes a real check instead of a no-op.
FROM runtime AS test

COPY --chown=airflow:root tests/unit/test_dag_integrity.py /opt/airflow/tests/unit/test_dag_integrity.py
RUN pip install --no-cache-dir pytest==8.3.4
