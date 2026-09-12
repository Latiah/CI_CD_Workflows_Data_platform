# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY docker/data_generator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY data_generator/ /app/data_generator/

# Run as a non-root user; the generator only ever talks to MinIO over HTTP.
RUN useradd --create-home --uid 10001 generator \
    && chown -R generator:generator /app
USER generator

ENTRYPOINT ["python", "-m", "data_generator.generate"]
CMD ["--rows", "1000"]
