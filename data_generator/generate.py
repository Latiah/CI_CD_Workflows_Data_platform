"""Synthetic sales data generator.

Writes a CSV batch either to disk/stdout or straight into the MinIO landing
bucket, which is what sets the Airflow pipeline in motion.

    python -m data_generator.generate --rows 2000                # one batch -> MinIO
    python -m data_generator.generate --rows 50 --out sample.csv # one batch -> file
    python -m data_generator.generate --interval 300             # keep producing
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

LOG = logging.getLogger("data_generator")

REGIONS = {
    "North America": ["United States", "Canada", "Mexico"],
    "Europe": ["Germany", "France", "United Kingdom", "Spain"],
    "Africa": ["Ghana", "Kenya", "Nigeria", "Rwanda", "South Africa"],
    "Asia Pacific": ["India", "Japan", "Singapore", "Australia"],
    "South America": ["Brazil", "Argentina", "Chile"],
}

CATALOG = {
    "Electronics": [
        ("Wireless Headphones", 89.99),
        ("4K Monitor", 329.00),
        ("Mechanical Keyboard", 119.50),
        ("USB-C Hub", 45.00),
    ],
    "Home": [
        ("Espresso Machine", 249.00),
        ("Air Purifier", 179.99),
        ("Desk Lamp", 39.90),
        ("Cookware Set", 149.00),
    ],
    "Apparel": [
        ("Running Shoes", 110.00),
        ("Rain Jacket", 95.00),
        ("Merino Socks", 18.50),
        ("Backpack", 72.00),
    ],
    "Groceries": [
        ("Coffee Beans 1kg", 24.00),
        ("Olive Oil 750ml", 16.75),
        ("Dark Chocolate", 4.20),
        ("Green Tea 100ct", 12.00),
    ],
    "Office": [
        ("Standing Desk", 420.00),
        ("Ergonomic Chair", 385.00),
        ("Notebook 5-pack", 14.99),
        ("Monitor Arm", 88.00),
    ],
}

CHANNELS = ["web", "mobile_app", "retail_store", "partner", "phone"]
CHANNEL_WEIGHTS = [0.42, 0.28, 0.16, 0.09, 0.05]

FIELDNAMES = [
    "order_id",
    "order_ts",
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
]

# Deliberate defect rate: the pipeline's cleaning step has to earn its keep.
DIRTY_ROW_RATE = 0.04


def _customer_name(faker) -> str:
    return faker.name() if faker else f"Customer {random.randint(1000, 9999)}"


def _make_row(faker, now: datetime, days_back: int) -> dict:
    region = random.choice(list(REGIONS))
    category = random.choice(list(CATALOG))
    product, base_price = random.choice(CATALOG[category])

    offset = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    order_ts = now - offset

    # Weekend lift so the daily-trend chart has some real shape to it.
    weekend_lift = 1.25 if order_ts.weekday() >= 5 else 1.0
    quantity = min(max(1, int(random.paretovariate(2.2) * weekend_lift)), 12)

    unit_price = round(base_price * random.uniform(0.92, 1.08), 2)
    discount = random.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20])

    return {
        "order_id": str(uuid.uuid4()),
        "order_ts": order_ts.isoformat(),
        "customer_id": f"CUST-{random.randint(1, 800):05d}",
        "customer_name": _customer_name(faker),
        "region": region,
        "country": random.choice(REGIONS[region]),
        "channel": random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        "category": category,
        "product": product,
        "quantity": quantity,
        "unit_price": f"{unit_price:.2f}",
        "discount_pct": f"{discount:.2f}",
    }


def _corrupt(row: dict) -> dict:
    """Introduce one realistic defect so downstream validation is exercised."""
    defect = random.choice(
        ["blank_region", "negative_qty", "bad_price", "missing_ts", "whitespace", "bad_discount"]
    )
    row = dict(row)
    if defect == "blank_region":
        row["region"] = ""
    elif defect == "negative_qty":
        row["quantity"] = -abs(int(row["quantity"]))
    elif defect == "bad_price":
        row["unit_price"] = "n/a"
    elif defect == "missing_ts":
        row["order_ts"] = ""
    elif defect == "whitespace":
        row["region"] = "  " + row["region"] + "  "
        row["channel"] = row["channel"].upper()
    elif defect == "bad_discount":
        row["discount_pct"] = "1.50"
    return row


def generate_rows(count: int, days_back: int = 45, seed: int | None = None) -> list[dict]:
    if seed is not None:
        random.seed(seed)
    try:
        from faker import Faker

        faker = Faker()
        if seed is not None:
            Faker.seed(seed)
    except ImportError:  # Faker is optional; names degrade gracefully.
        faker = None

    now = datetime.now(UTC)
    rows = []
    for _ in range(count):
        row = _make_row(faker, now, days_back)
        if random.random() < DIRTY_ROW_RATE:
            row = _corrupt(row)
        rows.append(row)
    return rows


def to_csv(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=FIELDNAMES, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def object_key(prefix: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    prefix = prefix.strip("/")
    name = f"sales_{stamp}_{uuid.uuid4().hex[:8]}.csv"
    return f"{prefix}/{name}" if prefix else name


def upload_to_minio(payload: str, key: str) -> str:
    import boto3
    from botocore.client import Config

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    bucket = os.environ.get("MINIO_BUCKET", "raw-data")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"), ContentType="text/csv")
    return f"s3://{bucket}/{key}"


def emit_batch(args) -> str:
    rows = generate_rows(args.rows, days_back=args.days_back, seed=args.seed)
    payload = to_csv(rows)

    if args.out == "-":
        sys.stdout.write(payload)
        return "stdout"
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="") as handle:
            handle.write(payload)
        LOG.info("Wrote %s rows to %s", len(rows), args.out)
        return args.out

    key = object_key(args.prefix)
    uri = upload_to_minio(payload, key)
    LOG.info("Uploaded %s rows to %s", len(rows), uri)
    return uri


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate synthetic sales data.")
    parser.add_argument("--rows", type=int, default=1000, help="rows per batch")
    parser.add_argument("--days-back", type=int, default=45, help="spread orders over N past days")
    parser.add_argument("--seed", type=int, default=None, help="deterministic output")
    parser.add_argument("--out", default=None, help="write to a file ('-' for stdout) instead of MinIO")
    parser.add_argument("--prefix", default=os.environ.get("MINIO_PREFIX", "sales/"), help="MinIO key prefix")
    parser.add_argument("--interval", type=int, default=0, help="seconds between batches (0 = one batch)")
    parser.add_argument("--batches", type=int, default=0, help="stop after N batches (0 = unlimited)")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)

    if args.interval <= 0:
        emit_batch(args)
        return 0

    produced = 0
    while args.batches == 0 or produced < args.batches:
        emit_batch(args)
        produced += 1
        if args.batches and produced >= args.batches:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
