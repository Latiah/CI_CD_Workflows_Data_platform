"""Provision Metabase: create the admin account and attach the analytics warehouse.

Idempotent — if Metabase is already set up, it logs in instead and only adds the
database connection when it is missing. Standard library only, so it runs in a
bare python image or on the host.

    python scripts/setup_metabase.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "Metabase123!")
DB_DISPLAY_NAME = os.environ.get("METABASE_DB_NAME", "Mini Data Platform")

PG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("ANALYTICS_DB", "analytics"),
    "user": os.environ.get("POSTGRES_USER", "platform"),
    "password": os.environ.get("POSTGRES_PASSWORD", "platform"),
}


def request(method: str, path: str, payload: dict | None = None, token: str | None = None) -> dict:
    url = f"{BASE_URL}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Metabase-Session", token)
    with urllib.request.urlopen(req, timeout=60) as response:
        body = response.read().decode()
        return json.loads(body) if body else {}


def wait_for_metabase(attempts: int = 60, delay: int = 5) -> None:
    for attempt in range(1, attempts + 1):
        try:
            health = request("GET", "/api/health")
            if health.get("status") == "ok":
                print(f"Metabase is healthy at {BASE_URL}")
                return
        except Exception as exc:  # noqa: BLE001 - any failure means "not ready yet"
            if attempt % 6 == 0:
                print(f"  waiting for Metabase ({attempt}/{attempts}): {exc}")
        time.sleep(delay)
    raise SystemExit(f"Metabase did not become healthy at {BASE_URL}")


def session_token() -> str:
    """Run first-time setup if needed, otherwise log in."""
    properties = request("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")

    if setup_token:
        print("Running Metabase first-time setup...")
        result = request(
            "POST",
            "/api/setup",
            {
                "token": setup_token,
                "user": {
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                    "first_name": "Platform",
                    "last_name": "Admin",
                    "site_name": "Mini Data Platform",
                },
                "prefs": {"site_name": "Mini Data Platform", "allow_tracking": False},
            },
        )
        token = result.get("id")
        if token:
            print(f"Admin account created: {ADMIN_EMAIL}")
            return token

    print("Metabase already initialised; signing in.")
    return request("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})["id"]


def ensure_database(token: str) -> int:
    existing = request("GET", "/api/database", token=token)
    databases = existing.get("data", existing) if isinstance(existing, dict) else existing
    for database in databases:
        if database.get("name") == DB_DISPLAY_NAME:
            print(f"Warehouse connection '{DB_DISPLAY_NAME}' already registered (id={database['id']}).")
            return database["id"]

    created = request(
        "POST",
        "/api/database",
        {
            "name": DB_DISPLAY_NAME,
            "engine": "postgres",
            "details": {
                "host": PG["host"],
                "port": PG["port"],
                "dbname": PG["dbname"],
                "user": PG["user"],
                "password": PG["password"],
                "ssl": False,
                "tunnel-enabled": False,
            },
            "is_full_sync": True,
        },
        token=token,
    )
    print(f"Registered warehouse connection '{DB_DISPLAY_NAME}' (id={created['id']}).")
    return created["id"]


def trigger_sync(token: str, database_id: int) -> None:
    for endpoint in ("sync_schema", "rescan_values"):
        try:
            request("POST", f"/api/database/{database_id}/{endpoint}", {}, token=token)
        except urllib.error.HTTPError as exc:
            print(f"  {endpoint} returned {exc.code} (non-fatal)")
    print("Schema sync requested; tables and KPI views will appear shortly.")


def main() -> int:
    wait_for_metabase()
    token = session_token()
    database_id = ensure_database(token)
    trigger_sync(token, database_id)
    print(
        "\nMetabase is ready.\n"
        f"  URL:      {BASE_URL}\n"
        f"  Login:    {ADMIN_EMAIL} / {ADMIN_PASSWORD}\n"
        "  Browse:   analytics schema -> v_kpi_summary, v_daily_sales, v_sales_by_region,\n"
        "            v_top_products, v_channel_performance, v_pipeline_health"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
