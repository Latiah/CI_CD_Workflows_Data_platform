"""Provision Metabase: create the admin account and attach the analytics warehouse.

Idempotent — if Metabase is already set up, it logs in instead and only adds the
database connection when it is missing. Standard library only, so it runs in a
bare python image or on the host.

    python -m scripts.provision_metabase
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

# These details are not used to connect from here — they are handed to Metabase,
# which resolves them from inside its own container. So the default is the
# compose service name, not localhost, whether this script runs on the host or
# in the network.
PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
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
    """Log in, falling back to first-time setup on a fresh instance.

    Login is tried first deliberately. Keying off the `setup-token` property
    instead looked cleaner but is unreliable: an already-provisioned Metabase
    can still report a token, and POSTing /api/setup then fails with a bare
    403 Forbidden. Whether we can sign in is the question that actually
    matters, so ask that one.
    """
    try:
        token = request("POST", "/api/session", {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})["id"]
        print(f"Signed in as {ADMIN_EMAIL} (already provisioned).")
        return token
    except urllib.error.HTTPError as exc:
        if exc.code not in (400, 401):
            raise
        print("Could not sign in; running first-time setup...")

    properties = request("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")
    if not setup_token:
        raise SystemExit(
            f"Cannot sign in as {ADMIN_EMAIL} and Metabase offers no setup token. "
            "It is already provisioned with different credentials - set "
            "METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD to match, or run "
            "`make clean` to start from an empty volume."
        )

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
    print(f"Admin account created: {ADMIN_EMAIL}")
    return result["id"]


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
