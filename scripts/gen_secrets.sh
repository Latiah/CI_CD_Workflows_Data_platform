#!/usr/bin/env bash
# Replace the placeholder secrets in .env with freshly generated ones.
#
# .env.example ships working defaults so the stack starts with no setup, but
# those values are public. This regenerates them, and is safe to re-run: each
# key is replaced in place rather than appended twice.
#
#   ./scripts/gen_secrets.sh [env-file]

set -euo pipefail

ENV_FILE="${1:-.env}"

if [ ! -f "$ENV_FILE" ]; then
    echo "No $ENV_FILE — copy .env.example first: cp .env.example $ENV_FILE" >&2
    exit 1
fi

PY="${PY:-}"
if [ -z "$PY" ]; then
    if command -v python >/dev/null 2>&1; then PY=python; else PY=python3; fi
fi

# A Fernet key is 32 random bytes, url-safe base64 encoded. Generated with the
# standard library so this does not depend on `cryptography` being installed.
fernet_key() {
    "$PY" -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
}

token() {
    "$PY" -c "import secrets; print(secrets.token_urlsafe(48))"
}

# Rewrites KEY=... in place, or appends it when absent. Uses awk rather than
# `sed -i`, whose in-place flag is not portable between GNU and BSD.
set_key() {
    local key="$1" value="$2" tmp
    tmp="$(mktemp)"
    if grep -q "^${key}=" "$ENV_FILE"; then
        awk -v k="$key" -v v="$value" \
            'index($0, k "=") == 1 { print k "=" v; next } { print }' \
            "$ENV_FILE" >"$tmp"
        mv "$tmp" "$ENV_FILE"
    else
        rm -f "$tmp"
        printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
    fi
    echo "  regenerated ${key}"
}

echo "Generating secrets in ${ENV_FILE}:"
set_key AIRFLOW_FERNET_KEY "$(fernet_key)"
set_key AIRFLOW_SECRET_KEY "$(token)"
set_key AIRFLOW_JWT_SECRET "$(token)"
echo "Done. These are local to ${ENV_FILE}, which is gitignored."
