#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PY=${PYTHON_BIN:-/opt/homebrew/bin/python3.13}
if ! command -v "$PY" >/dev/null 2>&1; then
    PY=$(command -v python3.13 || command -v python3.12 || command -v python3.11 || command -v python3)
fi

if [ ! -d .venv ]; then
    echo "[run] creating venv with $PY"
    "$PY" -m venv .venv
fi

echo "[run] installing deps (quiet)"
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

mkdir -p data

URL="http://localhost:8787"
echo "[run] launching uvicorn on $URL"

( sleep 1.5; open "$URL" 2>/dev/null || true ) &

exec .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8787
