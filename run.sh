#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
else
    echo "Python is required but was not found in PATH." >&2
    exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
    "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if ! python - <<'PY'
import importlib.util
import sys

required_modules = [
    "telegram",
    "pymongo",
    "httpx",
    "imageio_ffmpeg",
]
missing = [name for name in required_modules if importlib.util.find_spec(name) is None]
sys.exit(0 if not missing else 1)
PY
then
    python -m pip install --upgrade pip
    python -m pip install -r "$ROOT_DIR/requirements.txt"
fi

exec python "$ROOT_DIR/__main__.py"
