from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env"
STATE_DOCUMENT_ID = "vvv_uploader"
KEEP_VALUE = object()
