"""Writes the API's OpenAPI schema to ui/src/api/openapi.json, the source of the UI's TS types.

Run after changing any response model, then `npm run gen:api` in ui/. A unit test fails if the
committed snapshot is out of date.
"""

import json
import sys
from pathlib import Path

API_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(API_DIR))

from crypto_api.config import Settings  # noqa: E402
from crypto_api.main import create_app  # noqa: E402

TARGET = API_DIR.parent / "ui" / "src" / "api" / "openapi.json"

if __name__ == "__main__":
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(json.dumps(create_app(Settings()).openapi(), indent=2) + "\n")
    print(f"wrote {TARGET}")
