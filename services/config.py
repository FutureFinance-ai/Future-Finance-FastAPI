from __future__ import annotations

import os


def getenv_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None else default


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


ARTIFACTS_BASE_DIR = getenv_str("FF_ARTIFACTS_DIR", "/tmp/futurefinance_artifacts")

OCR_ENABLED = getenv_bool("FF_OCR_ENABLED", True)
OCR_MAX_PAGES = getenv_int("FF_OCR_MAX_PAGES", 5)
OCR_DPI = getenv_int("FF_OCR_DPI", 300)

MAX_PAGES = getenv_int("FF_MAX_PAGES", 200)
MAX_CHARS_PER_PAGE = getenv_int("FF_MAX_CHARS_PER_PAGE", 20000)


