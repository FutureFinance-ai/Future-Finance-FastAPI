from __future__ import annotations

import os
from typing import Optional, List


def getenv_str(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    return v if v is not None else default


def getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def getenv_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def getenv_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "on"}


def getenv_csv(name: str, default: Optional[str] = None) -> List[str]:
    v = os.getenv(name)
    raw = v if v is not None else (default or "")
    return [item.strip() for item in raw.split(",") if item.strip()]


ARTIFACTS_BASE_DIR = getenv_str("FF_ARTIFACTS_DIR", "/tmp/futurefinance_artifacts")

OCR_ENABLED = getenv_bool("FF_OCR_ENABLED", True)
OCR_MAX_PAGES = getenv_int("FF_OCR_MAX_PAGES", 5)
OCR_DPI = getenv_int("FF_OCR_DPI", 300)

MAX_PAGES = getenv_int("FF_MAX_PAGES", 200)
MAX_CHARS_PER_PAGE = getenv_int("FF_MAX_CHARS_PER_PAGE", 20000)

# Parallelism
PDF_MAX_WORKERS = getenv_int("FF_PDF_MAX_WORKERS", __import__("os").cpu_count() or 4)
USE_FITZ_FASTPATH = getenv_bool("FF_USE_FITZ_FASTPATH", False)

# PII masking controls
PII_MASK_ACCOUNT_ONLY = getenv_bool("FF_PII_MASK_ACCOUNT_ONLY", True)
PII_MASK_AMOUNTS = getenv_bool("FF_PII_MASK_AMOUNTS", False)

# Debug overlays
DEBUG_OVERLAY_ENABLED = getenv_bool("FF_DEBUG_OVERLAY_ENABLED", False)
DEBUG_OVERLAY_MAX_PAGES = getenv_int("FF_DEBUG_OVERLAY_MAX_PAGES", 5)

# OCR throttling and tuning
OCR_MAX_CONCURRENT = getenv_int("FF_OCR_MAX_CONCURRENT", 2)
TESS_LANGS = getenv_str("FF_TESS_LANGS", "eng")
TESS_OEM = getenv_int("FF_TESS_OEM", 1)
TESS_PSM = getenv_int("FF_TESS_PSM", 6)

# Artifact storage controls
ARTIFACTS_GZIP = getenv_bool("FF_ARTIFACTS_GZIP", False)
ARTIFACTS_INCLUDE_TEXTS = getenv_bool("FF_ARTIFACTS_INCLUDE_TEXTS", False)
ARTIFACTS_INCLUDE_WORDS = getenv_bool("FF_ARTIFACTS_INCLUDE_WORDS", False)

# Early stop controls
EARLY_STOP_MIN_ROWS = getenv_int("FF_EARLY_STOP_MIN_ROWS", 0)

# Metrics alerts
METRICS_ALERT_TOKENS_DOMINATE = getenv_bool("FF_METRICS_ALERT_TOKENS_DOMINATE", True)
METRICS_TOKENS_DOMINATE_RATIO = getenv_str("FF_METRICS_TOKENS_DOMINATE_RATIO", "0.6")


# --- Runtime / API settings ---
ENVIRONMENT = getenv_str("FF_ENV", "development")
IS_DEV = getenv_bool("FF_DEV", ENVIRONMENT != "production")

# CORS settings (primarily for development; configurable via env)
DEFAULT_DEV_CORS = (
    "http://localhost:3000,"
    "http://127.0.0.1:3000,"
    "http://localhost:5173,"
    "http://127.0.0.1:5173"
)
CORS_ALLOW_ORIGINS = getenv_csv("FF_CORS_ORIGINS", DEFAULT_DEV_CORS)
CORS_ALLOW_CREDENTIALS = getenv_bool("FF_CORS_ALLOW_CREDENTIALS", True)

