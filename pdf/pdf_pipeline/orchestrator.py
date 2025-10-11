from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import os

import pdfplumber
from pypdf import PdfReader, PdfWriter

from services import config
from services.json_logger import get_json_logger
from services.pdf_statement_processor import PdfStatementProcessor


logger = get_json_logger("pdf_pipeline")


@dataclass
class LayoutPageMeta:
    width: Optional[float]
    height: Optional[float]
    rotation: int
    images_count: int
    words_count: int


def _is_pdf_path(path: str) -> bool:
    return path.lower().endswith(".pdf")


def _read_file_bytes(file_path: str) -> bytes:
    with open(file_path, "rb") as f:
        return f.read()


def analyze_pdf_structure(pdf_bytes: bytes) -> Tuple[bool, Dict[str, object]]:
    """
    Inspect the PDF for a text layer and collect light layout metadata.
    Returns (is_text_searchable, layout_metadata_dict).
    """
    pages_meta: List[Dict[str, object]] = []
    is_text_searchable = False

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(
                keep_blank_chars=True,
                use_text_flow=True,
                x_tolerance=2,
                y_tolerance=2,
            ) or []
            text = page.extract_text() or ""
            has_images = bool(getattr(page, "images", []))
            bbox = getattr(page, "bbox", None)
            rotation = getattr(page, "rotation", 0)

            is_text_searchable = is_text_searchable or (len(words) > 0 or len(text.strip()) > 0)

            pages_meta.append({
                "width": (bbox[2] - bbox[0]) if bbox else None,
                "height": (bbox[3] - bbox[1]) if bbox else None,
                "rotation": rotation,
                "images_count": len(getattr(page, "images", [])),
                "words_count": len(words),
            })

        layout_metadata: Dict[str, object] = {
            "pages_count": len(pdf.pages),
            "pages": pages_meta,
        }

    return is_text_searchable, layout_metadata


def _decrypt_if_needed(pdf_bytes: bytes, password: Optional[str]) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Detect encrypted PDFs and decrypt when a password is provided.
    Returns (maybe_decrypted_bytes, error_code_if_any).
    """
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
    except Exception as exc:
        return None, "ERROR_PDF_LOAD_FAILED"

    if not getattr(reader, "is_encrypted", False):
        return pdf_bytes, None

    if not password:
        return None, "ERROR_ENCRYPTED"

    try:
        decrypt_result = reader.decrypt(password)
        # pypdf returns 0 on failure
        if decrypt_result == 0:
            return None, "ERROR_PASSWORD_INCORRECT"
    except Exception:
        return None, "ERROR_PASSWORD_INCORRECT"

    try:
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        out = BytesIO()
        writer.write(out)
        return out.getvalue(), None
    except Exception:
        return None, "ERROR_DECRYPTION_FAILED"


def process_pdf_file(
    file_path: str,
    account_id: Optional[str] = None,
    password: Optional[str] = None,
    drop_duplicates: bool = False,
) -> Dict[str, object]:
    """
    Phase 1 (ingestion + structure analysis) + delegate to PdfStatementProcessor.
    Keeps behavior additive and backward compatible.
    """
    max_mb = config.getenv_int("FF_MAX_PDF_SIZE_MB", 20)
    max_bytes = max_mb * 1024 * 1024

    if not _is_pdf_path(file_path):
        return {"status": "ERROR_INVALID_TYPE", "message": "Only PDF files are supported"}

    try:
        size = os.path.getsize(file_path)
    except Exception:
        size = None
    if isinstance(size, int) and size > max_bytes:
        return {"status": "ERROR_FILE_TOO_LARGE", "max_mb": max_mb, "size_bytes": size}

    # Read bytes
    content = _read_file_bytes(file_path)

    # Attempt decryption if needed
    decrypted_bytes, decrypt_error = _decrypt_if_needed(content, password)
    if decrypt_error is not None:
        return {"status": decrypt_error}
    content = decrypted_bytes if decrypted_bytes is not None else content

    # Fast structure analysis (OCR decision remains inside PdfStatementProcessor as well)
    try:
        is_text_searchable, layout_metadata = analyze_pdf_structure(content)
    except Exception as exc:
        logger.warning("layout_inspect_failed", extra={"extra": {"error": str(exc)}})
        is_text_searchable, layout_metadata = (False, {})

    # Delegate full pipeline to existing processor (which handles OCR fallback and storage)
    # If text is searchable, OCR can be skipped for performance; otherwise allow fallback OCR
    processor = PdfStatementProcessor(ocr_enabled=not is_text_searchable)
    result = processor.process_pdf(
        content,
        filename=os.path.basename(file_path),
        account_id=account_id,
        opening_balance=None,
        closing_balance=None,
        drop_duplicates=drop_duplicates,
    )

    # Enrich response with phase-1 metadata for traceability
    result.setdefault("analysis", {})
    result["analysis"].update({
        "is_text_searchable": is_text_searchable,
        "layout": layout_metadata,
        "ocr_planned": not is_text_searchable,
    })

    # Metrics alerts: warn if token-dominant pages exceed ratio
    try:
        metrics = result.get("metrics") or {}
        rows_from_tables = int(metrics.get("rows_from_tables", 0))
        rows_from_tokens = int(metrics.get("rows_from_tokens", 0))
        total_rows = rows_from_tables + rows_from_tokens + int(metrics.get("rows_from_text", 0))
        if total_rows > 0 and getattr(config, "METRICS_ALERT_TOKENS_DOMINATE", True):
            ratio = rows_from_tokens / float(total_rows)
            threshold = float(getattr(config, "METRICS_TOKENS_DOMINATE_RATIO", 0.6))
            if ratio >= threshold:
                logger.warning("tokens_dominate_rows", extra={"extra": {"ratio": ratio, "threshold": threshold, "total_rows": total_rows}})
    except Exception:
        pass

    return result


def process_many(files: List[str], account_id: Optional[str] = None, password: Optional[str] = None, drop_duplicates: bool = False) -> List[Dict[str, object]]:
    """
    Simple multi-file job queue runner with backpressure via PDF_MAX_WORKERS.
    Returns list of results in input order.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results: List[Optional[Dict[str, object]]] = [None] * len(files)
    max_workers = max(1, int(getattr(config, "PDF_MAX_WORKERS", 4)))

    def _run(idx: int, path: str) -> Tuple[int, Dict[str, object]]:
        try:
            r = process_pdf_file(path, account_id=account_id, password=password, drop_duplicates=drop_duplicates)
            return idx, r
        except Exception as exc:
            return idx, {"status": "ERROR", "message": str(exc), "file": path}

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_run, i, p) for i, p in enumerate(files)]
        for fut in as_completed(futs):
            idx, r = fut.result()
            results[idx] = r

    return [r for r in results if r is not None]


