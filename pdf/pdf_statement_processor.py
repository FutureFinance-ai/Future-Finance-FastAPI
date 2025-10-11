from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import hashlib
import re
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from threading import Semaphore
from pdf.json_logger import get_json_logger
from pdf import config
from pdf.artifact_storage import ArtifactStorage

import pdfplumber
from pdf.statement_parsers import ParserRegistry, NgGenericParser, OpayParser
from pdf.pdf_pipeline.tokenization import (
    words_to_rows,
    infer_column_bands,
    assign_tokens_to_columns,
)
from pdf.pdf_pipeline.debug_overlay import render_page_overlay

# Precompiled regex patterns used across normalization/text parsing
AMOUNT_TOKEN_RE_STR = r"(?:\(?-?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{2})?\)?|\(?-?\d+(?:\.\d{2})?\)?)"
DATE_TOKEN_RE_STR = r"\b(?:\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4}|\d{4}[\-/]\d{1,2}[\-/]\d{1,2}|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{2,4})\b"
LINE_RE = re.compile(rf"(?P<date>{DATE_TOKEN_RE_STR}).{{1,80}}?(?P<amount>{AMOUNT_TOKEN_RE_STR})(?:\s+(?P<balance>{AMOUNT_TOKEN_RE_STR}))?", re.IGNORECASE)


def _ocr_png_to_text(png_bytes: bytes, lang: str, oem: int, psm: int) -> str:
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(png_bytes))
        cfg = f"--oem {int(oem)} --psm {int(psm)}"
        return pytesseract.image_to_string(img, lang=lang, config=cfg)
    except Exception:
        return ""

try:
    import pytesseract  # type: ignore
    _HAVE_TESSERACT = True
except Exception:
    _HAVE_TESSERACT = False

try:
    from babel.numbers import parse_decimal as babel_parse_decimal  # type: ignore
    _HAVE_BABEL = True
except Exception:
    _HAVE_BABEL = False


@dataclass
class ExtractionResult:
    """
    Result of low-level PDF extraction prior to normalization/parsing.

    - page_texts: raw text per page (single string per page)
    - page_tables: list per page of tables, each table is list of rows (list of cell strings)
    - image_based_pages: flags indicating pages likely requiring OCR
    - pages_count: number of pages
    - first_page_text: convenience copy for fingerprinting
    - document_id: deterministic hash of the original content (idempotency key)
    - metadata: lightweight metadata per page (width/height, rotation) and at doc level
    """

    document_id: str
    pages_count: int
    page_texts: List[str] = field(default_factory=list)
    page_tables: List[List[List[List[str]]]] = field(default_factory=list)
    page_words: List[List[Dict[str, object]]] = field(default_factory=list)
    image_based_pages: List[bool] = field(default_factory=list)
    first_page_text: str = ""
    metadata: Dict[str, object] = field(default_factory=dict)


class PdfStatementProcessor:
    """
    Performs staged PDF processing for bank statements.

    This class focuses on the extraction stage first. Subsequent stages
    (fingerprint, normalize_rows, parse_transactions, validate_statement, mask_pii)
    will be added incrementally.
    """

    def __init__(self, storage=None, ocr_enabled: bool = True, ocr_max_pages: int = 5, ocr_dpi: int = 300) -> None:
        self.storage = storage
        self.ocr_enabled = config.OCR_ENABLED if ocr_enabled is True else ocr_enabled
        self.ocr_max_pages = int(config.OCR_MAX_PAGES if ocr_max_pages == 5 else ocr_max_pages)
        self.ocr_dpi = int(config.OCR_DPI if ocr_dpi == 300 else ocr_dpi)
        # Minimal registry; extend with bank-specific parsers over time
        self.parser_registry = ParserRegistry(parsers=[OpayParser(), NgGenericParser()])
        self.logger = get_json_logger("pdf_processor")
        # Security limits
        self.max_pages = config.MAX_PAGES
        self.max_chars_per_page = config.MAX_CHARS_PER_PAGE
        # PII masking flags
        self.mask_account_like_numbers_only = getattr(config, "PII_MASK_ACCOUNT_ONLY", True)
        self.mask_amounts = getattr(config, "PII_MASK_AMOUNTS", False)
        # OCR throttling
        self._ocr_semaphore = Semaphore(max(1, int(getattr(config, "OCR_MAX_CONCURRENT", 2))))
        # OCR process pool (for CPU-heavy OCR invocations)
        self._ocr_pool: Optional[ProcessPoolExecutor] = None

    def extract(self, content: bytes, filename: Optional[str] = None) -> ExtractionResult:
        """
        Extract page texts and tables using pdfplumber. Does not attempt parsing.

        - Robust to pages with no extractable text (flags as image-based candidates)
        - Normalizes table cells to stripped strings
        - Computes a deterministic document_id from the raw bytes (idempotency)
        """

        document_id = hashlib.sha256(content).hexdigest()

        page_texts: List[str] = []
        page_tables: List[List[List[List[str]]]] = []
        page_words: List[List[Dict[str, object]]] = []
        image_based_pages: List[bool] = []
        page_meta: List[Dict[str, object]] = []
        ocr_pages: List[int] = []

        # Precompiled regex already at module scope

        def _has_rulings(page) -> bool:
            try:
                return bool(getattr(page, "lines", []) or getattr(page, "curves", []))
            except Exception:
                return False

        def process_single_page(idx: int) -> Tuple[int, Dict[str, object]]:
            with pdfplumber.open(BytesIO(content)) as pdf_local:
                page = pdf_local.pages[idx]
                text = page.extract_text() or ""
                words: List[Dict[str, object]] = []

                tables_norm: List[List[List[str]]] = []
                tables_strategy = "stream"
                tables_raw_lattice = []
                # Lattice only if rulings are present (cheap precheck)
                if _has_rulings(page):
                    try:
                        tables_raw_lattice = page.extract_tables(table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "intersection_tolerance": 3,
                            "snap_tolerance": 3,
                            "join_tolerance": 3,
                        }) or []
                    except Exception:
                        tables_raw_lattice = []
                tables_raw = tables_raw_lattice
                if not tables_raw:
                    try:
                        tables_raw = page.extract_tables() or []
                    except Exception:
                        tables_raw = []
                    tables_strategy = "stream" if tables_raw else "none"
                else:
                    tables_strategy = "lattice"
                for table in tables_raw:
                    norm_rows: List[List[str]] = []
                    for row in table:
                        norm_rows.append([str(cell if cell is not None else "").strip() for cell in row])
                    tables_norm.append(norm_rows)

                has_images = bool(getattr(page, "images", []))
                is_image_like = has_images and (len(text.strip()) == 0)
                # Gate token extraction: only compute when needed (no tables) or for debug overlay
                if not tables_norm or getattr(config, "DEBUG_OVERLAY_ENABLED", False):
                    try:
                        words = page.extract_words(
                            keep_blank_chars=True,
                            use_text_flow=True,
                            x_tolerance=2,
                            y_tolerance=2,
                        ) or []
                    except Exception:
                        words = []
                ocr_applied = False
                if (self.ocr_enabled and _HAVE_TESSERACT and len(ocr_pages) < self.ocr_max_pages and (is_image_like or len(text.strip()) == 0)):
                    # Throttle OCR
                    with self._ocr_semaphore:
                        try:
                            page_image = page.to_image(resolution=max(200, min(self.ocr_dpi, 300)))
                            pil_img = getattr(page_image, "image", None)
                            if pil_img is not None:
                                # Optional downscale for speed
                                if max(pil_img.size) > 1800:
                                    factor = 1800.0 / float(max(pil_img.size))
                                    new_size = (int(pil_img.size[0] * factor), int(pil_img.size[1] * factor))
                                    pil_img = pil_img.resize(new_size)
                                # Route OCR via ProcessPool for CPU isolation
                                if self._ocr_pool is None:
                                    self._ocr_pool = ProcessPoolExecutor(max_workers=max(1, int(getattr(config, "OCR_MAX_CONCURRENT", 2))))
                                import io
                                buf = io.BytesIO()
                                pil_img.save(buf, format="PNG")
                                fut = self._ocr_pool.submit(
                                    _ocr_png_to_text,
                                    buf.getvalue(),
                                    str(getattr(config, "TESS_LANGS", "eng")),
                                    int(getattr(config, "TESS_OEM", 1)),
                                    int(getattr(config, "TESS_PSM", 6)),
                                )
                                ocr_text = fut.result()
                                if ocr_text and len(ocr_text.strip()) > 0:
                                    text = ocr_text
                                    ocr_applied = True
                        except Exception:
                            pass
                if len(text) > self.max_chars_per_page:
                    text = text[: self.max_chars_per_page]
                bbox = getattr(page, "bbox", None)
                rotation = getattr(page, "rotation", 0)
                meta = {
                    "width": bbox[2] - bbox[0] if bbox else None,
                    "height": bbox[3] - bbox[1] if bbox else None,
                    "rotation": rotation,
                    "has_images": has_images,
                    "tables_found": len(tables_norm),
                    "tables_strategy": tables_strategy,
                    "words_found": len(words),
                    "chars_count": len(text),
                    "ocr_applied": ocr_applied,
                }
                return idx, {
                    "text": text,
                    "tables": tables_norm,
                    "words": words,
                    "is_image_like": is_image_like,
                    "meta": meta,
                    "ocr_applied": ocr_applied,
                }

        with pdfplumber.open(BytesIO(content)) as pdf:
            pages_count = len(pdf.pages)
            if pages_count > self.max_pages:
                self.logger.warning("pdf_pages_exceed_limit", extra={"extra": {"document_id": document_id, "pages": pages_count, "max_pages": self.max_pages}})
                indices = list(range(0, self.max_pages))
            else:
                indices = list(range(0, pages_count))

        # Parallel processing of pages
        max_workers = max(1, int(getattr(config, "PDF_MAX_WORKERS", 4)))
        results_map: Dict[int, Dict[str, object]] = {}
        if max_workers == 1 or len(indices) == 1:
            for i in indices:
                idx, data = process_single_page(i)
                results_map[idx] = data
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_single_page, i): i for i in indices}
                for fut in as_completed(futures):
                    idx, data = fut.result()
                    results_map[idx] = data

        for idx in sorted(results_map.keys()):
            data = results_map[idx]
            page_texts.append(data["text"])  # type: ignore[index]
            page_tables.append(data["tables"])  # type: ignore[index]
            page_words.append(data["words"])  # type: ignore[index]
            image_based_pages.append(data["is_image_like"])  # type: ignore[index]
            page_meta.append(data["meta"])  # type: ignore[index]
            if data.get("ocr_applied"):
                ocr_pages.append(idx)

        first_page_text = page_texts[0] if page_texts else ""

        metadata: Dict[str, object] = {
            "filename": filename,
            "pages": page_meta,
            "ocr": {
                "enabled": bool(self.ocr_enabled and _HAVE_TESSERACT),
                "applied_pages": ocr_pages,
                "engine": "pytesseract" if _HAVE_TESSERACT else None,
                "dpi": self.ocr_dpi,
                "max_pages": self.ocr_max_pages,
            },
        }

        result = ExtractionResult(
            document_id=document_id,
            pages_count=pages_count if 'pages_count' in locals() else 0,
            page_texts=page_texts,
            page_tables=page_tables,
            page_words=page_words,
            image_based_pages=image_based_pages,
            first_page_text=first_page_text,
            metadata=metadata,
        )
        self.logger.info("pdf_extracted", extra={"extra": {"document_id": document_id, "pages": result.pages_count, "ocr_pages": metadata["ocr"]["applied_pages"]}})
        return result

    def fingerprint(self, extracted: ExtractionResult) -> Dict[str, object]:
        """
        Produce a lightweight fingerprint from the first page's text to help
        select a downstream parser/template. Returns a dict with likely bank,
        currency, country, and key anchors discovered, along with a confidence.

        Heuristics prefer explicit anchors (e.g., "IBAN", "Sort Code", "BVN",
        bank names) and currency symbols/codes.
        """

        first_text = extracted.first_page_text or ""
        text_norm = first_text.lower()

        anchors: Dict[str, List[str]] = {}

        def findall(pattern: str, flags: int = 0) -> List[str]:
            try:
                return re.findall(pattern, first_text, flags)
            except re.error:
                return []

        # Common identifiers
        account_numbers = findall(r"(?:account\s*(?:no\.|number)[:\s]*)([0-9\-\s]{6,})", flags=re.IGNORECASE)
        iban_matches = findall(r"\b[A-Z]{2}[0-9A-Z]{13,34}\b")
        sort_codes = findall(r"(?:sort\s*code[:\s]*)([0-9\-]{6,8})", flags=re.IGNORECASE)
        bvn_matches = findall(r"\b(?:bvn)[:\s]*([0-9]{11})\b", flags=re.IGNORECASE)
        routing_numbers = findall(r"(?:routing\s*number[:\s]*)([0-9\-]{5,})", flags=re.IGNORECASE)

        if account_numbers:
            anchors["account_number"] = account_numbers
        if iban_matches:
            anchors["iban"] = iban_matches
        if sort_codes:
            anchors["sort_code"] = sort_codes
        if bvn_matches:
            anchors["bvn"] = bvn_matches
        if routing_numbers:
            anchors["routing_number"] = routing_numbers

        # Currency hints
        currency = None
        currency_conf = 0.0
        currency_patterns: List[Tuple[str, str, float]] = [
            (r"\bngn\b|₦", "NGN", 0.8),
            (r"\busd\b|\$", "USD", 0.6),
            (r"\beur\b|€", "EUR", 0.6),
            (r"\bgpb\b|\bgbp\b|£", "GBP", 0.6),
            (r"\binr\b|₹", "INR", 0.6),
            (r"\bcad\b|\$", "CAD", 0.4),
        ]
        for pattern, code, conf in currency_patterns:
            if re.search(pattern, text_norm):
                currency = code
                currency_conf = conf
                break

        # Bank/template hints
        bank = None
        bank_conf = 0.0
        bank_hints: List[Tuple[str, str, float]] = [
            (r"\baccess bank\b", "ACCESS_BANK", 0.9),
            (r"\bgtbank\b|guaranty trust", "GTBANK", 0.9),
            (r"\bzenith bank\b", "ZENITH", 0.9),
            (r"\bfirst bank\b|firstbank", "FIRST_BANK", 0.9),
            (r"\bunited bank for africa\b|\buba\b", "UBA", 0.8),
            (r"\bpolaris bank\b", "POLARIS", 0.8),
            (r"\bunion bank\b", "UNION_BANK", 0.8),
            (r"\bsterling bank\b", "STERLING", 0.8),
            (r"\bfidelity bank\b", "FIDELITY", 0.8),
            (r"\becobank\b", "ECOBANK", 0.8),
            (r"\bkeystone bank\b", "KEYSTONE", 0.8),
            (r"\bfcmb\b|first city monument", "FCMB", 0.8),
            (r"\bstanbic ibtc\b", "STANBIC_IBTC", 0.8),
            (r"\bopay\b|\bopay digital\b|\bopay bank\b", "OPAY", 0.95),
        ]
        for pattern, code, conf in bank_hints:
            if re.search(pattern, text_norm):
                bank = code
                bank_conf = conf
                break

        # Country hints
        country = None
        country_conf = 0.0
        if bank in {"ACCESS_BANK", "GTBANK", "ZENITH", "FIRST_BANK", "UBA", "POLARIS", "UNION_BANK", "STERLING", "FIDELITY", "ECOBANK", "KEYSTONE", "FCMB", "STANBIC_IBTC"}:
            country = "NG"
            country_conf = 0.7
        elif currency in {"USD", "CAD"} and bank is None:
            country = None
            country_conf = 0.2

        # Confidence combines signals
        confidence = 0.2
        if bank:
            confidence += bank_conf
        if currency:
            confidence += currency_conf * 0.5
        if anchors:
            confidence += min(0.4, 0.1 * len(anchors))
        confidence = max(0.0, min(1.0, confidence))

        return {
            "document_id": extracted.document_id,
            "pages_count": extracted.pages_count,
            "bank": bank,
            "currency": currency,
            "country": country,
            "anchors": anchors,
            "confidence": confidence,
        }

    # ---------------------------------------------
    # Normalization stage
    # ---------------------------------------------
    def normalize_rows(self, extracted: ExtractionResult, fingerprint: Optional[Dict[str, object]] = None) -> List[Dict[str, object]]:
        """
        Convert extracted tables/text into a list of canonical row dicts with keys:
        - date (datetime.date)
        - description (str)
        - debit (Decimal | None)
        - credit (Decimal | None)
        - amount (Decimal | None)  # signed; positive credit, negative debit
        - balance (Decimal | None)
        - currency (str | None)
        - source ("table" | "text")
        - page_index (int)

        Strategy:
        1) Prefer tables. Detect header row, map to canonical columns, parse subsequent rows.
        2) Fallback to text lines by regex: detect date + amount patterns.
        """

        currency_hint = None
        if fingerprint and isinstance(fingerprint, dict):
            currency_hint = fingerprint.get("currency")  # type: ignore[assignment]

        normalized: List[Dict[str, object]] = []

        metrics = {
            "rows_from_tables": 0,
            "rows_from_tokens": 0,
            "rows_from_text": 0,
            "pages_with_tables": 0,
            "pages_with_tokens": 0,
        }

        # Tables first
        for page_index, tables in enumerate(extracted.page_tables):
            seq_counter = 0
            for table in tables:
                if not table:
                    continue
                header_map = self._detect_header_mapping(table[0])
                data_rows = table[1:] if header_map else table
                for raw_row in data_rows:
                    row_dict: Dict[str, object] = {
                        "date": None,
                        "description": None,
                        "debit": None,
                        "credit": None,
                        "amount": None,
                        "balance": None,
                        "currency": currency_hint,
                        "source": "table",
                        "page_index": page_index,
                        "row_seq": seq_counter,
                    }
                    seq_counter += 1

                    if header_map:
                        for col_index, canonical_name in header_map.items():
                            if col_index < len(raw_row):
                                value = raw_row[col_index]
                                self._assign_cell_value(row_dict, canonical_name, value, currency_hint)
                    else:
                        # Heuristic mapping for 3-6 column tables without header
                        # Try to parse last numeric as amount/balance, first date-like as date
                        for idx, value in enumerate(raw_row):
                            canonical_name = None
                            if self._looks_like_date(value) and row_dict["date"] is None:
                                canonical_name = "date"
                            elif self._looks_like_amount(value):
                                # Prefer to fill amount first, then balance
                                canonical_name = "amount" if row_dict["amount"] is None else "balance"
                            else:
                                canonical_name = "description" if row_dict["description"] in (None, "") else None
                            if canonical_name:
                                self._assign_cell_value(row_dict, canonical_name, value, currency_hint)

                    # Post-process amount sign from debit/credit if needed
                    if row_dict["amount"] is None:
                        debit = row_dict["debit"]
                        credit = row_dict["credit"]
                        if isinstance(debit, Decimal):
                            row_dict["amount"] = Decimal(0) - debit
                        elif isinstance(credit, Decimal):
                            row_dict["amount"] = credit

                    # Basic validity: need at least description or amount
                    if row_dict["description"] or row_dict["amount"]:
                        normalized.append(row_dict)
                        metrics["rows_from_tables"] += 1
            if tables:
                metrics["pages_with_tables"] += 1
            # Early stop if we have enough rows overall
            if getattr(config, "EARLY_STOP_MIN_ROWS", 0) > 0 and len(normalized) >= int(getattr(config, "EARLY_STOP_MIN_ROWS", 0)):
                return normalized

        # Stream-mode reconstruction from word tokens (header-anchored) if tables yielded nothing
        if not normalized and getattr(extracted, "page_words", None):
            for page_index, words in enumerate(extracted.page_words):
                if not words:
                    continue
                rows_tokens = words_to_rows(words)
                if not rows_tokens:
                    continue
                # Find probable header row by token text matching
                header_candidates = []
                for r in rows_tokens[:8]:  # inspect first few rows for headers
                    tokens_texts = [str(t.get("text", "")) for t in r.get("tokens", [])]
                    canonical_hits = [self._standardize_header_token(x) for x in tokens_texts]
                    if any(h in {"date", "description", "debit", "credit", "amount", "balance"} for h in canonical_hits if h):
                        header_candidates.append(r)
                header_row = header_candidates[0] if header_candidates else None
                if header_row is None:
                    continue
                bands = infer_column_bands(header_row.get("tokens", []))
                if not bands:
                    continue
                # Build header map from header tokens assigned to bands
                header_cells_tokens = assign_tokens_to_columns(header_row.get("tokens", []), bands)
                header_cells_text = [" ".join(t.get("text", "") for t in cell).strip() for cell in header_cells_tokens]
                header_map = {}
                for idx, token_text in enumerate(header_cells_text):
                    canonical = self._standardize_header_token(token_text)
                    if canonical:
                        header_map[idx] = canonical

                # Require minimal header
                values = set(header_map.values())
                if not ("date" in values and ("amount" in values or "debit" in values or "credit" in values or "description" in values)):
                    header_map = {}

                # Identify description column index for merge heuristic
                description_col_index = None
                for col_idx, name in header_map.items():
                    if name == "description":
                        description_col_index = col_idx
                        break

                # Process subsequent rows
                seq_counter = 0
                for r in rows_tokens[rows_tokens.index(header_row) + 1:]:
                    row_tokens = r.get("tokens", [])
                    if not row_tokens:
                        continue
                    col_tokens = assign_tokens_to_columns(row_tokens, bands)
                    col_texts = [" ".join(t.get("text", "") for t in col).strip() for col in col_tokens]
                    if not any(col_texts):
                        continue
                    row_dict: Dict[str, object] = {
                        "date": None,
                        "description": None,
                        "debit": None,
                        "credit": None,
                        "amount": None,
                        "balance": None,
                        "currency": currency_hint,
                        "source": "tokens",
                        "page_index": page_index,
                        "row_seq": seq_counter,
                    }
                    seq_counter += 1
                    if header_map:
                        for col_index, canonical_name in header_map.items():
                            if col_index < len(col_texts):
                                self._assign_cell_value(row_dict, canonical_name, col_texts[col_index], currency_hint)

                    # Post-process amount sign from debit/credit if needed
                    if row_dict["amount"] is None:
                        debit = row_dict["debit"]
                        credit = row_dict["credit"]
                        if isinstance(debit, Decimal):
                            row_dict["amount"] = Decimal(0) - debit
                        elif isinstance(credit, Decimal):
                            row_dict["amount"] = credit

                    # Continuation row heuristic: no numbers/date, description present → merge with previous
                    is_number_present = any(isinstance(row_dict[k], Decimal) for k in ("amount", "debit", "credit", "balance"))
                    has_date = isinstance(row_dict["date"], date)
                    if (not is_number_present and not has_date and isinstance(row_dict.get("description"), str) and (row_dict["description"] or "")):
                        if normalized:
                            prev = normalized[-1]
                            if isinstance(prev.get("description"), str):
                                # If we know the description column index, favor merge when that column has text
                                if description_col_index is not None and description_col_index < len(col_texts):
                                    desc_piece = col_texts[description_col_index]
                                else:
                                    desc_piece = row_dict.get("description") or ""
                                combined = (prev["description"] + " " + str(desc_piece)).strip()
                                prev["description"] = combined
                                continue
                    if row_dict["description"] or row_dict["amount"]:
                        normalized.append(row_dict)
                        metrics["rows_from_tokens"] += 1
                if rows_tokens:
                    metrics["pages_with_tokens"] += 1
                if getattr(config, "EARLY_STOP_MIN_ROWS", 0) > 0 and len(normalized) >= int(getattr(config, "EARLY_STOP_MIN_ROWS", 0)):
                    return normalized

        # Text fallback for pages where no rows captured
        if not normalized:
            amount_token = r"(?:\(?-?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{2})?\)?|\(?-?\d+(?:\.\d{2})?\)?)"
            date_token = r"\b(?:\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4}|\d{4}[\-/]\d{1,2}[\-/]\d{1,2}|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{2,4})\b"
            line_re = re.compile(rf"(?P<date>{date_token}).{{1,80}}?(?P<amount>{amount_token})(?:\s+(?P<balance>{amount_token}))?", re.IGNORECASE)

            for page_index, text in enumerate(extracted.page_texts):
                seq_counter = 0
                for raw_line in (text or "").splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    m = line_re.search(line)
                    if not m:
                        continue

                    parsed_date = self._parse_date(m.group("date"))
                    amount = self._parse_amount(m.group("amount"), currency_hint)
                    balance = self._parse_amount(m.group("balance") if m.groupdict().get("balance") else None, currency_hint)

                    description = line
                    # Remove the date and amount fragments for a cleaner description if possible
                    try:
                        description = re.sub(re.escape(m.group("date")), "", description, flags=re.IGNORECASE)
                        description = description.replace(m.group("amount"), "")
                        if m.groupdict().get("balance"):
                            description = description.replace(m.group("balance"), "")
                        description = re.sub(r"\s{2,}", " ", description).strip(" -|:")
                    except re.error:
                        pass

                    normalized.append({
                        "date": parsed_date,
                        "description": description or None,
                        "debit": None if (amount is None or amount >= 0) else (Decimal(0) - amount),
                        "credit": amount if (amount is not None and amount > 0) else None,
                        "amount": amount,
                        "balance": balance,
                        "currency": currency_hint,
                        "source": "text",
                        "page_index": page_index,
                        "row_seq": seq_counter,
                    })
                    seq_counter += 1
                    metrics["rows_from_text"] += 1
                    if getattr(config, "EARLY_STOP_MIN_ROWS", 0) > 0 and len(normalized) >= int(getattr(config, "EARLY_STOP_MIN_ROWS", 0)):
                        return normalized

        # Attach metrics into metadata for downstream quality gates
        try:
            extracted.metadata.setdefault("metrics", {})  # type: ignore[index]
            extracted.metadata["metrics"].update(metrics)  # type: ignore[index]
        except Exception:
            pass
        return normalized

    # -------------------------- balances detection --------------------------
    def _detect_balances_from_text(self, texts: List[str], currency_hint: Optional[str]) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        opening: Optional[Decimal] = None
        closing: Optional[Decimal] = None
        if not texts:
            return opening, closing
        # Search likely opening on early pages, closing on later pages
        head_text = "\n".join(texts[: min(2, len(texts))])
        tail_text = "\n".join(texts[max(0, len(texts) - 2):])

        def find_amount(patterns: List[str], hay: str) -> Optional[Decimal]:
            for pat in patterns:
                try:
                    m = re.search(pat, hay, flags=re.IGNORECASE)
                except re.error:
                    m = None
                if m:
                    amt_txt = m.group(1)
                    amt = self._parse_amount(amt_txt, currency_hint)
                    if isinstance(amt, Decimal):
                        return amt
            return None

        opening_patterns = [
            r"opening\s*balance[:\s-]*([^\n]+)",
            r"balance\s*brought\s*forward[:\s-]*([^\n]+)",
            r"start(?:ing)?\s*balance[:\s-]*([^\n]+)",
        ]
        closing_patterns = [
            r"closing\s*balance[:\s-]*([^\n]+)",
            r"balance\s*carried\s*forward[:\s-]*([^\n]+)",
            r"end(?:ing)?\s*balance[:\s-]*([^\n]+)",
        ]

        opening = find_amount(opening_patterns, head_text)
        closing = find_amount(closing_patterns, tail_text)
        return opening, closing

    # -------------------------- helpers --------------------------
    def _detect_header_mapping(self, header_row: List[str]) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        for idx, cell in enumerate(header_row):
            canonical = self._standardize_header_token(cell)
            if canonical:
                mapping[idx] = canonical
        # Require at least date and one of amount/debit/credit or description for a header to be valid
        values = set(mapping.values())
        if not ("date" in values and ("amount" in values or "debit" in values or "credit" in values or "description" in values)):
            return {}
        return mapping

    def _standardize_header_token(self, token: Optional[str]) -> Optional[str]:
        if token is None:
            return None
        t = str(token).strip().lower()
        t = re.sub(r"\s+", " ", t)
        synonyms = {
            # dates
            "date": "date",
            "value date": "date",
            "transaction date": "date",
            "posting date": "date",
            # description
            "description": "description",
            "details": "description",
            "narration": "description",
            "particulars": "description",
            "remarks": "description",
            # debit/credit/amount
            "debit": "debit",
            "withdrawal": "debit",
            "dr": "debit",
            "credit": "credit",
            "deposit": "credit",
            "cr": "credit",
            "amount": "amount",
            "transaction amount": "amount",
            # balance
            "balance": "balance",
            "running balance": "balance",
            "available balance": "balance",
            # currency
            "currency": "currency",
        }
        # exact match
        if t in synonyms:
            return synonyms[t]
        # partials
        if "date" in t:
            return "date"
        if "narration" in t or "details" in t or "descr" in t:
            return "description"
        if re.search(r"\bdr\b|debit|withdraw", t):
            return "debit"
        if re.search(r"\bcr\b|credit|deposit", t):
            return "credit"
        if "amount" in t:
            return "amount"
        if "balance" in t:
            return "balance"
        if "currency" in t:
            return "currency"
        return None

    def _assign_cell_value(self, row_dict: Dict[str, object], canonical_name: str, value: str, currency_hint: Optional[str]) -> None:
        if canonical_name == "date":
            row_dict["date"] = self._parse_date(value)
            return
        if canonical_name in ("debit", "credit", "amount", "balance"):
            parsed = self._parse_amount(value, currency_hint)
            if parsed is None:
                return
            if canonical_name == "debit":
                row_dict["debit"] = parsed
            elif canonical_name == "credit":
                row_dict["credit"] = parsed
            elif canonical_name == "amount":
                row_dict["amount"] = parsed
            elif canonical_name == "balance":
                row_dict["balance"] = parsed
            return
        if canonical_name == "currency":
            row_dict["currency"] = (value or "").strip() or row_dict.get("currency")
            return
        if canonical_name == "description":
            existing = row_dict.get("description") or ""
            combined = (str(existing) + " " + (value or "")).strip()
            row_dict["description"] = combined

    def _looks_like_date(self, text: Optional[str]) -> bool:
        if not text:
            return False
        s = str(text).strip()
        patterns = [
            r"^\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4}$",
            r"^\d{4}[\-/]\d{1,2}[\-/]\d{1,2}$",
            r"^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{2,4}$",
        ]
        return any(re.search(p, s, re.IGNORECASE) for p in patterns)

    def _looks_like_amount(self, text: Optional[str]) -> bool:
        if not text:
            return False
        s = str(text)
        return bool(re.search(r"\(?-?\d{1,3}(?:[,\s]\d{3})*(?:\.\d{2})?\)?|\(?-?\d+(?:\.\d{2})?\)?", s))

    def _parse_date(self, text: Optional[str]) -> Optional[date]:
        if not text:
            return None
        s = str(text).strip()
        candidates = [
            "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y",
            "%m/%d/%Y", "%m-%d-%Y",
            "%Y-%m-%d", "%Y/%m/%d",
            "%d/%m/%y", "%m/%d/%y", "%y-%m-%d",
        ]
        # Try month name formats
        try:
            dt = datetime.strptime(s, "%b %d %Y")
            return dt.date()
        except Exception:
            pass
        try:
            dt = datetime.strptime(s, "%b %d, %Y")
            return dt.date()
        except Exception:
            pass
        try:
            dt = datetime.strptime(s, "%d %b %Y")
            return dt.date()
        except Exception:
            pass
        try:
            dt = datetime.strptime(s, "%d %b, %Y")
            return dt.date()
        except Exception:
            pass

        for fmt in candidates:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.date()
            except Exception:
                continue
        # Attempt to extract a date token inside longer strings
        m = re.search(r"(\d{4}[\-/]\d{1,2}[\-/]\d{1,2}|\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4})", s)
        if m:
            return self._parse_date(m.group(1))
        return None

    def _parse_amount(self, text: Optional[str], currency_hint: Optional[str]) -> Optional[Decimal]:
        if not text:
            return None
        s = str(text)
        # Detect negative via parentheses or explicit sign or DR suffix
        is_negative = False
        if "(" in s and ")" in s:
            is_negative = True
        if re.search(r"(^|\s)-", s):
            is_negative = True
        if re.search(r"\bdr\b", s.lower()):
            is_negative = True
        if re.search(r"\bcr\b", s.lower()):
            # CR shouldn’t override explicit negatives; only set positive if nothing else indicates negative
            is_negative = is_negative and True or False

        # Locale-aware parsing with Babel when available; fallback to legacy
        value: Optional[Decimal] = None
        s_wo_ccy = re.sub(r"[A-Za-z₦$€£₹]", "", s)
        if _HAVE_BABEL:
            # Heuristics: detect separators by pattern
            # Examples: 1.234,56 → decimal="," grouping="." ; 1,234.56 → decimal="." grouping="," ; 1 234,56 thin space
            candidate_strings = []
            # remove parentheses/spaces for parsing
            candidate_strings.append(re.sub(r"[()\s]", "", s_wo_ccy))
            for cand in candidate_strings:
                try:
                    # Try common locales implicitly by replacing separators
                    if re.search(r"\d+\.\d{3},\d{2}$", cand) or ("," in cand and "." in cand and cand.rfind(",") > cand.rfind(".")):
                        # likely EU style
                        normalized = cand.replace(".", "").replace(",", ".")
                        value = Decimal(normalized)
                        break
                    if re.search(r"\d+,\d{3}\.\d{2}$", cand) or ("," in cand and "." in cand and cand.rfind(".") > cand.rfind(",")):
                        # likely US style
                        normalized = cand.replace(",", "")
                        value = Decimal(normalized)
                        break
                    # Fallback: single separator is decimal
                    if cand.count(",") == 1 and "." not in cand:
                        value = Decimal(cand.replace(",", "."))
                        break
                    if cand.count(".") == 1 and "," not in cand:
                        value = Decimal(cand)
                        break
                except Exception:
                    value = None
        if value is None:
            # Remove currency symbols and non-numeric separators
            s_clean = s_wo_ccy.replace(",", "").replace(" ", "")
            s_clean = s_clean.replace("(", "").replace(")", "")
            s_clean = s_clean.strip()
            if s_clean in ("", "-", "."):
                return None
            try:
                value = Decimal(s_clean)
            except (InvalidOperation, ValueError):
                return None
        if is_negative:
            value = Decimal(0) - value
        return value

    # ---------------------------------------------
    # Parsing stage
    # ---------------------------------------------
    def parse_transactions(
        self,
        normalized_rows: List[Dict[str, object]],
        document_id: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> List[Dict[str, object]]:
        """
        Convert normalized row dicts into canonical transactions with deterministic ids.

        Output fields per transaction:
        - id: stable hash of (document_id, account_id, date, description_norm, amount)
        - account_id: passthrough
        - date: datetime.date | None
        - description: str | None
        - amount: Decimal | None (signed: credit positive, debit negative)
        - debit: Decimal | None
        - credit: Decimal | None
        - balance: Decimal | None
        - currency: str | None
        - page_index: int | None
        - raw: original normalized row
        """

        transactions: List[Dict[str, object]] = []

        for row in normalized_rows:
            # Ensure amount is consistent with debit/credit if present
            amount = row.get("amount")
            debit = row.get("debit")
            credit = row.get("credit")

            if amount is None:
                if isinstance(debit, Decimal):
                    amount = Decimal(0) - debit
                elif isinstance(credit, Decimal):
                    amount = credit

            # Skip noise rows with neither description nor numbers
            has_number = isinstance(amount, Decimal) or isinstance(debit, Decimal) or isinstance(credit, Decimal) or isinstance(row.get("balance"), Decimal)
            description = row.get("description")
            description_norm = None
            if isinstance(description, str):
                description_norm = re.sub(r"\s+", " ", description).strip(" -|:") or None

            if not description_norm and not has_number:
                continue

            parsed_date = row.get("date")
            if parsed_date is None and isinstance(row.get("date"), str):
                parsed_date = self._parse_date(row.get("date"))

            currency = row.get("currency") if isinstance(row.get("currency"), str) else None
            balance = row.get("balance") if isinstance(row.get("balance"), Decimal) else None
            page_index = row.get("page_index") if isinstance(row.get("page_index"), int) else None

            # Recompute debit/credit from amount for consistency
            if isinstance(amount, Decimal):
                if amount < 0:
                    debit = Decimal(0) - amount
                    credit = None
                elif amount > 0:
                    credit = amount
                    debit = None
                else:
                    debit = None
                    credit = None

            # Compute deterministic id
            txn_id = self._compute_transaction_id(
                document_id=document_id,
                account_id=account_id,
                date_value=parsed_date,
                description=description_norm,
                amount=amount if isinstance(amount, Decimal) else None,
                page_index=page_index,
            )

            transactions.append({
                "id": txn_id,
                "account_id": account_id,
                "date": parsed_date,
                "description": description_norm,
                "amount": amount if isinstance(amount, Decimal) else None,
                "debit": debit if isinstance(debit, Decimal) else None,
                "credit": credit if isinstance(credit, Decimal) else None,
                "balance": balance,
                "currency": currency,
                "page_index": page_index,
                "row_seq": row.get("row_seq") if isinstance(row.get("row_seq"), int) else None,
                "raw": row,
            })

        return transactions

    def _compute_transaction_id(
        self,
        document_id: Optional[str],
        account_id: Optional[str],
        date_value: Optional[date],
        description: Optional[str],
        amount: Optional[Decimal],
        page_index: Optional[int],
    ) -> str:
        """
        Deterministic, collision-resistant id derived from stable fields.
        Page index included for extra disambiguation when descriptions repeat.
        """
        parts: List[str] = []
        parts.append(str(document_id or ""))
        parts.append(str(account_id or ""))
        parts.append(date_value.isoformat() if isinstance(date_value, date) else "")
        parts.append((description or "").lower())
        parts.append(str(amount.quantize(Decimal("0.01"))) if isinstance(amount, Decimal) else "")
        parts.append(str(page_index if isinstance(page_index, int) else ""))
        key = "|".join(parts)
        return hashlib.sha1(key.encode("utf-8")).hexdigest()

    # ---------------------------------------------
    # Validation stage
    # ---------------------------------------------
    def validate_statement(
        self,
        transactions: List[Dict[str, object]],
        opening_balance: Optional[Decimal] = None,
        closing_balance: Optional[Decimal] = None,
        drop_duplicates: bool = False,
    ) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
        """
        Validate and optionally amend transactions by:
        - Checking opening + sum(amounts) ~= closing (tolerance)
        - Rebuilding running balances when an opening is available
        - Flagging duplicates via a secondary content hash

        Returns (possibly_adjusted_transactions, summary_dict).
        """

        # Defensive copy
        txns = list(transactions)

        # Normalize amounts and sorting for stable computations
        normalized_txns: List[Dict[str, object]] = []
        for t in txns:
            t_copy = dict(t)
            amount = t_copy.get("amount")
            if not isinstance(amount, Decimal):
                debit = t_copy.get("debit")
                credit = t_copy.get("credit")
                if isinstance(debit, Decimal):
                    amount = Decimal(0) - debit
                elif isinstance(credit, Decimal):
                    amount = credit
                else:
                    amount = None
                t_copy["amount"] = amount
            normalized_txns.append(t_copy)

        # Sort by (date, page_index, row_seq, description) for deterministic ordering
        def sort_key(t: Dict[str, object]) -> Tuple[str, int, int, str]:
            date_part = ""  # empty sorts first
            d = t.get("date")
            if isinstance(d, date):
                date_part = d.isoformat()
            page_part = t.get("page_index") if isinstance(t.get("page_index"), int) else -1
            row_seq = t.get("row_seq") if isinstance(t.get("row_seq"), int) else -1
            descr = t.get("description") if isinstance(t.get("description"), str) else ""
            return (date_part, page_part, row_seq, descr)

        normalized_txns.sort(key=sort_key)

        # Totals
        total: Decimal = Decimal("0")
        for t in normalized_txns:
            if isinstance(t.get("amount"), Decimal):
                total += t["amount"]  # type: ignore[index]

        # Detect balances present
        indices_with_balance: List[int] = [i for i, t in enumerate(normalized_txns) if isinstance(t.get("balance"), Decimal)]
        detected_opening: Optional[Decimal] = None
        detected_closing: Optional[Decimal] = None

        if indices_with_balance:
            first_idx = indices_with_balance[0]
            last_idx = indices_with_balance[-1]
            bal_first: Decimal = normalized_txns[first_idx]["balance"]  # type: ignore[index]
            bal_last: Decimal = normalized_txns[last_idx]["balance"]  # type: ignore[index]

            # Opening before the first balanced txn: balance_after_first - sum(amounts up to that index)
            subtotal = Decimal("0")
            for i in range(0, first_idx + 1):
                amt = normalized_txns[i].get("amount")
                if isinstance(amt, Decimal):
                    subtotal += amt
            detected_opening = bal_first - subtotal
            detected_closing = bal_last

        # Choose opening to use
        opening_used = opening_balance if isinstance(opening_balance, Decimal) else detected_opening
        closing_used = closing_balance if isinstance(closing_balance, Decimal) else detected_closing

        # Balance check
        expected_closing = None
        balance_check_passed = None
        balance_check_delta = None
        if isinstance(opening_used, Decimal):
            expected_closing = opening_used + total
            if isinstance(closing_used, Decimal):
                delta = (expected_closing - closing_used).copy_abs()
                tolerance = Decimal("0.02")
                balance_check_passed = delta <= tolerance
                balance_check_delta = expected_closing - closing_used

        # Rebuild running balances if we have an opening
        running_balance_rebuilt = False
        if isinstance(opening_used, Decimal):
            running = opening_used
            for i, t in enumerate(normalized_txns):
                amt = t.get("amount") if isinstance(t.get("amount"), Decimal) else None
                if isinstance(amt, Decimal):
                    running = running + amt
                if not isinstance(t.get("balance"), Decimal):
                    normalized_txns[i]["balance"] = running
                    running_balance_rebuilt = True
                else:
                    # Sanity: keep track of drift but do not override explicit balances
                    pass

        # Duplicate detection
        seen_keys: Dict[str, str] = {}
        duplicates_indices: List[int] = []
        for idx, t in enumerate(normalized_txns):
            key_parts: List[str] = []
            d = t.get("date")
            key_parts.append(d.isoformat() if isinstance(d, date) else "")
            descr = t.get("description") if isinstance(t.get("description"), str) else ""
            key_parts.append(re.sub(r"\s+", " ", descr).strip().lower())
            amt = t.get("amount")
            key_parts.append(str(amt.copy_abs().quantize(Decimal("0.01"))) if isinstance(amt, Decimal) else "")
            ccy = t.get("currency") if isinstance(t.get("currency"), str) else ""
            key_parts.append(ccy)
            key = "|".join(key_parts)
            if key in seen_keys:
                duplicates_indices.append(idx)
                normalized_txns[idx]["duplicate_of"] = seen_keys[key]
            else:
                seen_keys[key] = t.get("id") if isinstance(t.get("id"), str) else key

        duplicates_removed = 0
        if drop_duplicates and duplicates_indices:
            # Remove in reverse order to keep indices stable
            for idx in reversed(duplicates_indices):
                normalized_txns.pop(idx)
                duplicates_removed += 1

        summary: Dict[str, object] = {
            "transactions_count": len(normalized_txns),
            "sum_amount": total,
            "opening_balance_input": opening_balance,
            "closing_balance_input": closing_balance,
            "opening_balance_detected": detected_opening,
            "closing_balance_detected": detected_closing,
            "opening_balance_used": opening_used,
            "closing_balance_used": closing_used,
            "expected_closing": expected_closing,
            "balance_check_passed": balance_check_passed,
            "balance_check_delta": balance_check_delta,
            "running_balance_rebuilt": running_balance_rebuilt,
            "duplicates_found": len(duplicates_indices),
            "duplicates_removed": duplicates_removed,
        }

        return normalized_txns, summary

    # ---------------------------------------------
    # PII masking stage
    # ---------------------------------------------

    def _stringify_obj(self, obj):
        if obj is None:
            return ""
        if isinstance(obj, str):
            return obj
        from decimal import Decimal
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (int, float, bool)):
            return str(obj)
        if isinstance(obj, list):
            return [self._stringify_obj(x) for x in obj]
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                out[str(k)] = self._stringify_obj(v)
            return out
        return str(obj)


    # def mask_pii(self, transactions: List[Dict[str, object]]) -> List[Dict[str, object]]:
    #     """
    #     Deterministically redact sensitive tokens in string fields (descriptions and raw).
    #     Keeps last 4 digits where safe and replaces the rest with a stable token.
    #     Masking is conservative by default: only account-like numbers unless configured otherwise.
    #     """
    #     masked: List[Dict[str, object]] = []
    #     for t in transactions:
    #         t_copy = dict(t)
    #         # Never mask numeric fields like debit/credit/amount unless explicitly enabled
    #         if not self.mask_amounts:
    #             # ensure numeric fields stay as-is
    #             pass
    #         # Mask top-level description
    #         if isinstance(t_copy.get("description"), str):
    #             t_copy["description"] = self._mask_pii_text(t_copy["description"])  # type: ignore[index]
    #         # Mask recursively in raw
    #         t_copy["raw"] = self._mask_in_obj(t_copy.get("raw"))
    #         masked.append(t_copy)
    #     return masked

    def mask_pii(self, transactions):
        masked = []
        for t in transactions:
            t_copy = dict(t)
            if isinstance(t_copy.get("description"), str):
                t_copy["description"] = self._mask_pii_text(t_copy["description"])
            masked_raw = self._mask_in_obj(t_copy.get("raw"))
            t_copy["raw"] = self._stringify_obj(masked_raw)  # <-- stringify leaves
            masked.append(t_copy)
        return masked
    
    def _mask_in_obj(self, obj):  # type: ignore[no-untyped-def]
        if obj is None:
            return None
        if isinstance(obj, str):
            return self._mask_pii_text(obj)
        if isinstance(obj, list):
            return [self._mask_in_obj(x) for x in obj]
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                out[k] = self._mask_in_obj(v)
            return out
        return obj

    def _mask_pii_text(self, text: str) -> str:
        s = text

        # IBAN
        def _sub_iban(m: re.Match) -> str:
            token = self._deterministic_token("IBAN", m.group(0), keep_last=4)
            return token
        s = re.sub(r"\b[A-Z]{2}[0-9A-Z]{13,34}\b", _sub_iban, s)

        # Card PAN (13-19 digits, allow separators). Validate with Luhn to reduce false positives.
        def _sub_pan(m: re.Match) -> str:
            raw = m.group(0)
            digits = re.sub(r"[^0-9]", "", raw)
            if 13 <= len(digits) <= 19 and self._luhn_check(digits):
                return self._deterministic_token("CARD", digits, keep_last=4)
            return raw
        # Only apply PAN masking if not restricted to account-only
        if not self.mask_account_like_numbers_only:
            s = re.sub(r"\b(?:\d[ -]?){13,19}\b", _sub_pan, s)

        # BVN (Nigeria 11 digits)
        def _sub_bvn(m: re.Match) -> str:
            digits = m.group(0)
            return self._deterministic_token("BVN", digits, keep_last=2)
        if not self.mask_account_like_numbers_only:
            s = re.sub(r"\b\d{11}\b", _sub_bvn, s)

        # Sort code (UK xx-xx-xx)
        def _sub_sort(m: re.Match) -> str:
            return self._deterministic_token("SORT", m.group(0).replace("-", ""), keep_last=2)
        if not self.mask_account_like_numbers_only:
            s = re.sub(r"\b\d{2}-\d{2}-\d{2}\b", _sub_sort, s)

        # Account numbers (NUBAN 10 digits; also 8-12 generic when preceded by keywords)
        def _sub_acct(m: re.Match) -> str:
            digits = re.sub(r"\D", "", m.group(0))
            if 8 <= len(digits) <= 12:
                return self._deterministic_token("ACCT", digits, keep_last=4)
            return m.group(0)
        s = re.sub(r"(?:(?:account\s*(?:no\.|number)[:\s]*)?[#:;\s-]*)((?:\d[\s-]?){8,12})\b", _sub_acct, s, flags=re.IGNORECASE)

        # Routing number (US 9 digits) when preceded by keyword
        def _sub_routing(m: re.Match) -> str:
            digits = re.sub(r"\D", "", m.group(0))
            if len(digits) == 9:
                return self._deterministic_token("ROUTING", digits, keep_last=2)
            return m.group(0)
        if not self.mask_account_like_numbers_only:
            s = re.sub(r"routing\s*number[:\s-]*([0-9\-\s]{9,})", _sub_routing, s, flags=re.IGNORECASE)

        # Email addresses
        def _sub_email(m: re.Match) -> str:
            return self._deterministic_token("EMAIL", m.group(0), keep_last=0)
        if not self.mask_account_like_numbers_only:
            s = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", _sub_email, s)

        return s

    def _deterministic_token(self, tag: str, original: str, keep_last: int = 4) -> str:
        clean = re.sub(r"\s", "", original)
        h = hashlib.sha1(clean.encode("utf-8")).hexdigest()[:10]
        tail = re.sub(r"\D", "", clean)[-keep_last:] if keep_last > 0 else ""
        if tail:
            return f"<{tag}:{h}:{tail}>"
        return f"<{tag}:{h}>"

    def _luhn_check(self, digits: str) -> bool:
        total = 0
        reverse = digits[::-1]
        for idx, ch in enumerate(reverse):
            n = ord(ch) - 48
            if idx % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return (total % 10) == 0

    # ---------------------------------------------
    # Orchestrator
    # ---------------------------------------------
    def process_pdf(
        self,
        content: bytes,
        filename: Optional[str] = None,
        account_id: Optional[str] = None,
        opening_balance: Optional[Decimal] = None,
        closing_balance: Optional[Decimal] = None,
        drop_duplicates: bool = False,
    ) -> Dict[str, object]:
        """
        Full processing pipeline: extract -> fingerprint -> normalize -> parse -> validate -> mask.
        Returns a structured result suitable for persistence or API response.
        """

        # extracted = self.extract (x, y)
        extracted = self.extract(content, filename)



        
        # Idempotency: short-circuit if result already persisted
        if self.storage is not None and hasattr(self.storage, "has") and self.storage.has(extracted.document_id):
            try:
                result = self.storage.get_result(extracted.document_id)
                self.logger.info("pdf_cache_hit", extra={"extra": {"document_id": extracted.document_id}})
                return result
            except Exception:
                pass
        fingerprint = self.fingerprint(extracted)
        # If a template parser supports this doc, allow it to provide rows
        # my fingerprint helps me make decision on how to go about processing the document
        selected_parser = self.parser_registry.select(fingerprint)
        rows = []
        if selected_parser is not None:
            try:
                rows = selected_parser.normalize_rows({
                    "page_texts": extracted.page_texts,
                    "page_tables": extracted.page_tables,
                    "metadata": extracted.metadata,
                    "pages_count": extracted.pages_count,
                    "image_based_pages": extracted.image_based_pages,
                })
            except Exception:
                rows = []
        if not rows:
            rows = self.normalize_rows(extracted, fingerprint)


        #get transactions in the pdf 
        txns = self.parse_transactions(rows, document_id=extracted.document_id, account_id=account_id)

        # If balances not provided, attempt detection from statement text
        if opening_balance is None or closing_balance is None:
            auto_open, auto_close = self._detect_balances_from_text(extracted.page_texts, fingerprint.get("currency") if isinstance(fingerprint, dict) else None)
            opening_balance = opening_balance if isinstance(opening_balance, Decimal) else auto_open
            closing_balance = closing_balance if isinstance(closing_balance, Decimal) else auto_close

        # Validate and give these transactions a standardized structure
        validated_txns, summary = self.validate_statement(txns, opening_balance, closing_balance, drop_duplicates)


        # Mask sensitive data like account numbers.
        masked_txns = self.mask_pii(validated_txns)

        result: Dict[str, object] = {
            "document_id": extracted.document_id,
            "filename": filename,
            "pages_count": extracted.pages_count,
            "fingerprint": fingerprint,
            "validation": summary,
            "transactions": masked_txns,
        }
        # Expose OCR info for upstream analysis
        try:
            result["ocr"] = extracted.metadata.get("ocr")  # type: ignore[assignment]
        except Exception:
            pass
        # Surface extraction metrics for alerts and dashboards
        try:
            metrics = extracted.metadata.get("metrics") if isinstance(extracted.metadata, dict) else None
            if isinstance(metrics, dict):
                result["metrics"] = metrics
        except Exception:
            pass
        # Persist artifacts and result for replay/debug
        if self.storage is not None and hasattr(self.storage, "persist"):
            try:
                artifacts = {
                    "pages_count": extracted.pages_count,
                    "page_tables": extracted.page_tables,
                    "image_based_pages": extracted.image_based_pages,
                    "first_page_text": extracted.first_page_text,
                    "metadata": extracted.metadata,
                }
                if getattr(config, "ARTIFACTS_INCLUDE_TEXTS", False):
                    artifacts["page_texts"] = extracted.page_texts
                if getattr(config, "ARTIFACTS_INCLUDE_WORDS", False):
                    artifacts["page_words"] = extracted.page_words
                # Optional debug overlays
                overlays: Dict[str, bytes] = {}
                if getattr(config, "DEBUG_OVERLAY_ENABLED", False):
                    max_pages = min(int(getattr(config, "DEBUG_OVERLAY_MAX_PAGES", 5)), extracted.pages_count)
                    for i in range(max_pages):
                        try:
                            # Best-effort render with words; column bands unavailable post-normalization, so omit here
                            img = render_page_overlay(content, i, words=extracted.page_words[i] if i < len(extracted.page_words) else None)
                            buf = BytesIO()
                            img.save(buf, format="PNG")
                            overlays[f"overlay_page_{i+1}.png"] = buf.getvalue()
                        except Exception:
                            continue
                self.storage.persist(
                    extracted.document_id,
                    artifacts=artifacts,
                    result=result,
                    raw_pdf=content,
                )
                # Store overlays as separate files if any
                if overlays:
                    try:
                        st = self.storage if hasattr(self.storage, "_doc_dir") else ArtifactStorage()
                        doc_dir = st._doc_dir(extracted.document_id)
                        import os
                        for name, data in overlays.items():
                            with open(os.path.join(doc_dir, name), "wb") as f:
                                f.write(data)
                    except Exception:
                        pass
                self.logger.info("pdf_persisted", extra={"extra": {"document_id": extracted.document_id}})
            except Exception:
                pass

        # with the result, can we create a dictionary and store as pdf? I don't want the original file to ever be mutable. so pdf and zero edits to original data. Updates can only happen on a file_id in the knowledge base
        return result


