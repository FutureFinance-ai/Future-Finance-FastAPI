from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from decimal import Decimal
from datetime import date


class StatementParser(ABC):
    """Base interface for bank-specific statement parsers."""

    name: str = "BASE"
    version: str = "0.1.0"

    @abstractmethod
    def supports(self, fingerprint: Dict[str, object]) -> bool:
        """Return True if this parser should handle the document."""
        raise NotImplementedError

    @abstractmethod
    def normalize_rows(self, extracted: Dict[str, object]) -> List[Dict[str, object]]:
        """Return canonical rows from parser-specific rules.
        Fallback to pipeline generic when returning empty list.
        """
        raise NotImplementedError


class ParserRegistry:
    def __init__(self, parsers: Optional[List[StatementParser]] = None) -> None:
        self.parsers = parsers or []

    def select(self, fingerprint: Dict[str, object]) -> Optional[StatementParser]:
        for p in self.parsers:
            try:
                if p.supports(fingerprint):
                    return p
            except Exception:
                continue
        return None


class NgGenericParser(StatementParser):
    name = "NG_GENERIC"
    version = "0.1.0"

    def supports(self, fingerprint: Dict[str, object]) -> bool:
        # Nigeria-focused generic rules if country is NG
        return (fingerprint or {}).get("country") == "NG"

    def normalize_rows(self, extracted: Dict[str, object]) -> List[Dict[str, object]]:
        # Example: prefer tables; apply NG-specific sign rules (DR negative, CR positive)
        page_tables = extracted.get("page_tables") or []
        page_texts = extracted.get("page_texts") or []
        result: List[Dict[str, object]] = []
        # Basic shim: let pipeline generic handle; this parser is a hook for future specialization
        return result


class OpayParser(StatementParser):
    name = "OPAY"
    version = "0.1.0"

    def supports(self, fingerprint: Dict[str, object]) -> bool:
        bank = (fingerprint or {}).get("bank")
        if isinstance(bank, str) and bank.upper() in {"OPAY", "OPAY_BANK", "OPAY_NG"}:
            return True
        # Also support if first-page text includes OPAY keywords
        title = (fingerprint or {}).get("anchors", {})
        return False

    def normalize_rows(self, extracted: Dict[str, object]) -> List[Dict[str, object]]:
        page_tables = extracted.get("page_tables") or []
        result: List[Dict[str, object]] = []
        # OPAY statements typically present tabular data with headers like:
        # Date | Narration | Type | Amount | Balance (may vary)
        # We try to detect OPAY-friendly headers and map accordingly.
        header_synonyms = {
            "date": {"date", "tran date", "transaction date", "value date"},
            "description": {"description", "narration", "details", "remark", "particulars"},
            "amount": {"amount", "transaction amount", "amt"},
            "balance": {"balance", "running balance", "bal"},
            "type": {"type", "dr/cr", "dc"},
            "credit": {"credit", "cr"},
            "debit": {"debit", "dr"},
        }

        def canon(cell: Optional[str]) -> Optional[str]:
            if cell is None:
                return None
            t = str(cell).strip().lower()
            for key, values in header_synonyms.items():
                if t in values:
                    return key
            if "date" in t:
                return "date"
            if any(x in t for x in ["narration", "details", "descr", "remark", "particular"]):
                return "description"
            if "balance" in t:
                return "balance"
            if "amount" in t or "amt" in t:
                return "amount"
            if t in ("dr", "debit"):
                return "debit"
            if t in ("cr", "credit"):
                return "credit"
            if t in ("type", "dr/cr", "dc"):
                return "type"
            return None

        for page_index, tables in enumerate(page_tables):
            for table in tables:
                if not table:
                    continue
                header = table[0]
                mapping: Dict[int, str] = {}
                for idx, cell in enumerate(header):
                    c = canon(cell)
                    if c:
                        mapping[idx] = c
                # Must have date and at least amount/credit/debit to treat as OPAY table
                vals = set(mapping.values())
                if not ("date" in vals and ("amount" in vals or "credit" in vals or "debit" in vals)):
                    continue
                for raw_row in table[1:]:
                    row: Dict[str, object] = {
                        "date": None,
                        "description": None,
                        "debit": None,
                        "credit": None,
                        "amount": None,
                        "balance": None,
                        "currency": None,
                        "source": "table",
                        "page_index": page_index,
                    }
                    for idx, key in mapping.items():
                        if idx >= len(raw_row):
                            continue
                        val = raw_row[idx]
                        if key == "date":
                            row["date"] = val
                        elif key == "description":
                            row["description"] = (str(row.get("description") or "") + " " + str(val or "")).strip()
                        elif key == "balance":
                            row["balance"] = val
                        elif key == "amount":
                            row["amount"] = val
                        elif key == "credit":
                            row["credit"] = val
                        elif key == "debit":
                            row["debit"] = val
                        elif key == "type":
                            # If type column indicates DR/CR, we will use it during parse stage
                            row["type"] = str(val or "").strip().lower()
                    result.append(row)
        return result


