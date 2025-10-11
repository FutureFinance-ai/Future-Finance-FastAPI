# data_service.py
from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Any
from io import BytesIO
from datetime import datetime
import re
import pdfplumber
import pandas as pd
import ast
import json

# Import your schema dataclasses (assumed to exist)
from schemas.UploadData import CleanedStatementDocument
from pdf.pdf_statement_processor import PdfStatementProcessor
from pdf.artifact_storage import ArtifactStorage


class DataService:
    """
    Robust DataService for ingesting Excel and PDF bank statements.
    - process_excel(...)  -> returns CleanedStatementDocument
    - process_pdf_plumber(...) -> returns CleanedStatementDocument
    """

    # ---------- configuration / regex ----------
    DATE_TIME_PATTERNS = [
        r"\b\d{4}\s+[A-Za-z]{3,9}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\b",
        r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b",   
        r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", 
    ]
    COMPILED_DATE_RE = re.compile("|".join(DATE_TIME_PATTERNS))
    AMOUNT_RE = re.compile(r"([+-]?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)")

    def __init__(self):
        storage = ArtifactStorage()  # TODO: make base path configurable
        self.pdf_processor = PdfStatementProcessor(storage=storage)

    # ----------------------------
    # Sensitive data detection & masking
    # ----------------------------
    def _mask_account_id(self, raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        raw = str(raw)
        m = re.search(r"\b\d{4,16}\b", raw)
        if not m:
            return self._mask_sensitive(raw)
        acct = m.group(0)
        return acct[-4:].rjust(len(acct), "X")

    def _mask_sensitive(self, text: Optional[str]) -> Optional[str]:
        """Mask account/card numbers, emails, phone numbers, IDs in a string."""
        if text is None:
            return None
        s = str(text)

        # Mask long digit sequences (account/card numbers) - keep last 4
        s = re.sub(r"\b\d{8,16}\b", lambda m: "X" * (len(m.group(0)) - 4) + m.group(0)[-4:], s)

        # Mask emails
        s = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[redacted-email]", s)

        # Mask phone numbers (simple)
        s = re.sub(r"\+?\d[\d\-\s]{6,}\d", "[redacted-phone]", s)

        # Mask other ID-like sequences
        s = re.sub(r"\b\d{3,4}[-\s]?\d{3,4}[-\s]?\d{3,4}\b", "[redacted-id]", s)

        return s

    def _sanitize_row(self, row: Dict[str, Any]) -> Dict[str, str]:
        """Mask and stringify all values so that raw conforms to Dict[str, str]."""
        def to_string(val: Any) -> str:
            if val is None:
                return ""
            # datetime/date
            if hasattr(val, "isoformat"):
                try:
                    return val.isoformat()
                except Exception:
                    pass
            # numbers / bools
            if isinstance(val, (int, float, bool)):
                return str(val)
            # bytes
            if isinstance(val, (bytes, bytearray)):
                try:
                    return val.decode("utf-8", errors="ignore")
                except Exception:
                    return str(val)
            # lists/dicts -> compact string repr
            if isinstance(val, (list, dict)):
                try:
                    import json as _json
                    return _json.dumps(val, ensure_ascii=False)
                except Exception:
                    return str(val)
            return str(val)

        sanitized: Dict[str, str] = {}
        for k, v in row.items():
            sanitized[k] = self._mask_sensitive(to_string(v)) or ""
        return sanitized

    def _coerce_date(self, tx: Dict[str, Any]) -> Optional[datetime.date]:
        """Best-effort to return a valid date for a transaction; None if unavailable."""
        # 1) Direct date/datetime
        dval = tx.get("date")
        if dval is not None:
            try:
                if isinstance(dval, datetime):
                    return dval.date()
                # date-like with isoformat
                if hasattr(dval, "isoformat") and not isinstance(dval, str):
                    return dval  # type: ignore[return-value]
                # string → parse
                d = pd.to_datetime(str(dval), errors="coerce")
                if d is not None and not pd.isna(d):
                    return d.date()
            except Exception:
                pass
        # 2) Look into raw fields for candidates
        raw = tx.get("raw") if isinstance(tx, dict) else None
        if isinstance(raw, dict):
            for key in ("date", "value_date", "trans_time"):
                try:
                    candidate = raw.get(key)
                    if candidate:
                        d = pd.to_datetime(str(candidate), errors="coerce")
                        if d is not None and not pd.isna(d):
                            return d.date()
                except Exception:
                    continue
        return None

    # ----------------------------
    # Column / schema helpers (Excel)
    # ----------------------------
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df

    def _detect_column(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for name in candidates:
            if name in df.columns:
                return name
        for col in df.columns:
            for name in candidates:
                if name in col:
                    return col
        return None

    def _detect_amount_columns(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        amount_col = self._detect_column(df, [
            "amount", "amt", "transaction amount", "value", "txn amount", "net amount",
        ])
        credit_col = self._detect_column(df, [
            "credit", "cr", "deposit", "paid in", "credits", "receipt",
        ])
        debit_col = self._detect_column(df, [
            "debit", "dr", "withdrawal", "paid out", "debits", "payment",
        ])
        return amount_col, credit_col, debit_col

    def _parse_date_column(self, df: pd.DataFrame) -> Optional[str]:
        date_col = self._detect_column(df, [
            "date", "tran date", "transaction date", "value date", "post date",
        ])
        if date_col is None:
            return None
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        return date_col

    def _detect_description_column(self, df: pd.DataFrame) -> Optional[str]:
        return self._detect_column(df, [
            "description", "narration", "details", "particulars", "remarks", "memo",
        ])

    def _detect_balance_column(self, df: pd.DataFrame) -> Optional[str]:
        return self._detect_column(df, [
            "balance", "closing balance", "running balance", "bal", "available balance",
        ])

    # ----------------------------
    # Categorization (unchanged)
    # ----------------------------
    def _categorize(self, description: str, amount: float, txn_type: str) -> Tuple[str, Optional[str]]:
        desc = (description or "").lower()
        rules = [
            ("salary", ("Income", "Salary")),
            ("payroll", ("Income", "Salary")),
            ("interest", ("Income", "Interest")),
            ("refund", ("Income", "Refund")),
            ("transfer", ("Transfers", None)),
            ("neft", ("Transfers", None)),
            ("rtgs", ("Transfers", None)),
            ("imps", ("Transfers", None)),
            ("pos", ("Expenses", "POS Purchase")),
            ("purchase", ("Expenses", "Shopping")),
            ("amazon", ("Expenses", "Shopping")),
            ("jumia", ("Expenses", "Shopping")),
            ("aliexpress", ("Expenses", "Shopping")),
            ("uber", ("Transport", None)),
            ("bolt", ("Transport", None)),
            ("lyft", ("Transport", None)),
            ("atm", ("Cash", "ATM Withdrawal")),
            ("withdrawal", ("Cash", None)),
            ("grocery", ("Groceries", None)),
            ("supermarket", ("Groceries", None)),
            ("rent", ("Housing", None)),
            ("utility", ("Utilities", None)),
            ("electric", ("Utilities", None)),
            ("water", ("Utilities", None)),
            ("internet", ("Utilities", None)),
            ("fee", ("Fees", None)),
            ("charge", ("Fees", None)),
            ("tax", ("Taxes", None)),
        ]
        for needle, (cat, subcat) in rules:
            if needle in desc:
                return cat, subcat
        return ("Income", None) if txn_type == "credit" else ("Expenses", None)

    # ----------------------------
    # Row -> CategorizedTransaction
    # ----------------------------
    def _row_to_transaction(
        self,
        row: pd.Series,
        date_col: Optional[str],
        desc_col: Optional[str],
        amount: float,
        txn_type: str,
        account: Optional[str],
    ):
        category, subcategory = self._categorize(str(row.get(desc_col, "")), amount, txn_type)
        return {
            "date": row.get(date_col),
            "description": self._mask_sensitive(str(row.get(desc_col, "")).strip()),
            "amount": round(float(abs(amount)), 2),
            "type": txn_type,
            "category": category,
            "subcategory": subcategory,
            "account": (account[-4:].rjust(len(account), "X") if account else None),
            "raw": self._sanitize_row(row.to_dict())
        }
        # return CategorizedTransaction(
        #     date=row.get(date_col),
        #     description=self._mask_sensitive(str(row.get(desc_col, "")).strip()),
        #     amount=round(float(abs(amount)), 2),
        #     type=txn_type,
        #     category=category,
        #     subcategory=subcategory,
        #     account=(account[-4:].rjust(len(account), "X") if account else None),
        #     raw=self._sanitize_row(row.to_dict())
        # )

    # ----------------------------
    # Extraction from DataFrame (Excel path)
    # ----------------------------
    def _extract_transactions(
        self,
        df: pd.DataFrame,
        account: Optional[str]
    ) -> Tuple[List[Dict[str, Any]], float, float]:
        df = df.copy()
        df = df.replace({"-": None, "": None})

        date_col = self._parse_date_column(df)
        desc_col = self._detect_description_column(df)
        amount_col, credit_col, debit_col = self._detect_amount_columns(df)

        transactions: List[Dict[str, Any]] = []
        total_credits = 0.0
        total_debits = 0.0

        if credit_col or debit_col:
            if credit_col and credit_col in df.columns:
                for _, r in df[df[credit_col].notna()].iterrows():
                    try:
                        amt = float(str(r[credit_col]).replace(",", ""))
                    except Exception:
                        continue
                    transactions.append(self._row_to_transaction(r, date_col, desc_col, amt, "credit", account))
                    total_credits += abs(amt)
            if debit_col and debit_col in df.columns:
                for _, r in df[df[debit_col].notna()].iterrows():
                    try:
                        amt = float(str(r[debit_col]).replace(",", ""))
                    except Exception:
                        continue
                    transactions.append(self._row_to_transaction(r, date_col, desc_col, amt, "debit", account))
                    total_debits += abs(amt)
        elif amount_col:
            type_col = self._detect_column(df, ["type", "dr/cr", "transaction type", "dc"])
            for _, r in df[df[amount_col].notna()].iterrows():
                try:
                    amt_val = float(str(r[amount_col]).replace(",", ""))
                except Exception:
                    continue
                inferred_type = "credit" if amt_val > 0 else "debit"
                if type_col and str(r.get(type_col, "")).strip().lower() in ["cr", "credit"]:
                    inferred_type = "credit"
                elif type_col and str(r.get(type_col, "")).strip().lower() in ["dr", "debit"]:
                    inferred_type = "debit"
                if inferred_type == "credit":
                    total_credits += abs(amt_val)
                else:
                    total_debits += abs(amt_val)
                transactions.append(self._row_to_transaction(r, date_col, desc_col, amt_val, inferred_type, account))
        else:
            return [], 0.0, 0.0

        transactions.sort(key=lambda t: (t["date"] or pd.Timestamp.min.date(), t["description"]))
        return transactions, round(total_credits, 2), round(total_debits, 2)

    # ----------------------------
    # Balances inference
    # ----------------------------
    def _detect_currency(self, df: pd.DataFrame) -> str:
      """Try to detect currency symbol or code from headers or values."""
      # Check column names
      for col in df.columns:
          col_lower = col.lower()
          if "usd" in col_lower:
              return "USD"
          if "ngn" in col_lower or "₦" in col_lower:
              return "NGN"
          if "eur" in col_lower or "€" in col_lower:
              return "EUR"
          if "gbp" in col_lower or "£" in col_lower:
              return "GBP"
      amount_col, _, _ = self._detect_amount_columns(df)
      if amount_col and amount_col in df.columns:
          sample_vals = df[amount_col].dropna().astype(str).head(20).values
          for v in sample_vals:
              if "₦" in v:
                  return "NGN"
              if "$" in v:
                  return "USD"
              if "€" in v:
                  return "EUR"
              if "£" in v:
                  return "GBP"

      return "UNKNOWN"
    
    def _infer_balances(
        self,
        df: pd.DataFrame,
        total_credits: float,
        total_debits: float,
    ) -> Tuple[float, float]:
        bal_col = self._detect_balance_column(df)
        if bal_col and bal_col in df.columns:
            series = pd.to_numeric(pd.Series(df[bal_col]).astype(str).str.replace(",", ""), errors="coerce").dropna()
            if not series.empty:
                opening = float(series.iloc[0])
                closing = float(series.iloc[-1])
                return round(opening, 2), round(closing, 2)
        opening = 0.0
        closing = opening + total_credits - total_debits
        return round(opening, 2), round(closing, 2)

    # ----------------------------
    # Excel public entry point (returns CleanedStatementDocument)
    # ----------------------------
    def process_excel(
        self,
        content: bytes,
        filename: str,
        statement_month: Optional[str] = None,
        sheet: Optional[str] = None,
        currency: Optional[str] = None,
        account_id: Optional[str] = None,
    ) -> CleanedStatementDocument:
        try:
            xl = pd.ExcelFile(pd.io.common.BytesIO(content))
        except Exception as e:
            raise ValueError(f"Could not read Excel: {e}")

        sheet_name = sheet or (xl.sheet_names[0] if xl.sheet_names else None)
        if sheet_name is None:
            raise ValueError("No sheets found in the uploaded Excel file")

        df = xl.parse(sheet_name)
        df = self._standardize_columns(df)

        # Auto-detect if not provided
        if not account_id:
            account_id = self._detect_account_id(df)

        if not currency:
            currency = self._detect_currency(df)

        transactions, total_credits, total_debits = self._extract_transactions(df, account_id)
        opening, closing = self._infer_balances(df, total_credits, total_debits)

        # Infer statement_month if not provided
        if statement_month is None:
            dates = [t["date"] for t in transactions if t["date"] is not None]
            if dates:
                first = min(dates)
                statement_month = f"{first.year:04d}-{first.month:02d}"
            else:
                statement_month = "unknown"

        safe_filename = self._mask_sensitive(filename)

        doc = CleanedStatementDocument(
            account_id=(account_id[-4:].rjust(len(account_id), "X") if account_id else None),
            statement_month=statement_month,
            opening_balance=opening,
            closing_balance=closing,
            total_credits=total_credits,
            total_debits=total_debits,
            currency=currency,
            transactions=transactions,
            metadata={
                "source_filename": safe_filename,
                "sheet": sheet_name,
                "num_rows": str(df.shape[0]),
            },
        )
        return doc

    # ----------------------------
    # PDF helpers (stitch + parse)
    # ----------------------------
    def _clean_amount(self, amt_str: str) -> Optional[float]:
        if not amt_str:
            return None
        amt_clean = str(amt_str).replace("$", "").replace("₦", "").replace("€", "").replace("£", "")
        negative = False
        if amt_clean.startswith("(") and amt_clean.endswith(")"):
            negative = True
            amt_clean = amt_clean.strip("()")
        try:
            val = float(amt_clean.replace(",", ""))
        except Exception:
            return None
        if str(amt_str).strip().startswith("-") or negative:
            return -abs(val)
        return val

    def _parse_date_safe(self, date_str: str) -> Optional[datetime]:
        try:
            return pd.to_datetime(date_str, errors="coerce").to_pydatetime()
        except Exception:
            return None

    def _clean_amount_token(self, tok: str) -> Optional[float]:
        if not tok:
            return None
        t = tok.strip()
        neg = False
        if t.startswith("(") and t.endswith(")"):
            neg = True
            t = t[1:-1]
        if t.startswith("+") or t.startswith("-"):
            if t.startswith("-"):
                neg = True
            t = t.lstrip("+-")
        t = t.replace(",", "")
        try:
            v = float(t)
            return -abs(v) if neg else abs(v)
        except Exception:
            return None

    def _stitch_rows(self, raw_lines: List[str]) -> List[str]:
        """Group lines into transactions by detecting date patterns."""
        stitched = []
        buffer: List[str] = []
        for line in raw_lines:
            if self.COMPILED_DATE_RE.search(line):
                if buffer:
                    stitched.append(" ".join(buffer))
                    buffer = []
            buffer.append(line.strip())
        if buffer:
            stitched.append(" ".join(buffer))
        return stitched

    def _parse_stitched_row(self, row: str) -> Optional[Dict]:
        dt_match = self.COMPILED_DATE_RE.search(row)
        if not dt_match:
            return None
        dt_token = dt_match.group(0)

        amounts = self.AMOUNT_RE.findall(row)
        if not amounts:
            return None

        if len(amounts) >= 2:
            debit_credit_tok = amounts[-2]
            balance_tok = amounts[-1]
        else:
            debit_credit_tok = amounts[-1]
            balance_tok = None

        debit_credit = self._clean_amount_token(debit_credit_tok)
        balance = self._clean_amount_token(balance_tok) if balance_tok else None

        desc = row.replace(dt_token, "")
        desc = desc.replace(debit_credit_tok, "")
        if balance_tok:
            desc = desc.replace(balance_tok, "")
        desc = re.sub(r"\s{2,}", " ", desc).strip(" -|:")

        tx_ref_match = re.search(r"\b\d{8,}\b", row)
        tx_ref = tx_ref_match.group(0) if tx_ref_match else None

        channel_match = re.search(
            r"\bE-Channel\b|\bPOS\b|\bATM\b", row, re.IGNORECASE
        )
        channel = channel_match.group(0) if channel_match else None

        try:
            value_dt = None
            try:
                value_dt = datetime.strptime(dt_token, "%Y %b %d %H:%M:%S")
            except Exception:
                value_dt = pd.to_datetime(dt_token, errors="coerce")
            value_date = (
                value_dt.date().isoformat() if value_dt is not None else None
            )
        except Exception:
            value_date = None

        return {
            "trans_time": dt_token,
            "value_date": value_date,
            "description": desc,
            "debit_credit": debit_credit,
            "balance": balance,
            "channel": channel,
            "transaction_reference": tx_ref,
            "counterparty": None,
            "raw": row,
        }
        def parse_variable_to_transaction_dict(self, data: Any) -> Dict[str, Dict[str, Any]]:
            """
            Convert a variable that may contain a raw statement dump (string),
            a JSON/Python-repr of transactions, or a pre-built structure into
            a dictionary keyed by a stable index (e.g. "000", "001"), where
            each value is a sub-dictionary representing one transaction.

            Heuristics:
            - If `data` is a list of dict-like items → index to dict
            - If `data` is a dict with a `transactions` list → unwrap
            - If `data` is a string → try JSON, then Python literal_eval
            - If still a string → treat as raw lines with optional "NNN:" prefixes
            and parse using existing PDF row parsing logic
            """
        # 1) Direct Python structures
        if isinstance(data, list):
            transactions_list: List[Dict[str, Any]] = []
            for item in data:
                if isinstance(item, dict):
                    transactions_list.append(item)
                else:
                    try:
                        transactions_list.append(dict(item))  # type: ignore[arg-type]
                    except Exception:
                        transactions_list.append({"raw": str(item)})
            return {f"{i:03d}": tx for i, tx in enumerate(transactions_list)}

        if isinstance(data, dict):
            if isinstance(data.get("transactions"), list):
                txns = data["transactions"]
                result: Dict[str, Dict[str, Any]] = {}
                for i, item in enumerate(txns):
                    result[f"{i:03d}"] = item if isinstance(item, dict) else {"raw": str(item)}
                return result
            # Otherwise: if values look like tx dicts already
            if all(isinstance(v, dict) for v in data.values()):
                # Normalize keys to 3-digit if not already
                normalized: Dict[str, Dict[str, Any]] = {}
                for i, (k, v) in enumerate(data.items()):
                    key = str(k)
                    if not key.isdigit() or len(key) != 3:
                        key = f"{i:03d}"
                    normalized[key] = v  # type: ignore[assignment]
                return normalized
            # Fallback
            return {"000": {"raw": self._mask_sensitive(str(data))}}

        # 2) Bytes → decode
        if isinstance(data, (bytes, bytearray)):
            try:
                data = data.decode("utf-8", errors="ignore")  # type: ignore[assignment]
            except Exception:
                data = str(data)

        # 3) Strings → try JSON / Python repr
        if isinstance(data, str):
            text = data.strip()
            # Try JSON first
            try:
                parsed = json.loads(text)
                return self.parse_variable_to_transaction_dict(parsed)
            except Exception:
                pass
            # Try Python literal (list/dict repr)
            try:
                parsed_py = ast.literal_eval(text)
                return self.parse_variable_to_transaction_dict(parsed_py)
            except Exception:
                pass

            # 4) Treat as raw statement text. Support optional "NNN:" id prefixes.
            lines = [ln.strip() for ln in text.splitlines() if ln and ln.strip()]

            # Group by leading numeric id like "001:"
            groups: List[Tuple[Optional[str], List[str]]] = []
            current_id: Optional[str] = None
            buffer: List[str] = []
            for raw_line in lines:
                m = re.match(r"^(\d{3}):\s*(.*)$", raw_line)
                if m:
                    # flush previous group
                    if buffer:
                        groups.append((current_id, buffer))
                        buffer = []
                    current_id = m.group(1)
                    remainder = m.group(2).strip()
                    if remainder:
                        buffer.append(remainder)
                else:
                    buffer.append(raw_line)
            if buffer:
                groups.append((current_id, buffer))

            result: Dict[str, Dict[str, Any]] = {}

            if groups:
                for i, (gid, glines) in enumerate(groups):
                    key = gid if gid is not None else f"{i:03d}"
                    joined = " | ".join(glines)
                    parsed = self._parse_stitched_row(joined)
                    if parsed is None:
                        result[key] = {"raw": self._mask_sensitive(joined)}
                    else:
                        # Sanitize string fields in parsed
                        result[key] = self._sanitize_row(parsed)  # type: ignore[arg-type]
                return result

            # Fallback: use stitcher to detect per-transaction rows
            stitched = self._stitch_rows(lines)
            for i, row in enumerate(stitched):
                key = f"{i:03d}"
                parsed = self._parse_stitched_row(row)
                if parsed is None:
                    result[key] = {"raw": self._mask_sensitive(row)}
                else:
                    result[key] = self._sanitize_row(parsed)  # type: ignore[arg-type]
            return result

        # Final fallback: best-effort stringification
        return {"000": {"raw": self._mask_sensitive(str(data))}}

    # ----------------------------
    # PDF public entry point (returns CleanedStatementDocument)
    # ----------------------------

    def process_pdf(self, content: bytes, filename: str) -> CleanedStatementDocument:
        """End-to-end PDF processing using PdfStatementProcessor, returning CleanedStatementDocument."""
        result = self.pdf_processor.process_pdf(content, filename=filename, account_id=None)

        # Convert processor transactions into CategorizedTransaction dicts
        categorized: List[Dict[str, Any]] = []
        total_credits = 0.0
        total_debits = 0.0
        for t in result.get("transactions", []):
            # Ensure a valid date or skip the transaction to satisfy schema
            t_date = self._coerce_date(t)
            if t_date is None:
                continue
            desc_raw = str(t.get("description") or "")
            amt = t.get("amount")
            amt_float: Optional[float] = None
            if amt is not None:
                try:
                    amt_float = float(str(amt))
                except Exception:
                    amt_float = None
            txn_type = "credit" if (amt_float is not None and amt_float > 0) else "debit"
            if amt_float is not None:
                if txn_type == "credit":
                    total_credits += abs(amt_float)
                else:
                    total_debits += abs(amt_float)

            category, subcategory = self._categorize(desc_raw, abs(amt_float or 0.0), txn_type)
            categorized.append({
                "date": t_date,
                "description": self._mask_sensitive(desc_raw),
                "amount": round(float(abs(amt_float or 0.0)), 2),
                "type": txn_type,
                "category": category,
                "subcategory": subcategory,
                "account": None,
                "raw": self._sanitize_row({k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in t.items()}),
            })

        # Opening/closing from validation summary when available
        summary = result.get("validation", {}) if isinstance(result.get("validation"), dict) else {}
        opening = summary.get("opening_balance_used")
        closing = summary.get("closing_balance_used")
        try:
            opening_f = float(str(opening)) if opening is not None else None
        except Exception:
            opening_f = None
        try:
            closing_f = float(str(closing)) if closing is not None else None
        except Exception:
            closing_f = None
        if opening_f is None:
            opening_f = 0.0
        if closing_f is None:
            closing_f = round(opening_f + total_credits - total_debits, 2)

        # Currency hint
        currency = None
        fp = result.get("fingerprint", {}) if isinstance(result.get("fingerprint"), dict) else {}
        if isinstance(fp.get("currency"), str):
            currency = fp.get("currency")

        # Infer statement_month from dates
        dates = [c.get("date") for c in categorized if c.get("date") is not None]
        if dates:
            try:
                first = min(dates)
                statement_month = f"{first.year:04d}-{first.month:02d}"
            except Exception:
                statement_month = "unknown"
        else:
            statement_month = "unknown"

        safe_filename = self._mask_sensitive(filename)

        doc = CleanedStatementDocument(
            account_id=None,
            statement_month=statement_month,
            opening_balance=round(float(opening_f), 2),
            closing_balance=round(float(closing_f), 2),
            total_credits=round(float(total_credits), 2),
            total_debits=round(float(total_debits), 2),
            currency=currency,
            transactions=categorized,
            metadata={
                "source_filename": safe_filename,
                "pages": str(result.get("pages_count")),
                "document_id": str(result.get("document_id")),
            },
        )
        return doc


