from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional
from services.config import ARTIFACTS_BASE_DIR


class _JsonEncoder(json.JSONEncoder):
    def default(self, o: Any):  # type: ignore[override]
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


class ArtifactStorage:
    """
    Simple filesystem-backed storage for PDF processing artifacts and results.
    Stores under base_dir/<document_id>:
      - raw.pdf
      - extracted.json
      - result.json
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        self.base_dir = base_dir or ARTIFACTS_BASE_DIR
        os.makedirs(self.base_dir, exist_ok=True)

    def _doc_dir(self, document_id: str) -> str:
        return os.path.join(self.base_dir, document_id)

    def has(self, document_id: str) -> bool:
        doc_dir = self._doc_dir(document_id)
        return os.path.exists(os.path.join(doc_dir, "result.json"))

    def get_result(self, document_id: str) -> Dict[str, Any]:
        doc_dir = self._doc_dir(document_id)
        result_path = os.path.join(doc_dir, "result.json")
        if not os.path.exists(result_path):
            raise FileNotFoundError(f"No stored result for {document_id}")
        with open(result_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def persist(
        self,
        document_id: str,
        artifacts: Dict[str, Any],
        result: Dict[str, Any],
        raw_pdf: Optional[bytes] = None,
    ) -> None:
        doc_dir = self._doc_dir(document_id)
        os.makedirs(doc_dir, exist_ok=True)

        if raw_pdf is not None:
            raw_path = os.path.join(doc_dir, "raw.pdf")
            try:
                with open(raw_path, "wb") as f:
                    f.write(raw_pdf)
            except Exception:
                # best-effort; do not fail pipeline on write issues
                pass

        extracted_path = os.path.join(doc_dir, "extracted.json")
        try:
            with open(extracted_path, "w", encoding="utf-8") as f:
                json.dump(artifacts, f, ensure_ascii=False, indent=2, cls=_JsonEncoder)
        except Exception:
            pass

        result_path = os.path.join(doc_dir, "result.json")
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, cls=_JsonEncoder)


