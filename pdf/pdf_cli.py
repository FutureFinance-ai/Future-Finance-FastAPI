from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict

from services.pdf_statement_processor import PdfStatementProcessor
from services.artifact_storage import ArtifactStorage


def main() -> None:
    parser = argparse.ArgumentParser(description="Process PDF bank statements and print a summary")
    parser.add_argument("paths", nargs="+", help="PDF file paths")
    parser.add_argument("--storage", default=None, help="Base dir for artifact storage (default: /tmp/futurefinance_artifacts)")
    args = parser.parse_args()

    storage = ArtifactStorage(args.storage) if args.storage else ArtifactStorage()
    processor = PdfStatementProcessor(storage=storage)

    for path in args.paths:
        if not os.path.exists(path):
            print(json.dumps({"file": path, "error": "not_found"}))
            continue
        try:
            with open(path, "rb") as f:
                content = f.read()
            result: Dict[str, Any] = processor.process_pdf(content, filename=os.path.basename(path))
            # result: Dict[str, Any] = processor.process_pdf(x, y)
            summary = {
                "file": path,
                "document_id": result.get("document_id"),
                "pages": result.get("pages_count"),
                "bank": (result.get("fingerprint") or {}).get("bank"),
                "currency": (result.get("fingerprint") or {}).get("currency"),
                "transactions": len(result.get("transactions") or []),
                "balance_check_passed": (result.get("validation") or {}).get("balance_check_passed"),
            }
            print(json.dumps(summary))
        except Exception as e:
            print(json.dumps({"file": path, "error": str(e)}))


if __name__ == "__main__":
    main()


