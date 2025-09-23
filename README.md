## PDF Statement Processing

Run the CLI on one or more PDFs:

```bash
python -m services.pdf_cli /path/to/statement.pdf --storage /tmp/ff_artifacts
```

It prints a JSON summary per file:

```json
{"file": "/path/to/statement.pdf", "document_id": "...", "pages": 3, "bank": "OPAY", "currency": "NGN", "transactions": 42, "balance_check_passed": true}
```

### Environment Variables

- `FF_ARTIFACTS_DIR`: base directory for artifacts (default: `/tmp/futurefinance_artifacts`).
- `FF_OCR_ENABLED`: enable OCR fallback (`true`/`false`, default: `true`).
- `FF_OCR_MAX_PAGES`: max pages to OCR (default: `5`).
- `FF_OCR_DPI`: OCR render DPI (default: `300`).
- `FF_MAX_PAGES`: hard cap on pages processed per PDF (default: `200`).
- `FF_MAX_CHARS_PER_PAGE`: cap on text characters per page (default: `20000`).

### Integration

`DataService.process_pdf(content, filename)` returns a `CleanedStatementDocument` using the pipeline in `services/pdf_statement_processor.py`.


