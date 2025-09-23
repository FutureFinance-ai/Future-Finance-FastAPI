from pathlib import Path
from services.data_service import DataService  # make sure this has process_pdf_with_plumber
import pdfplumber
from schemas.UploadData import DocumentUploadResponse

def main():
    service = DataService()
    pdf_path = "/home/ghost/Downloads/test2.pdf"

    # Read file as bytes
    with open(pdf_path, "rb") as f:
        content = f.read()

    print(f"[*] Testing pdfplumber parsing for: {pdf_path}")

    # === Debug: peek at raw extracted content ===
    print("\n=== Raw Extracted (first 40 lines/tables) ===")
    lines = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Grab tables if any
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    for row in table:
                        lines.append(" | ".join([str(cell or "").strip() for cell in row]))
            else:
                text = page.extract_text() or ""
                for raw_line in text.splitlines():
                    lines.append(raw_line.strip())

    for i, line in enumerate(lines[:10]):
        print(f"{i:03d}: {line}")

    # === Run the new processor ===
    doc = service.process_pdf(content, filename=Path(pdf_path).name)
    doc["metadata"]["doc_url"] = "AWS/save/here"
    # result = service.parse_variable_to_transaction_dict(doc)
    print(DocumentUploadResponse(num_transactions=doc["metadata"]["num_transactions"], filename=doc["metadata"]["filename"], doc_url=doc["metadata"]["doc_url"]))
    print(doc)

    # print("\n=== Parsed Document ===")
    # # print(f"Account ID: {doc.account_id}")
    # print(f"Statement Month: {doc.statement_month}")
    # print(f"Currency: {doc.currency}")
    # print(f"Opening Balance: {doc.opening_balance}")
    # print(f"Closing Balance: {doc.closing_balance}")
    # print(f"Total Credits: {doc.total_credits}")
    # print(f"Total Debits: {doc.total_debits}")
    # print(f"Transactions: {len(doc.transactions)} rows")

    # print("\n=== Sample Transactions (first 10) ===")
    # for t in doc.transactions[:10]:
    #     print(vars(t))

if __name__ == "__main__":
    main()
