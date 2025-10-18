

from fastapi import UploadFile, HTTPException, status
from analysis_service.analysis_service import AnalysisService, get_analysis_service


import os


class UploadService:
    def __init__(self, analysis_service: AnalysisService | None = None):
        self.analysis_service = analysis_service or get_analysis_service()

    async def upload_document(self, db, file: UploadFile):

      pdf_file_bytes = await file.read()
      doc_type = await self.process_uploaded_file_check(file)

      if doc_type == "pdf":
          try:
              data = await self.extract_financial_data_llm(pdf_file_bytes)
              # Convert to strict DataFrame structure
              df = await self.analysis_service.transactions_to_dataframe(data)
              # Return normalized JSON for API clients
              normalized = {
                "account_name": data.get("account_name", ""),
                "account_number": data.get("account_number", ""),
                "opening_balance": float(data.get("opening_balance", 0.0) or 0.0),
                "closing_balance": float(data.get("closing_balance", 0.0) or 0.0),
                "transactions": df.to_dict(orient="records"),
              }
              return normalized
          except Exception as e:
              raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error extracting financial data: {e}")
      else:
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported file type: {doc_type}")

    async def process_uploaded_file_check(self, file: UploadFile):
      SUPPORTED_MIME_TYPES = {
            "application/pdf": "pdf",
            "text/csv": "csv",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",  # .xlsx
            "application/vnd.ms-excel": "xls",  # .xls
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",  # .docx
            # Note: Use file.content_type for reliable checks, not just extensions.
        }
      content_type = file.content_type or "application/octet-stream"
      file_handler_key = SUPPORTED_MIME_TYPES.get(content_type)

      if file_handler_key is None:
          _, ext = os.path.splitext(file.filename)
          ext = ext.lower().lstrip('.')
          if ext in ["pdf", "csv", "xlsx", "xls", "docx"]:
              file_handler_key = ext
          else:
              raise HTTPException(
                  status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                  detail=f"Unsupported file type or MIME type: {content_type} / {ext}"
              )
      return file_handler_key

    async def extract_financial_data_llm(self, pdf_file_bytes: bytes) -> dict:
      """
      Sends a PDF file to a multimodal LLM for hyper-accurate, structured data extraction.
      
      :param pdf_file_bytes: The raw byte content of the PDF file.
      :param model_name: The LLM to use (e.g., gemini-2.5-pro or gemini-2.5-flash).
      :return: A dictionary containing the structured financial data.
      """

      json_schema = {
          "type": "object",
          "properties": {
              "account_name": {"type": "string"},
              "account_number": {"type": "string"},
              "opening_balance": {"type": "number", "description": "Prioritize the value from the 'Summary - Wallet Balance' table, cleaned and normalized."},
              "closing_balance": {"type": "number", "description": "Prioritize the value from the 'Summary - Wallet Balance' table, cleaned and normalized."},
              "transactions": {
                  "type": "array",
                  "items": {
                      "type": "object",
                      "properties": {
                          "transaction_date": {"type": "string", "description": "The date of the transaction (e.g., 2025 May 04)."},
                          "transaction_description": {"type": "string", "description": "The full description, including all stitched fragments from continuation rows (Ref and Counterparty can be appended to this if they don't fit the schema)."},
                          "debit": {"type": "number", "description": "The transaction amount as a negative float (e.g., -900.00)."},
                          "credit": {"type": "number", "description": "The transaction amount as a positive float (e.g., 5000.00)."},
                          "balance": {"type": "number"},
                          "transaction_category": {"type": "string", "description": "The transaction category."},
                          "counterparty": {"type": "string", "description": "The counterparty name and account/bank."}
                      },
                      "required": ["transaction_date", "transaction_description", "balance", "transaction_category", "counterparty"]
                  }
              }
          },
          "required": ["account_name", "account_number", "opening_balance", "closing_balance", "transactions"]
      }

      SYSTEM_INSTRUCTION = (
          "You are a hyper-accurate financial data extraction engine. "
          "Your task requires advanced visual analysis and reasoning. "
          "You MUST output a single JSON object conforming exactly to the provided schema."
          # Crucial directive for reliability:
          "**Crucial Directive:** When extracting transactions, you must visually process the layout. "
          "Stitch together all fields from multi-line rows into one single transaction object. "
          "The final list of transactions must be **merged and sorted chronologically**."
      )

      USER_PROMPT = (
          "Analyze this bank statement PDF. "
          "1. Extract the primary header fields: Account Name, Account Number, Opening Balance, and Closing Balance (prioritize the Summary - Wallet Balance). "
          "2. Extract ALL transactions from the entire document (Wallet and OWealth sections). "
          "3. **Normalization**: Set the 'Debit' field as a negative float (or 0) and the 'Credit' field as a positive float (or 0). All currency symbols and commas must be removed from numerical outputs. "
          "4. **Final Output**: The 'transactions' array must contain all records and be sorted ascending by the 'trans_time' field."
      )

      # 2. API CALL (Conceptual)
      # Replace the actual LLM client initialization and call with your chosen SDK implementation
      # Example conceptual code for handling the file:
      # client = LLM_Client()
      # uploaded_file = client.files.upload(pdf_file_bytes, mime_type="application/pdf")
      # response = client.generate_content(
      #     model=model_name,
      #     contents=[uploaded_file, USER_PROMPT],
      #     config={
      #         "system_instruction": SYSTEM_INSTRUCTION,
      #         "response_mime_type": "application/json",
      #         "response_schema": json_schema
      #     }
      # )
      # client.files.delete(uploaded_file)
      # return json.loads(response.text)

      # Since we can't execute the API call, we return the structured logic
      return {
          "status": "Awaiting LLM API Execution"
      }

