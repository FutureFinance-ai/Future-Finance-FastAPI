from fastapi import UploadFile, HTTPException , File
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from services.pdf_statement_processor import PdfStatementProcessor

from services.artifact_storage import ArtifactStorage
from typing import Dict, Any



class StatementService:
    def __init__(self, storage_dir: str | None = None):
        self.storage = ArtifactStorage(storage_dir) if storage_dir else ArtifactStorage()

    async def upload_pdf_bank_statement(self, file: UploadFile = File(...)):
        if file is None:
            raise HTTPException(status_code=400, detail="No file provided")

        filename = file.filename or "uploaded.pdf"
        content_type = (file.content_type or "").lower()

        if "pdf" not in content_type and not filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files are supported")

        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        try:
            processor = PdfStatementProcessor(storage=self.storage)
            result: Dict[str, Any] = processor.process_pdf(content=content, filename=filename)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to process PDF: {e}")

        return JSONResponse(content=jsonable_encoder(result))

        