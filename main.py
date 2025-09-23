from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional
from io import BytesIO
import pandas as pd
from schemas.UploadData import DocumentUploadResponse
from services.data_service import DataService
from services.statement_service import StatementService



app = FastAPI(title="Excel Upload API")
statement_service = StatementService()


@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {"status": "ok"}



router.include



@app.post("/upload-bank-statement", response_model=DocumentUploadResponse)
async def upload_bank_statement(
    file: UploadFile = File(...)
) -> DocumentUploadResponse:
    """Accept a bank statement upload."""

    try:
        return await statement_service.upload_pdf_bank_statement(file)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.delete("/delete-bank-statement/{statement_id}")
async def delete_bank_statement(statement_id:statement_id):
    try:
        return await statement_service.upload_pdf_bank_statement(file)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

