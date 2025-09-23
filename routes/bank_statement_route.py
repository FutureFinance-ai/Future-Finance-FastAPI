from fastapi import UploadFile, File, HTTPException, status
from schemas.UploadData import DocumentUploadResponse
from services.upload_service import UploadService
from fastapi import APIRouter

router = APIRouter()
statement_service = StatementService()


@router.post("/upload-data", response_model=DocumentUploadResponse)
async def data_upload_route(
    file: UploadFile = File(...)
) -> DocumentUploadResponse:
    """Accept a data upload."""

    try:
        result = await statement_service.upload_pdf_bank_statement(file)
        return result
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))