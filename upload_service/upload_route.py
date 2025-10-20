from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, HTTPException, status

from upload_service.upload_service import UploadService
from surrealdb import AsyncSurreal
from fastapi import Depends
from settings.db import get_db

router = APIRouter(prefix="/upload", tags=["upload"])

def get_upload_service() -> UploadService:
    return UploadService()


@router.post("/document", status_code=status.HTTP_201_CREATED)
async def upload_document(
    file: UploadFile = File(...),
    db: AsyncSurreal = Depends(get_db),
    upload_service: UploadService = Depends(get_upload_service),
    user_id: str = Depends(get_current_user),
):
    if file is None or file.filename is None or file.filename.strip() == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")

    return await upload_service.upload_document(db, file, user_id)