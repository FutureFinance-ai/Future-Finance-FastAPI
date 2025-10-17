from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, HTTPException, status

from upload_service.upload_service import UploadService
from surrealdb import AsyncSurreal
from fastapi import Depends
from config.db import get_db

router = APIRouter(prefix="/upload", tags=["upload"])
upload_service = UploadService()    


@router.post("/document", status_code=status.HTTP_201_CREATED)
async def upload_document(file: UploadFile = File(...), db: AsyncSurreal = Depends(get_db)):
    if file is None or file.filename is None or file.filename.strip() == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")

    return await upload_service.upload_document(db, file)
   

