from __future__ import annotations

from fastapi import APIRouter, HTTPException
from settings.config import settings
from storage.s3_client import S3Client


router = APIRouter(prefix="/storage", tags=["storage"])


@router.get("/raw-json-url/{account_id}")
async def raw_json_url(account_id: str):
    # Placeholder: fetch bucket/key from SurrealDB by account
    if not settings.S3_BUCKET_RAW_JSON:
        raise HTTPException(status_code=500, detail="S3 bucket not configured")
    bucket = settings.S3_BUCKET_RAW_JSON
    key = f"statements/{account_id}.json"
    url = await S3Client().presigned_get(bucket, key)
    return {"url": url}


@router.get("/original-pdf-url/{upload_id}")
async def original_pdf_url(upload_id: str):
    if not settings.S3_BUCKET_UPLOADS:
        raise HTTPException(status_code=500, detail="S3 bucket not configured")
    bucket = settings.S3_BUCKET_UPLOADS
    key = f"uploads/{upload_id}.pdf"
    url = await S3Client().presigned_get(bucket, key)
    return {"url": url}


