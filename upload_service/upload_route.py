from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from auth.auth import get_current_active_user
from db.postgres import get_async_session
import json as _json

def json_dumps(obj: dict) -> str:
    return _json.dumps(obj, ensure_ascii=False)

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/document", status_code=status.HTTP_202_ACCEPTED)
async def upload_document(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_async_session),
    user = Depends(get_current_active_user),
):
    if file is None or file.filename is None or file.filename.strip() == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")
    # Read file payload (bounded by server limits)
    content = await file.read()
    blob = {
        "filename": file.filename,
        "content_type": file.content_type,
        "size": len(content),
    }
    # Insert raw record
    result = await session.execute(
        text(
            """
            INSERT INTO transactions_raw (user_id, source, blob)
            VALUES (:user_id::uuid, :source, :blob::jsonb)
            RETURNING id
            """
        ),
        {"user_id": str(user.id), "source": "upload_document", "blob": json_dumps(blob)},
    )
    raw_id = result.scalar_one()
    # Emit outbox event for cleaning/embedding pipeline
    await session.execute(
        text(
            """
            INSERT INTO outbox_events (kind, aggregate_id, payload)
            VALUES (:kind, :agg, :payload::jsonb)
            """
        ),
        {
            "kind": "TRANSACTION_INGESTED",
            "agg": raw_id,
            "payload": json_dumps({"raw_id": str(raw_id), "user_id": str(user.id), "blob": blob}),
        },
    )
    await session.commit()
    return {"status": "accepted", "upload_id": str(raw_id)}