from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends

from upload_service.upload_service import UploadService
from surrealdb import AsyncSurreal
from auth.auth import get_current_active_user
from settings.deps import get_db_from_surreal_bearer
from workers.tasks.categorize import categorize_transactions
from workers.tasks.ordering import assign_sequence_and_trails
from workers.tasks.subscriptions import detect_subscriptions

router = APIRouter(prefix="/upload", tags=["upload"])

def get_upload_service() -> UploadService:
    return UploadService()


@router.post("/document", status_code=status.HTTP_201_CREATED)
async def upload_document(
    file: UploadFile = File(...),
    db: AsyncSurreal = Depends(get_db_from_surreal_bearer),
    upload_service: UploadService = Depends(get_upload_service),
    user = Depends(get_current_active_user),
):
    if file is None or file.filename is None or file.filename.strip() == "":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")
    res = await upload_service.upload_document(db, file, user.id)
    try:
        upload_id = res.get("res", {}).get("upload_id") or res.get("res", {}).get("id") or res.get("res", {}).get("uploadId") or res.get("res", {}).get("upload_id")
        if upload_id:
            # Fire-and-forget categorization and ordering; subscriptions will follow
            # Discover tenant namespace from current Surreal auth claims
            auth_info = await db.query("RETURN $auth")
            claims = (auth_info[0].get("result") or [{}])[0] if auth_info else {}
            tenant_ns = claims.get("tenant") or claims.get("NS") or claims.get("ns")
            categorize_transactions.delay(tenant_ns, user.id, upload_id)
            # If an account id is returned, trigger ordering
            acct = res.get("res", {}).get("account_id") or res.get("res", {}).get("account")
            if acct:
                assign_sequence_and_trails.delay(tenant_ns, acct)
            detect_subscriptions.delay(tenant_ns, user.id)
    except Exception:
        # Non-fatal if queue unavailable
        pass
    return res