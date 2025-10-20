from __future__ import annotations

from fastapi import APIRouter


router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}")
async def job_status(job_id: str):
    # Placeholder: wire to Arq result store
    return {"job_id": job_id, "status": "queued"}


