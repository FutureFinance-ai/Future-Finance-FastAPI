from __future__ import annotations

from fastapi import APIRouter, Query
from settings.db import get_db
from insights.insight_repo import InsightRepo
from surrealdb import AsyncSurreal


router = APIRouter(prefix="/insights", tags=["insights"])


@router.get("/")
async def list_insights(user_id: str, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    db: AsyncSurreal = await get_db()
    repo = InsightRepo(db)
    return await repo.get_user_insights(user_id, limit=limit, offset=offset)


@router.get("/{insight_id}")
async def get_insight(insight_id: str):
    db: AsyncSurreal = await get_db()
    res = await db.query("SELECT * FROM $id;", {"id": insight_id})
    return res[0][0] if res and isinstance(res[0], list) and res[0] else {}


