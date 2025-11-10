from __future__ import annotations

from fastapi import APIRouter, Query
from surrealdb import AsyncSurreal
from settings.db import get_db


router = APIRouter(prefix="/accounts", tags=["forecasts"])


@router.get("/{account_id}/forecast")
async def get_account_forecast(account_id: str, days: int = Query(30, ge=1, le=60)):
    db: AsyncSurreal = await get_db()
    query = "SELECT * FROM forecast WHERE account = type::thing($account_id) ORDER BY ds ASC LIMIT $limit;"
    res = await db.query(query, {"account_id": account_id, "limit": days})
    return res[0]


