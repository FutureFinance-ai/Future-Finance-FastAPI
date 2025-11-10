from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Depends, Query
from surrealdb import AsyncSurreal

from settings.deps import get_db_from_surreal_bearer
from auth.auth import get_current_active_user
from auth.models import User

router = APIRouter(prefix="/ai", tags=["ai"])


@router.get("/transactions")
async def list_transactions(
	db: AsyncSurreal = Depends(get_db_from_surreal_bearer),
	user: User = Depends(get_current_active_user),
	account_id: str = Query(..., description="Account record id, e.g., account:xyz"),
	start: Optional[str] = Query(None),
	end: Optional[str] = Query(None),
	limit: int = Query(200, ge=1, le=1000),
	offset: int = Query(0, ge=0),
):
	q = "SELECT * FROM transaction WHERE account = type::thing($account) "
	vars: dict = {"account": account_id, "limit": limit, "offset": offset}
	if start:
		q += "AND trans_time >= $start "
		vars["start"] = start
	if end:
		q += "AND trans_time < $end "
		vars["end"] = end
	q += "ORDER BY trans_time ASC LIMIT $limit START $offset;"
	res = await db.query(q, vars)
	return res[0].get("result", []) if res else []


@router.get("/trails/{account_id}")
async def transaction_trail(
	account_id: str,
	db: AsyncSurreal = Depends(get_db_from_surreal_bearer),
	user: User = Depends(get_current_active_user),
	after_tx: Optional[str] = Query(None, description="Start after transaction id"),
	limit: int = Query(200, ge=1, le=1000),
):
	if after_tx:
		q = "SELECT ->follows->transaction AS next FROM $start LIMIT $limit;"
		res = await db.query(q, {"start": after_tx, "limit": limit})
		return res[0].get("result", []) if res else []
	else:
		# head of the chain
		q = "SELECT * FROM transaction WHERE account = type::thing($account) ORDER BY trans_time ASC LIMIT 1;"
		res = await db.query(q, {"account": account_id})
		return res[0].get("result", []) if res else []


@router.get("/insights")
async def list_insights(
	db: AsyncSurreal = Depends(get_db_from_surreal_bearer),
	user: User = Depends(get_current_active_user),
	account_id: Optional[str] = Query(None),
):
	if account_id:
		res = await db.query("SELECT * FROM insight WHERE user = $u AND account = $a ORDER BY created_at DESC LIMIT 200", {"u": f"users:{user.id}", "a": account_id})
	else:
		res = await db.query("SELECT * FROM insight WHERE user = $u ORDER BY created_at DESC LIMIT 200", {"u": f"users:{user.id}"})
	return res[0].get("result", []) if res else []


@router.get("/forecast/{account_id}")
async def get_forecast(
	account_id: str,
	db: AsyncSurreal = Depends(get_db_from_surreal_bearer),
	user: User = Depends(get_current_active_user),
):
	res = await db.query("SELECT * FROM forecast WHERE user = $u AND account = $a ORDER BY generated_at DESC LIMIT 1", {"u": f"users:{user.id}", "a": account_id})
	return (res[0].get("result", []) if res else [])[:1]


