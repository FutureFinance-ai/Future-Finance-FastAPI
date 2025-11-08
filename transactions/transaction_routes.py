from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from surrealdb import AsyncSurreal

from settings.db import get_db
from transactions.transaction_repo import TransactionRepo


router = APIRouter(prefix="/transactions", tags=["transactions"])


class CategoryUpdate(BaseModel):
    category: str


@router.patch("/{transaction_id}/category")
async def update_category(transaction_id: str, body: CategoryUpdate):
    db: AsyncSurreal = await get_db()
    repo = TransactionRepo(db)
    try:
        await repo.record_user_correction(transaction_id, body.category)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get("/{transaction_id}/related")
async def get_related(transaction_id: str):
    db: AsyncSurreal = await get_db()
    # Fetch related transactions via graph edges
    query = """
    SELECT {
      follows_next: (SELECT out as id FROM follows WHERE in = $id),
      follows_prev: (SELECT in as id FROM follows WHERE out = $id),
      sends_to:     (SELECT in as id FROM sends_to WHERE out = $id),
      received_from:(SELECT out as id FROM sends_to WHERE in = $id),
      refunded_by:  (SELECT in as id FROM refunded_by WHERE out = $id),
      covers:       (SELECT in as id FROM covers WHERE out = $id),
      related_to:   (SELECT in as id FROM related_to WHERE out = $id)
    };
    """
    res = await db.query(query, {"id": transaction_id})
    return res[0][0] if res and isinstance(res[0], list) and res[0] else {}


