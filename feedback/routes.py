from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from db.postgres import get_async_session

router = APIRouter(prefix="/feedback", tags=["feedback"])


class FeedbackIn(BaseModel):
    txn_id: str
    kind: str  # e.g., 'category', 'anomaly'
    chosen_label: str | None = None


@router.post("/")
async def submit_feedback(payload: FeedbackIn, session: AsyncSession = Depends(get_async_session)) -> dict:
    await session.execute(
        text(
            """
            INSERT INTO user_feedback (txn_id, kind, chosen_label)
            VALUES (:txn_id, :kind, :label)
            """
        ),
        {"txn_id": payload.txn_id, "kind": payload.kind, "label": payload.chosen_label},
    )
    await session.commit()
    return {"status": "ok"}


