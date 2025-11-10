from __future__ import annotations

from typing import Any, List, Optional
from surrealdb import AsyncSurreal


class InsightRepo:
    def __init__(self, db: AsyncSurreal):
        self.db = db

    async def create_insight(
        self,
        user_id: str,
        message: str,
        insight_type: str,
        account_id: Optional[str] = None,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        query = """
        CREATE insight CONTENT {
          user: type::thing($user_id),
          account: $account_id != NONE ? type::thing($account_id) : NONE,
          insight_type: $insight_type,
          message: $message,
          period_start: $period_start,
          period_end: $period_end,
          metadata: $metadata
        };
        """
        vars = {
            "user_id": user_id,
            "account_id": account_id,
            "insight_type": insight_type,
            "message": message,
            "period_start": period_start,
            "period_end": period_end,
            "metadata": metadata or {},
        }
        res = await self.db.query(query, vars)
        rec = res[0][0] if res and isinstance(res[0], list) and res[0] else None
        return rec["id"] if rec else ""

    async def get_user_insights(self, user_id: str, limit: int = 100, offset: int = 0) -> List[dict[str, Any]]:
        query = "SELECT * FROM insight WHERE user = type::thing($user_id) ORDER BY created_at DESC LIMIT $limit START $offset;"
        res = await self.db.query(query, {"user_id": user_id, "limit": limit, "offset": offset})
        return res[0]

    async def get_account_insights(self, account_id: str, limit: int = 100, offset: int = 0) -> List[dict[str, Any]]:
        query = "SELECT * FROM insight WHERE account = type::thing($account_id) ORDER BY created_at DESC LIMIT $limit START $offset;"
        res = await self.db.query(query, {"account_id": account_id, "limit": limit, "offset": offset})
        return res[0]


