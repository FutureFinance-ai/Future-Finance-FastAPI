from __future__ import annotations

from typing import List
from surrealdb import AsyncSurreal


class TransactionRepo:
    def __init__(self, db: AsyncSurreal):
        self.db = db

    async def bulk_create(self, account_id: str, rows: List[dict]) -> int:
        query = (
            "LET $rows = array::map($rows, function($t) { $t.account = type::thing($account_id); return $t; });\n"
            "INSERT INTO transaction $rows;"
        )
        await self.db.query(query, {"rows": rows, "account_id": account_id})
        return len(rows)

    async def find_by_date_range(self, account_id: str, start_iso: str, end_iso: str, limit: int = 200, offset: int = 0) -> list[dict]:
        query = (
            "SELECT * FROM transaction WHERE account = type::thing($account_id) AND value_date >= $start AND value_date < $end ORDER BY value_date ASC LIMIT $limit START $offset;"
        )
        res = await self.db.query(query, {"account_id": account_id, "start": start_iso, "end": end_iso, "limit": limit, "offset": offset})
        return res[0]

    async def search_description(self, account_id: str, q: str, limit: int = 50) -> list[dict]:
        query = (
            "SELECT * FROM transaction WHERE account = type::thing($account_id) AND description @ $q LIMIT $limit;"
        )
        res = await self.db.query(query, {"account_id": account_id, "q": q, "limit": limit})
        return res[0]

    async def aggregate_monthly(self, account_id: str, start_iso: str, end_iso: str) -> list[dict]:
        query = (
            "SELECT math::time::month(value_date) AS month, "
            "math::sum(amount_minor) AS net_minor, "
            "math::sum(if amount_minor > 0 THEN amount_minor ELSE 0 END) AS credits_minor, "
            "math::sum(if amount_minor < 0 THEN -amount_minor ELSE 0 END) AS debits_minor "
            "FROM transaction WHERE account = type::thing($account_id) AND value_date >= $start AND value_date < $end GROUP BY month ORDER BY month ASC;"
        )
        res = await self.db.query(query, {"account_id": account_id, "start": start_iso, "end": end_iso})
        return res[0]

    async def upsert_rollup_monthly(self, account_id: str, period_start_iso: str, credits_minor: int, debits_minor: int, net_minor: int) -> None:
        query = (
            "UPSERT txn_rollup CONTENT {account: type::thing($account_id), period_start: $period_start, credits_minor: $credits, debits_minor: $debits, net_minor: $net, updated_at: time::now()} ON DUPLICATE KEY UPDATE credits_minor = $credits, debits_minor = $debits, net_minor = $net, updated_at = time::now();"
        )
        await self.db.query(query, {"account_id": account_id, "period_start": period_start_iso, "credits": credits_minor, "debits": debits_minor, "net": net_minor})


    async def get_uncategorized_transactions(self, account_id: str | None = None, user_id: str | None = None, limit: int = 200) -> list[dict]:
        filters = ["(category = NONE OR category = 'Uncategorized')"]
        vars: dict = {"limit": limit}
        if account_id:
            filters.append("account = type::thing($account_id)")
            vars["account_id"] = account_id
        if user_id:
            filters.append("account.owner = type::thing($user_id)")
            vars["user_id"] = user_id
        where_clause = " AND ".join(filters)
        query = f"SELECT id, description FROM transaction WHERE {where_clause} ORDER BY value_date ASC LIMIT $limit;"
        res = await self.db.query(query, vars)
        return res[0]

    async def update_category(self, txn_id: str, category: str, merchant_name: str | None = None, is_subscription: bool | None = None) -> None:
        query = "UPDATE $id SET category = $category, merchant_name = $merchant, is_subscription = $is_sub, ai_categorized_at = time::now();"
        await self.db.query(query, {"id": txn_id, "category": category, "merchant": merchant_name, "is_sub": is_subscription})

    async def record_user_correction(self, txn_id: str, corrected_category: str) -> None:
        query = "UPDATE $id SET user_corrected_category = $corrected, category = $corrected;"
        await self.db.query(query, {"id": txn_id, "corrected": corrected_category})

