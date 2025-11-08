from __future__ import annotations

from typing import Any, List
from datetime import datetime, timezone

from arq.connections import RedisSettings
from arq import cron

from settings.config import settings
from settings.db import get_service_db
from ai_services.categorization import CategorizationService


async def categorize_new_transactions(ctx: dict[str, Any], user_id: str | None = None, account_id: str | None = None, limit: int = 500) -> dict:
    """
    Categorize uncategorized transactions.
    - If user_id provided: scope to accounts owned by user
    - If account_id provided: scope to that account
    - Otherwise: process globally (admin maintenance)
    """
    svc = CategorizationService()
    db = await get_service_db()

    filters: List[str] = ["(category = NONE OR category = 'Uncategorized')"]
    vars: dict[str, Any] = {"limit": limit}
    if account_id:
        filters.append("account = type::thing($account_id)")
        vars["account_id"] = account_id
    if user_id:
        filters.append("account.owner = type::thing($user_id)")
        vars["user_id"] = user_id

    where_clause = " AND ".join(filters)
    select_q = f"SELECT id, description FROM transaction WHERE {where_clause} ORDER BY value_date ASC LIMIT $limit;"
    rows = await db.query(select_q, vars)
    txs: List[dict[str, Any]] = rows[0] if rows and isinstance(rows[0], list) else []

    updated = 0
    for tx in txs:
        desc = tx.get("description") or ""
        category, merchant, source = svc.categorize_description(desc)
        is_sub = True if category == "Subscriptions" else False
        now_iso = datetime.now(timezone.utc).isoformat()
        await db.query(
            "UPDATE $id SET category = $category, merchant_name = $merchant, is_subscription = $is_sub, ai_categorized_at = $now;",
            {"id": tx["id"], "category": category, "merchant": merchant, "is_sub": is_sub, "now": now_iso},
        )
        updated += 1

    await db.close()
    return {"processed": len(txs), "updated": updated}


class WorkerSettings:
    functions = [categorize_new_transactions]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")


