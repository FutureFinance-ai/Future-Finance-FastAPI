from __future__ import annotations

from typing import Any, List
from surrealdb import AsyncSurreal
from settings.db import get_service_db


async def relate_transfer_out_to_in(out_txn_id: str, in_txn_id: str) -> None:
    db = await get_service_db()
    await db.query("RELATE $out->sends_to->$in;", {"out": out_txn_id, "in": in_txn_id})
    await db.close()


async def relate_refund(purchase_txn_id: str, refund_txn_id: str) -> None:
    db = await get_service_db()
    await db.query("RELATE $purchase->refunded_by->$refund;", {"purchase": purchase_txn_id, "refund": refund_txn_id})
    await db.close()


async def relate_sequential_transactions(account_id: str, limit: int = 5000) -> int:
    """
    Create 'follows' edges between consecutive transactions of an account, ordered by value_date then trans_time.
    Returns number of edges created.
    """
    db = await get_service_db()
    res = await db.query(
        "SELECT id FROM transaction WHERE account = type::thing($account_id) ORDER BY value_date ASC, trans_time ASC LIMIT $limit;",
        {"account_id": account_id, "limit": limit},
    )
    rows: List[dict[str, Any]] = res[0] if res and isinstance(res[0], list) else []
    created = 0
    for prev, nxt in zip(rows, rows[1:]):
        await db.query("RELATE $prev->follows->$next;", {"prev": prev["id"], "next": nxt["id"]})
        created += 1
    await db.close()
    return created


