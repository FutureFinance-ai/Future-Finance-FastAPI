from __future__ import annotations

from celery import shared_task
from settings.tenant_db import get_db_for_namespace


@shared_task(name="assign_sequence_and_trails", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def assign_sequence_and_trails(self, tenant_ns: str, account_id: str) -> int:
	"""
	Assign sequence_index within an account ordered by (trans_time, id) and create follows edges.
	Returns number of transactions linked.
	"""
	linked = 0
	async def _run() -> int:
		nonlocal linked
		db = await get_db_for_namespace(tenant_ns)
		try:
			# Fetch transactions ordered
			res = await db.query(
				"SELECT id, trans_time FROM transaction WHERE account = $account ORDER BY trans_time ASC, id ASC",
				{"account": account_id},
			)
			rows = res[0].get("result", []) if res else []
			prev_id = None
			for idx, row in enumerate(rows, start=1):
				tx_id = row["id"]
				await db.query("UPDATE $id SET sequence_index = $idx", {"id": tx_id, "idx": idx})
				if prev_id:
					await db.query("RELATE $prev->follows->$cur SET account = $acct, order_index = $idx", {"prev": prev_id, "cur": tx_id, "acct": account_id, "idx": idx})
					linked += 1
				prev_id = tx_id
			return linked
		finally:
			await db.close()
	return __import__("asyncio").get_event_loop().run_until_complete(_run())


