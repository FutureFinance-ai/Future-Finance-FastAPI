from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Dict, List

from celery import shared_task
from settings.tenant_db import get_db_for_namespace


@shared_task(name="detect_subscriptions", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def detect_subscriptions(self, tenant_ns: str, user_id: str) -> int:
	"""
	Detect recurring subscriptions per user and create/update subscription records.
	Returns number of subscriptions upserted.
	"""
	upserted = 0
	async def _run() -> int:
		nonlocal upserted
		db = await get_db_for_namespace(tenant_ns)
		try:
			res = await db.query(
				"SELECT * FROM transaction WHERE category = 'Subscriptions' OR is_subscription = true"
			)
			rows = res[0].get("result", []) if res else []
			key_to_txns: Dict[str, List[dict]] = defaultdict(list)
			for row in rows:
				merchant_name = (row.get("merchant_name") or "").lower()
				amount_minor = int(row.get("debit") or 0) or int(row.get("credit") or 0)
				key = f"{merchant_name}|{abs(amount_minor)}"
				key_to_txns[key].append(row)
			for key, txns in key_to_txns.items():
				if len(txns) < 2:
					continue
				merchant_name = txns[0].get("merchant_name", "")
				account = txns[0].get("account")
				first_seen = min([t.get("trans_time") or t.get("value_date") for t in txns])
				last_seen = max([t.get("trans_time") or t.get("value_date") for t in txns])
				# Upsert merchant record
				mq = await db.query("SELECT * FROM merchant WHERE normalized_name = $n", {"n": merchant_name.lower()})
				mr = mq[0].get("result", []) if mq else []
				if mr:
					merchant_id = mr[0]["id"]
				else:
					cr = await db.create("merchant", {"name": merchant_name, "normalized_name": merchant_name.lower()})
					merchant_id = cr["id"] if isinstance(cr, dict) else cr[0]["id"]
				# Upsert subscription
				await db.create(
					"subscription",
					{
						"user": f"users:{user_id}",
						"merchant": merchant_id,
						"account": account,
						"amount_estimate": abs(int(txns[0].get("debit") or 0) or int(txns[0].get("credit") or 0)),
						"currency": txns[0].get("currency") or "NGN",
						"cadence": "monthly",
						"active": True,
						"first_seen": first_seen,
						"last_seen": last_seen,
					},
				)
				upserted += 1
			return upserted
		finally:
			await db.close()
	return __import__("asyncio").get_event_loop().run_until_complete(_run())


