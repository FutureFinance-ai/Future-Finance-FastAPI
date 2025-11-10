from __future__ import annotations

import re
from typing import Any

from celery import shared_task
from joblib import load as joblib_load
from pathlib import Path

from settings.tenant_db import get_db_for_namespace


def _normalize_merchant(description: str) -> str:
	s = description.upper()
	s = re.sub(r"[^A-Z0-9\s&\-]", " ", s)
	s = re.sub(r"\s+", " ", s).strip()
	return s


def _heuristic_category(description: str) -> tuple[str, str, float]:
	desc = description.lower()
	if "starbucks" in desc or "coffee" in desc:
		return ("Food & Drink", "Starbucks", 0.7)
	if "uber" in desc:
		return ("Transport", "Uber", 0.7)
	if "netflix" in desc:
		return ("Subscriptions", "Netflix", 0.8)
	return ("Uncategorized", "", 0.1)


def _load_model() -> Any | None:
	model_path = Path("models/category_model.joblib")
	if model_path.exists():
		try:
			return joblib_load(model_path)
		except Exception:
			return None
	return None


@shared_task(name="categorize_transactions", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=5)
def categorize_transactions(self, tenant_ns: str, user_id: str, upload_id: str) -> int:
	"""
	Find newly ingested transactions by upload_id and categorize them.
	Returns number of transactions updated.
	"""
	model = _load_model()
	updated = 0
	async def _run() -> int:
		nonlocal updated
		db = await get_db_for_namespace(tenant_ns)
		try:
			# Find transactions by upload id
			res = await db.query("SELECT * FROM transaction WHERE upload_id = $upload_id", {"upload_id": upload_id})
			records = res[0].get("result", []) if res else []
			for row in records:
				desc = row.get("description") or row.get("Description") or ""
				category = ""
				merchant_name = ""
				confidence = 0.0
				if model:
					try:
						pred = model.predict([desc])[0]
						category = str(pred)
						confidence = 0.6
					except Exception:
						category, merchant_name, confidence = _heuristic_category(desc)
				else:
					category, merchant_name, confidence = _heuristic_category(desc)
				if not merchant_name:
					merchant_name = _normalize_merchant(desc)[:64]
				# Upsert merchant node
				merch = await db.query(
					"LET $n := string::lower($name);"
					"SELECT * FROM merchant WHERE normalized_name = $n;"
					,
					{"name": merchant_name}
				)
				existing = merch[0].get("result", []) if merch else []
				if existing:
					merchant_id = existing[0]["id"]
				else:
					created = await db.create("merchant", {"name": merchant_name, "normalized_name": merchant_name.lower()})
					merchant_id = created["id"] if isinstance(created, dict) else created[0]["id"]
				# Update transaction fields
				await db.query(
					"UPDATE $tx SET category = $category, merchant_name = $merchant_name;"
					"RELATE $tx->purchased_at->$merchant;"
					"RELATE $tx->tagged_as->(SELECT * FROM type::table($cat_tbl) WHERE name = $category LIMIT 1);",
					{"tx": row["id"], "category": category, "merchant_name": merchant_name, "merchant": merchant_id, "cat_tbl": "category"},
				)
				updated += 1
			return updated
		finally:
			await db.close()
	return __import__("asyncio").get_event_loop().run_until_complete(_run())


