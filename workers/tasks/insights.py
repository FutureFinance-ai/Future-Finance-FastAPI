from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
from celery import shared_task
from settings.tenant_db import get_db_for_namespace


@shared_task(name="generate_insights", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=3)
def generate_insights(self, tenant_ns: str, user_id: str, months: int = 3) -> int:
	"""
	Compute MoM category trends and subscriptions summary.
	Returns number of insights written.
	"""
	written = 0
	async def _run() -> int:
		nonlocal written
		db = await get_db_for_namespace(tenant_ns)
		try:
			now = datetime.utcnow()
			start = now - timedelta(days=months * 31)
			res = await db.query(
				"SELECT trans_time, amount_minor, debit, credit, category, account FROM transaction WHERE trans_time >= $start",
				{"start": start.isoformat()},
			)
			rows = res[0].get("result", []) if res else []
			if not rows:
				return 0
			records = []
			for r in rows:
				amt = 0
				if r.get("debit"):
					try:
						amt = -float(str(r["debit"]).replace(",", "").replace("+", ""))
					except Exception:
						amt = 0.0
				elif r.get("credit"):
					try:
						amt = float(str(r["credit"]).replace(",", "").replace("+", ""))
					except Exception:
						amt = 0.0
				records.append({"date": pd.to_datetime(r.get("trans_time") or datetime.utcnow()), "category": r.get("category") or "Uncategorized", "amount": amt})
			df = pd.DataFrame(records)
			df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
			pivot = df.pivot_table(index="month", columns="category", values="amount", aggfunc="sum").fillna(0)
			if len(pivot.index) >= 2:
				latest, prev = pivot.iloc[-1], pivot.iloc[-2]
				for cat in pivot.columns:
					prev_val = prev[cat]
					cur_val = latest[cat]
					change = 0.0 if prev_val == 0 else (cur_val - prev_val) / abs(prev_val) * 100.0
					msg = f"Your '{cat}' spending is {change:+.1f}% vs last month."
					await db.create("insight", {"user": f"users:{user_id}", "type": "category_trend", "message": msg, "score": abs(change), "period_start": str(pivot.index[-1])})
					written += 1
			# Subscriptions summary (count)
			subq = await db.query("SELECT count() AS c FROM subscription WHERE user = $u AND active = true", {"u": f"users:{user_id}"})
			count = (subq[0]["result"][0]["c"] if subq and subq[0].get("result") else 0)
			await db.create("insight", {"user": f"users:{user_id}", "type": "subscriptions_summary", "message": f"You have {count} active subscriptions.", "score": float(count)})
			written += 1
			return written
		finally:
			await db.close()
	return __import__("asyncio").get_event_loop().run_until_complete(_run())


