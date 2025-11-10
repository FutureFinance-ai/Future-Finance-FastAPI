from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Dict

from celery import shared_task
from settings.tenant_db import get_db_for_namespace

try:
	from prophet import Prophet
	HAS_PROPHET = True
except Exception:
	HAS_PROPHET = False
	import math


@shared_task(name="forecast_cashflow", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_jitter=True, max_retries=3)
def forecast_cashflow(self, tenant_ns: str, user_id: str, account_id: str, horizon_days: int = 30) -> int:
	"""
	Build a daily balance series from transactions and forecast next `horizon_days` balances.
	Returns number of forecast points written.
	"""
	written = 0
	async def _run() -> int:
		nonlocal written
		db = await get_db_for_namespace(tenant_ns)
		try:
			res = await db.query(
				"SELECT trans_time, debit, credit FROM transaction WHERE account = $acct ORDER BY trans_time ASC",
				{"acct": account_id},
			)
			rows = res[0].get("result", []) if res else []
			if not rows:
				return 0
			# Build daily balance approximation
			bal = 0.0
			daily: Dict[str, float] = {}
			for r in rows:
				if r.get("debit"):
					try:
						bal -= float(str(r["debit"]).replace(",", "").replace("+", ""))
					except Exception:
						pass
				if r.get("credit"):
					try:
						bal += float(str(r["credit"]).replace(",", "").replace("+", ""))
					except Exception:
						pass
				day = (r.get("trans_time") or datetime.utcnow()).split("T")[0] if isinstance(r.get("trans_time"), str) else (r.get("trans_time") or datetime.utcnow()).date().isoformat()
				daily[day] = bal
			points: List[dict] = []
			if HAS_PROPHET:
				import pandas as pd
				df = pd.DataFrame({"ds": pd.to_datetime(list(daily.keys())), "y": list(daily.values())}).sort_values("ds")
				m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=False, changepoint_prior_scale=0.1)
				m.fit(df)
				future = m.make_future_dataframe(periods=horizon_days)
				fc = m.predict(future)
				tail = fc.tail(horizon_days)
				for _, row in tail.iterrows():
					points.append({"date": row["ds"].date().isoformat(), "balance": float(row["yhat"])})
			else:
				# Simple flat projection if Prophet is unavailable
				last_balance = list(daily.values())[-1]
				for i in range(1, horizon_days + 1):
					points.append({"date": (datetime.utcnow().date() + timedelta(days=i)).isoformat(), "balance": float(last_balance)})
			await db.create(
				"forecast",
				{
					"user": f"users:{user_id}",
					"account": account_id,
					"horizon_days": horizon_days,
					"generated_at": datetime.utcnow().isoformat(),
					"method": "prophet" if HAS_PROPHET else "flat",
					"version": "v1",
					"points": points,
				},
			)
			written = len(points)
			return written
		finally:
			await db.close()
	return __import__("asyncio").get_event_loop().run_until_complete(_run())


