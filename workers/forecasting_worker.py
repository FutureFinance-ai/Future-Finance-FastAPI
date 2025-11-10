from __future__ import annotations

from typing import Any, List
from datetime import datetime, timedelta, timezone
import pandas as pd

from arq.connections import RedisSettings
from arq import cron

from settings.config import settings
from settings.db import get_service_db


async def _fetch_account_transactions(db, account_id: str, days: int = 365) -> List[dict]:
    since_iso = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    query = """
    SELECT value_date, amount_minor, balance
    FROM transaction
    WHERE account = type::thing($account_id)
      AND value_date >= $since
    ORDER BY value_date ASC;
    """
    res = await db.query(query, {"account_id": account_id, "since": since_iso})
    return res[0]


def _prepare_daily_series(rows: List[dict]) -> tuple[pd.DataFrame, float]:
    if not rows:
        return pd.DataFrame(columns=["ds", "y"]), 0.0
    s = pd.DataFrame(rows)
    s["ds"] = pd.to_datetime(s["value_date"], errors="coerce").dt.floor("D")
    s["amount"] = (s["amount_minor"] or 0) / 100.0 if "amount_minor" in s.columns else 0.0
    # Daily net
    daily = s.groupby("ds")["amount"].sum().reset_index().rename(columns={"amount": "y"})
    # Last known balance in major units if available
    last_balance = None
    if "balance" in s.columns and not s["balance"].isna().all():
        try:
            last_non_null = s[~s["balance"].isna()].iloc[-1]["balance"]
            last_balance = float(last_non_null) / 100.0
        except Exception:
            last_balance = None
    return daily, (last_balance or 0.0)


async def forecast_account_balance(ctx: dict[str, Any], account_id: str, periods: int = 30) -> dict:
    try:
        from prophet import Prophet
    except Exception:
        return {"created": 0, "error": "prophet_not_available"}

    db = await get_service_db()
    rows = await _fetch_account_transactions(db, account_id=account_id, days=365)
    daily, last_balance = _prepare_daily_series(rows)
    if daily.empty:
        await db.close()
        return {"created": 0}

    model = Prophet(seasonality_mode="additive", yearly_seasonality=True, weekly_seasonality=True)
    model.fit(daily)
    future = model.make_future_dataframe(periods=periods, freq="D")
    fc = model.predict(future)
    # Convert net flow forecast to balance by cumulative sum from last known balance
    hist_days = daily.set_index("ds")["y"].reindex(fc["ds"]).fillna(0.0)
    flow = hist_days.copy()
    flow.loc[fc["ds"].iloc[-periods:]] = fc.set_index("ds")["yhat"].iloc[-periods:]
    balance = flow.cumsum() + last_balance

    # Remove existing future forecasts to avoid duplicates
    cutoff_iso = datetime.now(timezone.utc).isoformat()
    await db.query("DELETE forecast WHERE account = type::thing($account_id) AND ds >= $cutoff;", {"account_id": account_id, "cutoff": cutoff_iso})

    # Insert new forecasts
    rows_to_insert = []
    for ds, bal in balance.iloc[-periods:].items():
        rows_to_insert.append(
            {"account": account_id, "ds": ds.isoformat(), "yhat": float(bal)}
        )
    if rows_to_insert:
        await db.query(
            "LET $rows = array::map($rows, function($r){ $r.account = type::thing($r.account); return $r; }); INSERT INTO forecast $rows;",
            {"rows": rows_to_insert},
        )
    await db.close()
    return {"created": len(rows_to_insert)}


class WorkerSettings:
    functions = [forecast_account_balance]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")
    # Example daily schedule at 02:00
    cron_jobs = [cron(forecast_account_balance, hour=2, minute=0)]


